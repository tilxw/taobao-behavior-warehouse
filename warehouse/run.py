"""调度入口：按依赖顺序执行任务，按天分区 INSERT OVERWRITE，支持回填，重跑不重复。

  python -m warehouse.run --env sample init        建库建表、静态维表、ODS 全量落地
  python -m warehouse.run --env sample daily --start 2017-11-25 --end 2017-12-03 [--tasks a,b]
  python -m warehouse.run --env sample all         init + 整个数据窗口按天跑
  python -m warehouse.run --env sample reset       删库重来

任一质检规则（error 级）不通过：打印原因、退出码 2，后面的任务和日期都不再执行。
"""
import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from warehouse import config
from warehouse.quality import QualityGateError, run_checks
from warehouse.spark_session import build_spark
from warehouse.sql_runner import run_sql_file
from warehouse.tasks import (ONCE_TASKS, TASKS, ZIP_TABLE, ZIP_WATERMARK_KEY, Task, build_params,
                             daily_order, get_table_property)


@dataclass
class Context:
    db: str
    source_csv: Path | None          # None：调用方已经注册好 src_user_behavior 视图（测试用）
    run_checks: bool = True
    log_file: Path | None = None
    quiet: bool = False


class MissingUpstreamError(RuntimeError):
    pass


def log(ctx: Context, msg: str) -> None:
    if not ctx.quiet:
        print(msg, flush=True)


def _partition_exists(spark: SparkSession, db: str, table: str, dt: str) -> bool:
    return spark.sql(f"SHOW PARTITIONS {db}.{table} PARTITION (dt = '{dt}')").count() > 0


def check_upstream(spark: SparkSession, ctx: Context, task: Task, dt: str) -> None:
    for table, offset in task.reads:
        day = config.shift(dt, offset)
        if day < config.WINDOW_START:
            continue   # 数据起始日之前本来就没有数据
        if table == ZIP_TABLE:
            # 拉链表不按日期分区，看它的处理水位
            mark = get_table_property(spark, ctx.db, ZIP_TABLE, ZIP_WATERMARK_KEY)
            if mark is None or mark < day:
                raise MissingUpstreamError(
                    f"{task.name} {dt} 需要拉链表至少处理到 {day}，当前水位 {mark}；请先回填之前的日期")
        elif not _partition_exists(spark, ctx.db, table, day):
            raise MissingUpstreamError(f"{task.name} {dt} 缺少上游分区 {table}/dt={day}；请先回填 {day}")


def _count_rows(spark: SparkSession, db: str, task: Task, dt: str | None) -> int:
    where = f"WHERE dt = '{dt}'" if (dt and task.count_partition) else ""
    return spark.sql(f"SELECT count(*) AS c FROM {db}.{task.name} {where}").collect()[0]["c"]


def _append_log(ctx: Context, record: dict) -> None:
    if ctx.log_file is None:
        return
    ctx.log_file.parent.mkdir(parents=True, exist_ok=True)
    with ctx.log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def run_task(spark: SparkSession, ctx: Context, task: Task, dt: str | None) -> dict:
    params = build_params(ctx.db, dt)
    if dt:
        check_upstream(spark, ctx, task, dt)
        if task.name == ZIP_TABLE:
            mark = get_table_property(spark, ctx.db, ZIP_TABLE, ZIP_WATERMARK_KEY)
            if mark and mark > dt:
                log(ctx, f"  ! 拉链表已处理到 {mark}，重跑 {dt} 会先回滚到 {config.shift(dt, -1)}，"
                         f"之后的日期需要接着重跑")

    record = {"db": ctx.db, "task": task.name, "layer": task.layer, "dt": dt or "ALL",
              "started_at": datetime.now().isoformat(timespec="seconds")}
    spark.sparkContext.setJobDescription(f"{task.name} {dt or 'ALL'}")
    t0 = time.perf_counter()
    try:
        if task.runner:
            task.runner(spark, ctx, params)
        else:
            for f in task.sql:
                run_sql_file(spark, f, params)
        record["seconds"] = round(time.perf_counter() - t0, 2)
        record["rows"] = _count_rows(spark, ctx.db, task, dt)

        t1 = time.perf_counter()
        results = run_checks(spark, ctx.db, task.name, dt or "ALL", list(task.checks), params) \
            if ctx.run_checks else []
        record["check_seconds"] = round(time.perf_counter() - t1, 2)
        record["checks_passed"] = sum(r.passed for r in results)
        record["checks_total"] = len(results)
        record["status"] = "ok"
        warns = [r for r in results if not r.passed]
        log(ctx, f"  {task.name:<32} ok  {record['seconds']:>7.1f}s  rows={record['rows']:>12,}  "
                 f"checks {record['checks_passed']}/{record['checks_total']}"
                 + "".join(f"\n    [warn] {r.check.name}: {r.detail}" for r in warns))
        return record
    except QualityGateError as exc:
        record.update(status="dq_failed", error=str(exc))
        log(ctx, f"  {task.name:<32} 质检不通过\n{exc}")
        raise
    except Exception as exc:
        record.update(status="failed", error=f"{type(exc).__name__}: {exc}"[:2000])
        log(ctx, f"  {task.name:<32} 失败：{type(exc).__name__}: {str(exc)[:500]}")
        raise
    finally:
        record.setdefault("seconds", round(time.perf_counter() - t0, 2))
        _append_log(ctx, record)
        spark.sparkContext.setJobDescription(None)


def create_tables(spark: SparkSession, ctx: Context) -> None:
    run_sql_file(spark, "ddl/create_tables.sql", {"db": ctx.db})


def run_init(spark: SparkSession, ctx: Context) -> list[dict]:
    log(ctx, f"[init] 建库建表 {ctx.db}")
    create_tables(spark, ctx)
    return [run_task(spark, ctx, t, None) for t in ONCE_TASKS]


def run_daily(spark: SparkSession, ctx: Context, start: str, end: str,
              tasks: list[str] | None = None) -> list[dict]:
    if start < config.WINDOW_START or end > config.WINDOW_END or start > end:
        raise ValueError(f"日期范围必须在 {config.WINDOW_START} ~ {config.WINDOW_END} 内且 start <= end")
    order = daily_order(tasks)
    records = []
    for dt in config.date_range(start, end):
        log(ctx, f"[{dt}]")
        for task in order:
            records.append(run_task(spark, ctx, task, dt))
    return records


def reset(spark: SparkSession, ctx: Context) -> None:
    spark.sql(f"DROP DATABASE IF EXISTS {ctx.db} CASCADE")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="电商用户行为数仓调度")
    parser.add_argument("--env", default="sample", choices=config.ENVS)
    parser.add_argument("--no-checks", action="store_true", help="跳过质检（只用于调试）")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    sub.add_parser("reset")
    sub.add_parser("all")
    d = sub.add_parser("daily")
    d.add_argument("--start", default=config.WINDOW_START)
    d.add_argument("--end", default=None, help="默认等于 --start")
    d.add_argument("--tasks", default=None, help="逗号分隔，只跑这些任务（仍按依赖排序）")
    args = parser.parse_args(argv)

    env = config.ENVS[args.env]
    ctx = Context(db=env.database, source_csv=env.source_csv, run_checks=not args.no_checks,
                  log_file=config.LOG_DIR / f"task_runs_{env.name}.jsonl")
    spark = build_spark(f"tbw-{env.name}-{args.cmd}")
    t0 = time.perf_counter()
    try:
        if args.cmd == "reset":
            reset(spark, ctx)
        elif args.cmd == "init":
            run_init(spark, ctx)
        elif args.cmd == "all":
            run_init(spark, ctx)
            run_daily(spark, ctx, config.WINDOW_START, config.WINDOW_END)
        else:
            tasks = [t.strip() for t in args.tasks.split(",")] if args.tasks else None
            run_daily(spark, ctx, args.start, args.end or args.start, tasks)
    except QualityGateError:
        return 2
    except MissingUpstreamError as exc:
        print(f"上游缺失：{exc}", file=sys.stderr)
        return 3
    finally:
        log(ctx, f"总耗时 {time.perf_counter() - t0:.1f}s")
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
