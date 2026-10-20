"""PyFlink + Flink SQL：按事件时间回放用户行为流，用水位线处理乱序。

产出两张结果表（与离线同口径）：
  minute_pv_uv     1 分钟滚动窗口的 PV 和 UV（UV = 窗口内有浏览行为的去重用户数）
  hourly_top_item  每小时浏览量 Top10 商品（浏览量相同按 item_id 升序）

数据源用文件按到达顺序回放；生产环境中数据源换成 Kafka（connector = 'kafka'），SQL 其余部分不变。
作业只用 Flink SQL、没有 Python UDF，所以全部在 JVM 里执行，Python 只负责提交。

用法（Flink 的依赖和 PySpark 冲突，用单独的虚拟环境）：
  .venv-flink/bin/python realtime/flink_job.py --input data/replay/xxx.csv --out data/realtime/wm10 --watermark-delay 10
"""
import argparse
import json
import os
import shutil
import subprocess
import time
from pathlib import Path


def interval(seconds: int) -> str:
    if seconds % 60 == 0:
        return f"INTERVAL '{seconds // 60}' MINUTE"
    return f"INTERVAL '{seconds}' SECOND"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--watermark-delay", type=int, default=10, help="水位线延迟（秒）")
    parser.add_argument("--parallelism", type=int, default=1)
    args = parser.parse_args()

    if not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = subprocess.check_output(
            ["/usr/libexec/java_home", "-v", "21"], text=True).strip()
    # 不用 Python UDF，跳过 PyFlink 为 Python worker 启动的 Beam 回环服务
    os.environ["_python_worker_execution_mode"] = "process"
    from pyflink.table import EnvironmentSettings, TableEnvironment

    src = Path(args.input).resolve()
    out = Path(args.out).resolve()
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    conf = t_env.get_config()
    conf.set("parallelism.default", str(args.parallelism))
    conf.set("table.local-time-zone", "Asia/Shanghai")      # 窗口按北京时间对齐、输出北京时间
    conf.set("execution.checkpointing.interval", "10s")     # 文件 sink 在 checkpoint 时提交，保证 exactly-once
    conf.set("pipeline.name", f"user-behavior-realtime-wm{args.watermark_delay}s")

    t_env.execute_sql(f"""
        CREATE TABLE user_behavior (
            user_id       BIGINT,
            item_id       BIGINT,
            category_id   BIGINT,
            behavior_type STRING,
            event_ts      BIGINT,
            late_flag     INT,
            event_time AS TO_TIMESTAMP_LTZ(event_ts * 1000, 3),
            -- 有界乱序水位线：已见到的最大事件时间 - 延迟；落在已经触发过的窗口里的数据会被丢弃
            WATERMARK FOR event_time AS event_time - {interval(args.watermark_delay)}
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{src}',
            'format' = 'csv'
        )""")

    t_env.execute_sql(f"""
        CREATE TABLE minute_pv_uv (
            window_start STRING,
            pv           BIGINT,
            uv           BIGINT
        ) WITH ('connector' = 'filesystem', 'path' = '{out / "minute_pv_uv"}', 'format' = 'csv')""")

    t_env.execute_sql(f"""
        CREATE TABLE hourly_top_item (
            window_start STRING,
            rk           BIGINT,
            item_id      BIGINT,
            pv           BIGINT
        ) WITH ('connector' = 'filesystem', 'path' = '{out / "hourly_top_item"}', 'format' = 'csv')""")

    stmts = t_env.create_statement_set()
    stmts.add_insert_sql("""
        INSERT INTO minute_pv_uv
        SELECT DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss'), count(*), count(DISTINCT user_id)
        FROM TABLE(TUMBLE(TABLE user_behavior, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
        WHERE behavior_type = 'pv'
        GROUP BY window_start, window_end""")
    # 窗口 Top-N：窗口聚合之后按 window_start/window_end 分组排名，每个窗口结束时只输出一次最终结果
    stmts.add_insert_sql("""
        INSERT INTO hourly_top_item
        SELECT DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss'), rk, item_id, pv
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end
                                         ORDER BY pv DESC, item_id ASC) AS rk
            FROM (
                SELECT window_start, window_end, item_id, count(*) AS pv
                FROM TABLE(TUMBLE(TABLE user_behavior, DESCRIPTOR(event_time), INTERVAL '1' HOUR))
                WHERE behavior_type = 'pv'
                GROUP BY window_start, window_end, item_id
            )
        )
        WHERE rk <= 10""")

    t0 = time.time()
    stmts.execute().wait()
    elapsed = round(time.time() - t0, 1)
    meta = {"input": str(src), "watermark_delay_seconds": args.watermark_delay,
            "parallelism": args.parallelism, "elapsed_seconds": elapsed}
    (out / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1))
    print(json.dumps(meta, ensure_ascii=False))


if __name__ == "__main__":
    main()
