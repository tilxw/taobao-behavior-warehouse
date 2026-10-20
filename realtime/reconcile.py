"""实时结果和离线结果逐小时对账（同口径：都基于 DWD 清洗后的明细）。

离线：DWD 按北京时间分钟 / 小时分组，算 PV、UV（浏览用户去重）、每小时浏览量 Top10 商品（同值 item_id 升序）。
实时：Flink 作业输出的 minute_pv_uv、hourly_top_item。
差异解释：回放文件里 late_flag = 1 的事件被人为推迟了 180 秒到达，超过水位线延迟时会被 Flink 当作迟到数据丢弃；
逐分钟检查"离线 - 实时"的 PV 差值是否都不超过该分钟被推迟的 PV 条数。

用法：python -m realtime.reconcile --run-dir data/realtime/wm10 --replay data/replay/xxx.csv
"""
import argparse
import csv
import json
from pathlib import Path

from warehouse import config
from warehouse.spark_session import build_spark


def read_flink_csv(spark, path: Path, schema: str):
    files = [str(p) for p in path.rglob("*") if p.is_file() and not p.name.startswith(".")]
    if not files:
        raise FileNotFoundError(f"{path} 下没有 Flink 输出文件")
    return spark.read.schema(schema).csv(files)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--replay", required=True)
    parser.add_argument("--db", default=config.ENVS["full"].database)
    args = parser.parse_args()
    run_dir = Path(args.run_dir)
    meta = json.loads((run_dir / "run_meta.json").read_text())
    name = run_dir.name

    spark = build_spark("realtime-reconcile")
    db = args.db
    replay = spark.read.schema(
        "user_id BIGINT, item_id BIGINT, category_id BIGINT, behavior_type STRING, event_ts BIGINT, late_flag INT"
    ).csv(args.replay)
    replay.createOrReplaceTempView("replay")
    dt_range = spark.sql("""SELECT date_format(timestamp_seconds(min(event_ts)), 'yyyy-MM-dd') AS s,
                                   date_format(timestamp_seconds(max(event_ts)), 'yyyy-MM-dd') AS e,
                                   count(*) AS rows, count_if(late_flag = 1) AS late_rows,
                                   count_if(late_flag = 1 AND behavior_type = 'pv') AS late_pv
                            FROM replay""").collect()[0]

    read_flink_csv(spark, run_dir / "minute_pv_uv", "minute STRING, pv BIGINT, uv BIGINT") \
        .createOrReplaceTempView("rt_minute")
    read_flink_csv(spark, run_dir / "hourly_top_item", "hour STRING, rk BIGINT, item_id BIGINT, pv BIGINT") \
        .createOrReplaceTempView("rt_topn")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW off_minute AS
        SELECT date_format(event_time, 'yyyy-MM-dd HH:mm:00') AS minute, count(*) AS pv, count(DISTINCT user_id) AS uv
        FROM {db}.dwd_user_behavior_inc
        WHERE dt BETWEEN '{dt_range.s}' AND '{dt_range.e}' AND behavior_type = 'pv'
        GROUP BY 1""")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW off_topn AS
        SELECT hour, rk, item_id, pv FROM (
            SELECT hour, item_id, pv, row_number() OVER (PARTITION BY hour ORDER BY pv DESC, item_id ASC) AS rk
            FROM (SELECT date_format(event_time, 'yyyy-MM-dd HH:00:00') AS hour, item_id, count(*) AS pv
                  FROM {db}.dwd_user_behavior_inc
                  WHERE dt BETWEEN '{dt_range.s}' AND '{dt_range.e}' AND behavior_type = 'pv'
                  GROUP BY 1, 2) t
        ) x WHERE rk <= 10""")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW late_minute AS
        SELECT date_format(timestamp_seconds(event_ts), 'yyyy-MM-dd HH:mm:00') AS minute, count(*) AS late_pv
        FROM replay WHERE late_flag = 1 AND behavior_type = 'pv' GROUP BY 1""")

    minute = spark.sql("""
        SELECT coalesce(o.minute, r.minute) AS minute,
               substr(coalesce(o.minute, r.minute), 1, 13) AS hour,
               coalesce(o.pv, 0) AS pv_off, coalesce(r.pv, 0) AS pv_rt,
               coalesce(o.uv, 0) AS uv_off, coalesce(r.uv, 0) AS uv_rt,
               coalesce(l.late_pv, 0) AS late_pv
        FROM off_minute o
        FULL OUTER JOIN rt_minute r ON o.minute = r.minute
        LEFT JOIN late_minute l ON coalesce(o.minute, r.minute) = l.minute""")
    minute.cache().createOrReplaceTempView("cmp_minute")

    topn = spark.sql("""
        SELECT substr(coalesce(o.hour, r.hour), 1, 13) AS hour,
               count_if(o.item_id IS NOT NULL AND o.item_id = r.item_id) AS same_item,
               count_if(o.item_id IS NOT NULL AND o.item_id = r.item_id AND o.pv = r.pv) AS same_item_and_pv
        FROM off_topn o FULL OUTER JOIN rt_topn r ON o.hour = r.hour AND o.rk = r.rk
        GROUP BY 1""")
    topn.createOrReplaceTempView("cmp_topn")

    hourly = spark.sql("""
        SELECT m.hour,
               sum(pv_off) AS pv_off, sum(pv_rt) AS pv_rt, sum(pv_off - pv_rt) AS pv_diff,
               sum(late_pv) AS late_pv,
               count(*) AS minutes,
               count_if(pv_off = pv_rt AND uv_off = uv_rt) AS minutes_exact,
               count_if(pv_off - pv_rt > late_pv OR pv_rt > pv_off) AS minutes_unexplained,
               max(t.same_item) AS topn_same_item, max(t.same_item_and_pv) AS topn_same_item_and_pv
        FROM cmp_minute m LEFT JOIN cmp_topn t ON m.hour = t.hour
        GROUP BY m.hour ORDER BY m.hour""").collect()

    total = spark.sql("""
        SELECT count(*) AS minutes, count_if(pv_off = pv_rt AND uv_off = uv_rt) AS minutes_exact,
               sum(pv_off) AS pv_off, sum(pv_rt) AS pv_rt, sum(late_pv) AS late_pv,
               count_if(pv_off - pv_rt > late_pv OR pv_rt > pv_off) AS minutes_unexplained,
               count_if(uv_rt > uv_off) AS uv_over
        FROM cmp_minute""").collect()[0]
    hours = len(hourly)
    hours_exact = sum(1 for h in hourly if h.minutes_exact == h.minutes and h.topn_same_item_and_pv == 10)
    hours_topn_items = sum(1 for h in hourly if h.topn_same_item == 10)
    hours_topn_exact = sum(1 for h in hourly if h.topn_same_item_and_pv == 10)

    summary = {
        "run": name,
        "watermark_delay_seconds": meta["watermark_delay_seconds"],
        "flink_elapsed_seconds": meta["elapsed_seconds"],
        "replay_rows": dt_range.rows,
        "injected_late_rows": dt_range.late_rows,
        "injected_late_pv": dt_range.late_pv,
        "hours": hours,
        "hours_fully_matched": hours_exact,
        "hours_topn_same_items": hours_topn_items,
        "hours_topn_same_items_and_pv": hours_topn_exact,
        "minutes": total.minutes,
        "minutes_exact": total.minutes_exact,
        "minutes_unexplained": total.minutes_unexplained,
        "pv_offline": total.pv_off,
        "pv_realtime": total.pv_rt,
        "pv_dropped": total.pv_off - total.pv_rt,
        "minutes_uv_realtime_higher": total.uv_over,
    }
    out_md = config.REPORT_DIR / f"realtime_reconcile_{name}.md"
    out_csv = config.REPORT_DIR / f"realtime_reconcile_{name}_hourly.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(hourly[0].__fields__)
        for h in hourly:
            w.writerow(list(h))

    L = [f"# 实时对账：{name}（水位线延迟 {meta['watermark_delay_seconds']} 秒）", "",
         f"- 回放 {dt_range.rows:,} 条事件（{dt_range.s} ~ {dt_range.e}），其中 {dt_range.late_rows:,} 条被推迟 180 秒到达"
         f"（浏览行为 {dt_range.late_pv:,} 条）；Flink 作业耗时 {meta['elapsed_seconds']} 秒（并行度 {meta['parallelism']}）",
         f"- 逐小时：{hours} 个小时中 {hours_exact} 个完全一致（每分钟 PV、UV 都相等，且 Top10 商品和浏览量都相等）",
         f"- 每小时 Top10：{hours_topn_items}/{hours} 个小时名次与商品一致，{hours_topn_exact}/{hours} 个小时浏览量也一致",
         f"- 逐分钟：{total.minutes:,} 个分钟窗口中 {total.minutes_exact:,} 个 PV、UV 完全一致",
         f"- PV：离线 {total.pv_off:,}，实时 {total.pv_rt:,}，实时少 {total.pv_off - total.pv_rt:,} 条",
         f"- 无法用注入的迟到数据解释的分钟数：{total.minutes_unexplained}（判定：离线 - 实时 > 该分钟被推迟的浏览数，或实时 > 离线）",
         f"- 实时 UV 高于离线的分钟数：{total.uv_over}（迟到数据只会让实时少算，不应多算）",
         "", "## 有差异的小时（前 20 个）", "",
         "| 小时 | 离线 PV | 实时 PV | 差值 | 该小时被推迟的 PV | 分钟数 | 完全一致的分钟 | Top10 商品一致 | Top10 浏览量一致 |",
         "|---|---|---|---|---|---|---|---|---|"]
    diff_hours = [h for h in hourly if not (h.minutes_exact == h.minutes and h.topn_same_item_and_pv == 10)]
    for h in diff_hours[:20]:
        L.append(f"| {h.hour}:00 | {h.pv_off:,} | {h.pv_rt:,} | {h.pv_diff:,} | {h.late_pv:,} | {h.minutes} | "
                 f"{h.minutes_exact} | {h.topn_same_item}/10 | {h.topn_same_item_and_pv}/10 |")
    if not diff_hours:
        L.append("| （无） | | | | | | | | |")
    L += ["", f"逐小时明细：[{out_csv.name}]({out_csv.name})"]
    out_md.write_text("\n".join(L) + "\n", encoding="utf-8")
    (config.REPORT_DIR / f"realtime_reconcile_{name}.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print("\n".join(L[:9]))
    spark.stop()


if __name__ == "__main__":
    main()
