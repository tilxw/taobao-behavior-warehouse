"""从 DWD 导出实时回放用的行为流（单个 CSV，按"到达顺序"排好）。

和离线用同一份清洗后的明细，保证对账是同口径。到达顺序 = 事件时间 + 抖动：
  - 每条事件随机晚到 0~5 秒（由行内容哈希决定，结果可复现），制造小幅乱序；
  - 约 0.05% 的事件额外晚到 180 秒，模拟网络抖动导致的严重迟到。
文件里还带一列 late_flag（是否被注入了严重迟到），只用于对账时解释差异，Flink 作业的计算不使用它。

用法：python -m realtime.prepare_replay [--db tb_full] [--start 2017-11-25 --end 2017-12-03]
"""
import argparse
import shutil

from warehouse import config
from warehouse.spark_session import build_spark

REPLAY_DIR = config.DATA_DIR / "replay"
JITTER_SECONDS = 6          # 0~5 秒
LATE_PER_10000 = 5          # 0.05%
LATE_DELAY_SECONDS = 180


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=config.ENVS["full"].database)
    parser.add_argument("--start", default=config.WINDOW_START)
    parser.add_argument("--end", default=config.WINDOW_END)
    args = parser.parse_args()

    spark = build_spark("prepare-replay")
    out_tmp = REPLAY_DIR / "_tmp"
    shutil.rmtree(out_tmp, ignore_errors=True)
    df = spark.sql(f"""
        SELECT user_id, item_id, category_id, behavior_type, event_ts, late_flag,
               event_ts + pmod(hash(user_id, item_id, behavior_type, event_ts), {JITTER_SECONDS})
                        + IF(late_flag = 1, {LATE_DELAY_SECONDS}, 0) AS arrival_ts
        FROM (
            SELECT user_id, item_id, category_id, behavior_type, event_ts,
                   IF(pmod(hash(event_ts, user_id, item_id, behavior_type, 'late'), 10000) < {LATE_PER_10000},
                      1, 0) AS late_flag
            FROM {args.db}.dwd_user_behavior_inc
            WHERE dt BETWEEN '{args.start}' AND '{args.end}'
        ) t
    """)
    # 全局排序（按范围分区并行排序），part 文件按编号首尾相接就是整体有序的；
    # 再拼成一个文件：Flink 的 CSV 文件源不可切分，一个文件只由一个读线程顺序读，到达顺序不会被打乱
    (df.orderBy("arrival_ts", "event_ts", "user_id", "item_id", "behavior_type")
       .select("user_id", "item_id", "category_id", "behavior_type", "event_ts", "late_flag")
       .write.option("header", "false").csv(str(out_tmp)))
    target = REPLAY_DIR / f"user_behavior_{args.start}_{args.end}.csv"
    with target.open("wb") as out:
        for part in sorted(out_tmp.glob("part-*.csv")):
            with part.open("rb") as f:
                shutil.copyfileobj(f, out, length=16 << 20)
    shutil.rmtree(out_tmp, ignore_errors=True)
    print(f"写入 {target}")
    spark.stop()


if __name__ == "__main__":
    main()
