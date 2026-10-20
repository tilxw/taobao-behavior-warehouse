"""原始数据探查：重复行、越界时间、商品→类目映射、热点 key 分布、用户购买次数分布。

用法：python -m scripts.profile_raw --env full
结果写到 reports/raw_profile_<env>.md
"""
import argparse
import shutil
import tempfile
import time
from pathlib import Path

from warehouse import config
from warehouse.source import register_source_view
from warehouse.spark_session import build_spark


def q(spark, sql):
    return spark.sql(sql).collect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="sample", choices=config.ENVS)
    parser.add_argument("--tmp-dir", default=None, help="临时 Parquet 目录，默认系统临时目录")
    args = parser.parse_args()
    env = config.ENVS[args.env]

    spark = build_spark("profile-raw")
    t0 = time.time()
    register_source_view(spark, env.source_csv)
    # 先把 CSV 转成临时 Parquet，后面十几个查询就不用反复解析 CSV
    tmp = Path(tempfile.mkdtemp(prefix="profile_", dir=args.tmp_dir)) / "raw.parquet"
    spark.sql(f"""
        SELECT *, ts >= {config.WINDOW_START_TS} AND ts < {config.WINDOW_END_TS} AS in_window
        FROM src_user_behavior
    """).write.parquet(str(tmp))
    spark.read.parquet(str(tmp)).createOrReplaceTempView("raw")
    lines = [f"# 原始数据探查（{env.name}）", "", f"源文件：`{env.source_csv.name}`", ""]

    r = q(spark, """
        SELECT count(*) AS rows, count_if(NOT in_window) AS out_window,
               count_if(user_id IS NULL OR item_id IS NULL OR category_id IS NULL
                        OR behavior_type IS NULL OR ts IS NULL) AS null_rows,
               count(DISTINCT user_id) AS users, count(DISTINCT item_id) AS items,
               count(DISTINCT category_id) AS categories,
               min(ts) AS min_ts, max(ts) AS max_ts
        FROM raw""")[0]
    lines += ["## 总体", "", "| 指标 | 值 |", "|---|---|"]
    for k, v in r.asDict().items():
        lines.append(f"| {k} | {v:,} |" if isinstance(v, int) else f"| {k} | {v} |")

    r = q(spark, """
        SELECT count(*) AS rows, count(DISTINCT user_id, item_id, category_id, behavior_type, ts) AS distinct_rows
        FROM raw WHERE in_window""")[0]
    lines += ["", "## 重复行（窗口内，五列完全相同）", "",
              f"- 窗口内行数 {r.rows:,}，去重后 {r.distinct_rows:,}，重复 {r.rows - r.distinct_rows:,} 行"]

    rows = q(spark, """
        SELECT date_format(timestamp_seconds(ts), 'yyyy-MM-dd') AS d, count(*) AS c
        FROM raw WHERE NOT in_window GROUP BY 1 ORDER BY c DESC""")
    lines += ["", "## 越界时间戳按日期分布", "",
              f"- 越界行落在 {len(rows)} 个不同日期上；行数最多的 10 个：", "",
              "| 日期 | 行数 |", "|---|---|"]
    lines += [f"| {x.d} | {x.c:,} |" for x in rows[:10]]

    r = q(spark, """
        SELECT count(*) AS items, count_if(n > 1) AS multi_cat_items, max(n) AS max_cat
        FROM (SELECT item_id, count(DISTINCT category_id) AS n FROM raw WHERE in_window GROUP BY item_id)""")[0]
    lines += ["", "## 商品→类目映射", "",
              f"- 商品 {r.items:,} 个，其中挂了多个类目的 {r.multi_cat_items:,} 个，单个商品最多 {r.max_cat} 个类目"]

    total = q(spark, "SELECT count(*) AS c FROM raw WHERE in_window")[0].c
    for key, label in (("item_id", "商品"), ("category_id", "类目"), ("user_id", "用户")):
        rows = q(spark, f"""
            SELECT {key} AS k, count(*) AS c FROM raw WHERE in_window
            GROUP BY {key} ORDER BY c DESC LIMIT 5""")
        med = q(spark, f"""
            SELECT percentile_approx(c, 0.5) AS p50, percentile_approx(c, 0.99) AS p99
            FROM (SELECT count(*) AS c FROM raw WHERE in_window GROUP BY {key})""")[0]
        lines += ["", f"## 按{label}的行数分布（倾斜参考）", "",
                  f"- 每个{label}行数中位数 {med.p50:,}，P99 {med.p99:,}", "",
                  f"| {label} | 行数 | 占比 |", "|---|---|---|"]
        lines += [f"| {x.k} | {x.c:,} | {x.c / total:.3%} |" for x in rows]

    rows = q(spark, """
        SELECT buy_cnt, count(*) AS users FROM (
            SELECT user_id, count_if(behavior_type = 'buy') AS buy_cnt FROM raw WHERE in_window GROUP BY user_id)
        GROUP BY buy_cnt ORDER BY buy_cnt""")
    lines += ["", "## 用户窗口内累计购买次数分布（用于划分用户等级）", "", "| 购买次数 | 用户数 |", "|---|---|"]
    tail = 0
    for x in rows:
        if x.buy_cnt <= 10:
            lines.append(f"| {x.buy_cnt} | {x.users:,} |")
        else:
            tail += x.users
    lines.append(f"| >10 | {tail:,} |")

    lines += ["", f"探查耗时 {time.time() - t0:.0f}s"]
    out = config.REPORT_DIR / f"raw_profile_{env.name}.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    spark.stop()
    shutil.rmtree(tmp.parent, ignore_errors=True)


if __name__ == "__main__":
    main()
