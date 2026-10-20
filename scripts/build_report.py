"""汇总一次完整运行的真实数字：各层行数、守恒明细、任务耗时、质检结果、ADS 指标。

用法：python -m scripts.build_report --env full   → reports/run_report_<env>.md
"""
import argparse
import json
from collections import defaultdict

from warehouse import config
from warehouse.spark_session import build_spark
from warehouse.tasks import DAILY_TASKS, ONCE_TASKS


def table_md(rows, headers):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    for r in rows:
        out.append("| " + " | ".join(f"{v:,}" if isinstance(v, int) else str(v) for v in r) + " |")
    return out


def latest_task_runs(env_name):
    path = config.LOG_DIR / f"task_runs_{env_name}.jsonl"
    latest = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        rec = json.loads(line)
        if rec.get("status") == "ok":
            latest[(rec["task"], rec["dt"])] = rec
    return latest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="full", choices=config.ENVS)
    args = parser.parse_args()
    env = config.ENVS[args.env]
    db = env.database
    spark = build_spark("build-report")
    q = lambda sql: spark.sql(sql).collect()
    L = [f"# 运行报告（{env.name}，库 `{db}`）", ""]

    # ---------------- 层间守恒
    ods = q(f"""SELECT count(*) AS total,
                       count_if(dt BETWEEN '{config.WINDOW_START}' AND '{config.WINDOW_END}') AS in_window,
                       count(DISTINCT IF(dt < '{config.WINDOW_START}' OR dt > '{config.WINDOW_END}', dt, NULL)) AS bad_parts
                FROM {db}.ods_user_behavior_inc""")[0]
    dwd = q(f"SELECT count(*) AS c FROM {db}.dwd_user_behavior_inc")[0].c
    details = q(f"""SELECT detail FROM {db}.dq_check_result
                    WHERE check_name = 'dwd_user_behavior_inc.row_conservation'""")
    invalid = dup = 0
    for d in details:
        kv = dict(x.split("=") for x in d.detail.split())
        invalid += int(kv["invalid"])
        dup += int(kv["duplicate"])
    L += ["## ODS → DWD 行数守恒", ""]
    L += table_md([
        ("ODS 总行数（= 源 CSV 行数）", ods.total),
        (f"越界时间戳（落在 {ods.bad_parts} 个窗口外日期分区）", ods.total - ods.in_window),
        ("窗口内行数", ods.in_window),
        ("窗口内非法行（空值/非法枚举）", invalid),
        ("窗口内重复行", dup),
        ("DWD 行数", dwd),
        ("核对：窗口内 - 非法 - 重复 - DWD", ods.in_window - invalid - dup - dwd),
    ], ["项", "行数"])

    # ---------------- 各表行数
    L += ["", "## 各表行数", ""]
    rows = []
    for t in ONCE_TASKS + DAILY_TASKS:
        r = q(f"SELECT count(*) AS c FROM {db}.{t.name}")[0].c
        parts = len(q(f"SHOW PARTITIONS {db}.{t.name}")) if t.name not in ("dim_date", "dim_user_level") else "-"
        rows.append((t.layer, t.name, r, parts))
    L += table_md(rows, ["层", "表", "总行数", "分区数"])

    # ---------------- 耗时
    runs = latest_task_runs(env.name)
    by_task = defaultdict(lambda: [0.0, 0.0, 0])
    for (task, dt), rec in runs.items():
        by_task[task][0] += rec["seconds"]
        by_task[task][1] += rec.get("check_seconds", 0)
        by_task[task][2] += 1
    L += ["", "## 任务耗时（最近一次成功运行，按天任务为 9 天合计）", ""]
    rows = [(TASK_LAYER.get(t, ""), t, n, round(s, 1), round(c, 1)) for t, (s, c, n) in by_task.items()]
    total_s = sum(r[3] for r in rows)
    total_c = sum(r[4] for r in rows)
    rows.append(("", "合计", sum(r[2] for r in rows), round(total_s, 1), round(total_c, 1)))
    L += table_md(rows, ["层", "任务", "执行次数", "计算耗时(s)", "质检耗时(s)"])

    # ---------------- 质检
    dq = q(f"""SELECT severity, count(*) AS n, count_if(passed) AS passed
               FROM {db}.dq_check_result GROUP BY severity ORDER BY severity""")
    by_type = q(f"""SELECT check_type, count(*) AS n, count_if(passed) AS passed
                    FROM {db}.dq_check_result GROUP BY check_type ORDER BY n DESC""")
    L += ["", "## 质检闸门", ""]
    L += table_md([(r.severity, r.n, r.passed) for r in dq], ["级别", "执行次数", "通过"])
    L += [""]
    L += table_md([(r.check_type, r.n, r.passed) for r in by_type], ["类型", "执行次数", "通过"])
    warns = q(f"SELECT check_name, detail FROM {db}.dq_check_result WHERE NOT passed")
    if warns:
        L += ["", "未通过（warn 级，不阻断）：", ""] + [f"- `{w.check_name}`：{w.detail}" for w in warns]

    # ---------------- ADS
    L += ["", "## ADS 指标", "", "### 每日流量", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT dt, day_name, pv_cnt, uv, active_user_cnt, buy_cnt, buy_user_cnt, pv_per_uv, buy_user_rate
        FROM {db}.ads_traffic_stats_1d ORDER BY dt""")],
        ["日期", "星期", "PV", "UV", "活跃用户", "购买次数", "购买用户", "人均浏览", "购买用户占比"])
    L += ["", "### 有序转化漏斗", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT dt, step1_pv_users, step2_cart_fav_users, step3_buy_users,
               pv_to_cart_fav_rate, cart_fav_to_buy_rate, pv_to_buy_rate
        FROM {db}.ads_conversion_funnel_1d ORDER BY dt""")],
        ["日期", "浏览", "加购/收藏", "购买", "浏览→加购收藏", "加购收藏→购买", "浏览→购买"])
    L += ["", "### 留存", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT cohort_date, cohort_type, retention_day, cohort_size, retained_cnt, retention_rate
        FROM {db}.ads_user_retention ORDER BY cohort_type, retention_day, cohort_date""")],
        ["同期群日期", "类型", "第N日", "人数", "留存人数", "留存率"])
    L += ["", "### 复购率（截至当天累计）", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT dt, buyer_cnt, repurchase_user_cnt_by_day, repurchase_rate_by_day,
               repurchase_user_cnt_by_times, repurchase_rate_by_times
        FROM {db}.ads_repurchase_rate_td ORDER BY dt""")],
        ["日期", "购买用户", "按天复购人数", "按天复购率", "按次复购人数", "按次复购率"])
    L += ["", "### 近 7 日连续 3 天活跃", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT dt, window_start, active_user_cnt, continuous_3d_user_cnt, continuous_3d_ratio
        FROM {db}.ads_user_continuous_active_7d ORDER BY dt""")],
        ["日期", "窗口起", "窗口内活跃", "连续3天活跃", "占比"])
    L += ["", "### 用户等级分布（从拉链表回溯）", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT dt, concat_ws(' / ', collect_list(concat(user_level, level_name, ' ', user_cnt))) AS dist
        FROM (SELECT * FROM {db}.ads_user_level_dist_1d ORDER BY dt, user_level) GROUP BY dt ORDER BY dt""")],
        ["日期", "分布"])
    last = config.WINDOW_END
    L += ["", f"### {last} 浏览量 Top10 商品", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT rk, CAST(item_id AS STRING), CAST(category_id AS STRING), pv_cnt, pv_user_cnt, buy_cnt
        FROM {db}.ads_item_topn_1d
        WHERE dt = '{last}' ORDER BY rk""")], ["名次", "商品", "类目", "浏览", "浏览用户", "购买"])
    L += ["", f"### {last} 购买次数 Top10 类目", ""]
    L += table_md([tuple(r) for r in q(f"""
        SELECT rk, CAST(category_id AS STRING), buy_cnt, buy_user_cnt, pv_cnt, pv_user_cnt, buy_user_rate
        FROM {db}.ads_category_topn_1d WHERE dt = '{last}' ORDER BY rk""")],
        ["名次", "类目", "购买次数", "购买用户", "浏览", "浏览用户", "购买用户/浏览用户"])

    zip_stats = q(f"""SELECT count(*) AS records, count(DISTINCT user_id) AS users,
                             count_if(dt = '{config.OPEN_END_DATE}') AS open_records
                      FROM {db}.dim_user_zip""")[0]
    snap_rows = q(f"SELECT count(*) AS c FROM {db}.dws_user_milestone_td")[0].c
    L += ["", "### 拉链表 vs 每日全量快照", "",
          f"- 拉链表 {zip_stats.records:,} 条记录覆盖 {zip_stats.users:,} 个用户、9 天的等级历史"
          f"（当前有效 {zip_stats.open_records:,} 条）；同样的信息用每日全量快照要存 {snap_rows:,} 行"
          f"（累积快照表 9 个分区的行数之和），拉链表是它的 {zip_stats.records / snap_rows:.1%}"]

    out = config.REPORT_DIR / f"run_report_{env.name}.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"写入 {out}")
    spark.stop()


TASK_LAYER = {t.name: t.layer for t in ONCE_TASKS + DAILY_TASKS}

if __name__ == "__main__":
    main()
