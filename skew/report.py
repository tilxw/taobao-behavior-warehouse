"""把 reports/skew_experiment.json 整理成 reports/skew_experiment.md。"""
import json
import statistics

from warehouse import config

LABELS = {
    "agg_baseline_aqe_off": "聚合 · 直接 GROUP BY · AQE 关",
    "agg_salted_aqe_off": "聚合 · 加盐两阶段 · AQE 关",
    "agg_baseline_aqe_on": "聚合 · 直接 GROUP BY · AQE 开",
    "agg_salted_aqe_on": "聚合 · 加盐两阶段 · AQE 开",
    "join_smj_aqe_off": "关联 · SortMergeJoin · AQE 关",
    "join_smj_aqe_default": "关联 · SortMergeJoin · AQE 开（默认阈值）",
    "join_smj_aqe_tuned": "关联 · SortMergeJoin · AQE 开（阈值调低）",
    "join_salted_aqe_off": "关联 · 手工加盐 · AQE 关",
    "join_broadcast": "关联 · 广播小表",
}


def heaviest_stage(stages):
    readers = [s for s in stages if s["shuffle_read_records"] > 0] or stages
    return max(readers, key=lambda s: s["task_ms_min_p50_p95_max"][3])


def main():
    data = json.loads((config.REPORT_DIR / "skew_experiment.json").read_text(encoding="utf-8"))
    L = ["# 数据倾斜实验结果", "",
         f"- 生成时间 {data['generated_at']}，Spark `{data['spark_master']}`，盐 {data['salt_buckets']} 个桶",
         f"- DWD {data['dwd_rows']:,} 行；类目维表 {data['dim_category_rows']:,} 行",
         f"- 最热类目 {data['hottest_category']}：{data['hottest_category_rows']:,} 行"
         f"（{data['hottest_category_rows'] / data['dwd_rows']:.2%}），类目行数中位数 {data['median_category_rows']:,}",
         f"- 聚合各方案结果一致：{'是' if data['agg_results_equal'] else '否'}", "",
         "每个配置跑 3 次。\"最重阶段\"指读 shuffle 的阶段里最长 task 最长的那个，即聚合 / 关联真正发生的阶段；"
         "task 时长和读入行数取第 1 次运行之后各次的中位数。", "",
         "| 配置 | 耗时中位数(s) | 3 次耗时(s) | 最重阶段 task 数 | task 时长 p50 / p95 / max (ms) | max / p50 | "
         "task 读入行数 p50 / max | 执行计划 |",
         "|---|---|---|---|---|---|---|---|"]
    for run in data["runs"]:
        reps = run["reps"]
        secs = [r["seconds"] for r in reps]
        later = reps[1:] or reps
        stages = [heaviest_stage(r["stages"]) for r in later]
        p50 = statistics.median(s["task_ms_min_p50_p95_max"][1] for s in stages)
        p95 = statistics.median(s["task_ms_min_p50_p95_max"][2] for s in stages)
        mx = statistics.median(s["task_ms_min_p50_p95_max"][3] for s in stages)
        rec_p50 = stages[-1]["task_shuffle_read_records_min_p50_p95_max"][1]
        rec_max = stages[-1]["task_shuffle_read_records_min_p50_p95_max"][3]
        flags = reps[-1].get("plan_flags", {})
        plan = ("广播" if flags.get("broadcast_hash_join") else
                "SMJ + 倾斜拆分" if flags.get("skew_join_split") else
                "SMJ" if flags.get("sort_merge_join") else "聚合")
        L.append(f"| {LABELS.get(run['name'], run['name'])} | {statistics.median(secs):.2f} | "
                 f"{' / '.join(f'{s:.1f}' for s in secs)} | {stages[-1]['num_tasks']} | "
                 f"{p50:.0f} / {p95:.0f} / {mx:.0f} | {mx / max(p50, 1):.1f} | {rec_p50:,} / {rec_max:,} | {plan} |")
    out = config.REPORT_DIR / "skew_experiment.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))


if __name__ == "__main__":
    main()
