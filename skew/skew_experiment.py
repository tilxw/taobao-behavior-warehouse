"""数据倾斜实验：热门类目造成的聚合倾斜和关联倾斜。

数据：全量 DWD（约 1 亿行）。探查结论是商品粒度基本不倾斜（最热商品只占 0.033%），
真正的热点在类目：最大类目 4756105 占 4.86%（486 万行），而类目行数的中位数只有 303 行。

实验一 聚合：按类目算行为数和去重用户数
  agg_baseline  直接 GROUP BY category_id
  agg_salted    两阶段：先按 (类目, 盐) 局部聚合，再按类目汇总；盐由 user_id 派生，保证同一用户只落一个桶
实验二 关联：DWD JOIN 类目维表（约 9.4k 行），关掉广播，模拟大表关联只能走 SortMergeJoin 的情况
  join_smj_aqe_off       AQE 关闭
  join_smj_aqe_default   AQE 开启 + 倾斜关联优化，默认阈值
  join_smj_aqe_tuned     AQE 开启 + 倾斜关联优化，阈值调低到本地数据量级
  join_salted            手工加盐：大表随机打散到 N 个桶，小表复制 N 份
  join_broadcast         广播小表（小维表的常规做法）

每个配置跑 3 次取中位数；task 时长分布从 Spark UI 的 REST API 读取（每次运行用 job group 区分）。
结果写到 reports/skew_experiment.json 和 reports/skew_experiment.md。

用法：python -m skew.skew_experiment [--db tb_full] [--repeat 3]
"""
import argparse
import json
import time
import urllib.request
from datetime import datetime

from warehouse import config
from warehouse.spark_session import build_spark

SALT = 16

AQE_OFF = {"spark.sql.adaptive.enabled": "false"}
AQE_DEFAULT_SKEW = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
}
AQE_TUNED_SKEW = dict(AQE_DEFAULT_SKEW, **{
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "16MB",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "16MB",
})
NO_BROADCAST = {"spark.sql.autoBroadcastJoinThreshold": "-1"}


def agg_baseline(db):
    return f"""
        SELECT category_id, count(*) AS cnt, count(DISTINCT user_id) AS uv
        FROM {db}.dwd_user_behavior_inc
        GROUP BY category_id"""


def agg_salted(db):
    return f"""
        SELECT category_id, sum(cnt) AS cnt, sum(uv) AS uv
        FROM (
            SELECT category_id, pmod(hash(user_id), {SALT}) AS salt,
                   count(*) AS cnt, count(DISTINCT user_id) AS uv
            FROM {db}.dwd_user_behavior_inc
            GROUP BY category_id, pmod(hash(user_id), {SALT})
        ) t
        GROUP BY category_id"""


def join_plain(db, hint="MERGE"):
    return f"""
        SELECT /*+ {hint}(c) */ d.user_id, d.item_id, d.behavior_type, d.event_ts, c.item_cnt
        FROM {db}.dwd_user_behavior_inc d
        JOIN {db}.tmp_skew_dim_category c ON d.category_id = c.category_id"""


def join_salted(db):
    return f"""
        SELECT /*+ MERGE(c) */ d.user_id, d.item_id, d.behavior_type, d.event_ts, c.item_cnt
        FROM (
            SELECT *, pmod(hash(user_id, item_id, event_ts), {SALT}) AS salt
            FROM {db}.dwd_user_behavior_inc
        ) d
        JOIN (
            SELECT c.category_id, c.item_cnt, s.salt
            FROM {db}.tmp_skew_dim_category c
            LATERAL VIEW explode(sequence(0, {SALT - 1})) s AS salt
        ) c
        ON d.category_id = c.category_id AND d.salt = c.salt"""


EXPERIMENTS = [
    # (名字, 分组, SQL 构造, 配置)
    ("agg_baseline_aqe_off", "agg", agg_baseline, AQE_OFF),
    ("agg_salted_aqe_off", "agg", agg_salted, AQE_OFF),
    ("agg_baseline_aqe_on", "agg", agg_baseline, {"spark.sql.adaptive.enabled": "true"}),
    ("agg_salted_aqe_on", "agg", agg_salted, {"spark.sql.adaptive.enabled": "true"}),
    ("join_smj_aqe_off", "join", join_plain, dict(NO_BROADCAST, **AQE_OFF)),
    ("join_smj_aqe_default", "join", join_plain, dict(NO_BROADCAST, **AQE_DEFAULT_SKEW)),
    ("join_smj_aqe_tuned", "join", join_plain, dict(NO_BROADCAST, **AQE_TUNED_SKEW)),
    ("join_salted_aqe_off", "join", join_salted, dict(NO_BROADCAST, **AQE_OFF)),
    ("join_broadcast", "join", lambda db: join_plain(db, "BROADCAST"), AQE_OFF),
]

# 每次运行前恢复成这组基线，避免上一个实验的配置残留
BASELINE_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
    "spark.sql.autoBroadcastJoinThreshold": "10MB",
    "spark.sql.shuffle.partitions": "200",
}


class UiApi:
    def __init__(self, spark):
        sc = spark.sparkContext
        self.base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"

    def get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=30) as resp:
            return json.loads(resp.read())

    def jobs_of_group(self, group):
        return [j for j in self.get("/jobs?status=succeeded") if j.get("jobGroup") == group]

    def final_plans_of_group(self, group):
        """AQE 运行结束后的最终物理计划（能看到是否触发了倾斜拆分、是否改成了广播）。"""
        job_ids = {j["jobId"] for j in self.jobs_of_group(group)}
        plans = []
        for ex in self.get("/sql?details=false&planDescription=true&offset=0&length=100000"):
            if job_ids & set(ex.get("successJobIds", [])):
                plans.append(ex.get("planDescription", ""))
        return plans

    def stages_of_group(self, group):
        stage_ids = set()
        for job in self.jobs_of_group(group):
            stage_ids.update(job["stageIds"])
        out = []
        for sid in sorted(stage_ids):
            for st in self.get(f"/stages/{sid}"):
                if st["status"] != "COMPLETE":
                    continue
                q = self.get(f"/stages/{sid}/{st['attemptId']}/taskSummary?quantiles=0.0,0.5,0.95,1.0")
                out.append({
                    "stage_id": sid,
                    "name": st["name"][:80],
                    "num_tasks": st["numTasks"],
                    "executor_run_ms": st["executorRunTime"],
                    "shuffle_read_records": st["shuffleReadRecords"],
                    "shuffle_write_records": st["shuffleWriteRecords"],
                    "shuffle_write_bytes": st["shuffleWriteBytes"],
                    "task_ms_min_p50_p95_max": [round(x) for x in q["executorRunTime"]],
                    "task_shuffle_read_records_min_p50_p95_max":
                        [round(x) for x in q["shuffleReadMetrics"]["readRecords"]],
                })
        return out


def run_one(spark, api, db, name, group, build_sql, conf, rep):
    for k, v in {**BASELINE_CONF, **conf}.items():
        spark.conf.set(k, v)
    job_group = f"{name}#{rep}"
    spark.sparkContext.setJobGroup(job_group, job_group)
    df = spark.sql(build_sql(db))
    t0 = time.perf_counter()
    if group == "agg":
        rows = df.collect()
        result = {"categories": len(rows), "cnt": sum(r.cnt for r in rows), "uv_sum": sum(r.uv for r in rows)}
    else:
        df.write.format("noop").mode("overwrite").save()   # 完整执行但不落盘，排除写文件的干扰
        result = None
    seconds = time.perf_counter() - t0
    time.sleep(1)   # 等 UI 的监听器把最后的 task 指标写完
    stages = api.stages_of_group(job_group)
    plans = api.final_plans_of_group(job_group)
    plan = "\n\n".join(plans)
    flags = {
        "skew_join_split": "isSkewJoin=true" in plan or "skew=true" in plan,
        "sort_merge_join": "SortMergeJoin" in plan,
        "broadcast_hash_join": "BroadcastHashJoin" in plan,
    }
    return {"seconds": round(seconds, 2), "result": result, "stages": stages, "plan_flags": flags, "plan": plan}


def heaviest_stage(stages):
    """读 shuffle 的阶段里，task 耗时最长的那个：聚合 / 关联真正发生的地方。"""
    readers = [s for s in stages if s["shuffle_read_records"] > 0] or stages
    return max(readers, key=lambda s: s["task_ms_min_p50_p95_max"][3])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=config.ENVS["full"].database)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--only", default=None, help="逗号分隔，只跑这些实验")
    args = parser.parse_args()

    spark = build_spark("skew-experiment", extra_conf={"spark.ui.enabled": "true",
                                                         "spark.ui.retainedJobs": "5000",
                                                         "spark.ui.retainedStages": "5000"})
    api = UiApi(spark)
    db = args.db

    # 类目维表：每个类目一行，带一个属性列（商品数），约 9.4k 行。
    # 落成真正的 Parquet 表，而不是 cache 的临时视图：cache 住的聚合结果自带"已按 category_id 分区"的属性，
    # SortMergeJoin 就只会 shuffle 大表一边，AQE 的倾斜拆分和分区合并都要求两边都是 shuffle 阶段，会整个跳过。
    spark.sql(f"DROP TABLE IF EXISTS {db}.tmp_skew_dim_category")
    spark.sql(f"""
        CREATE TABLE {db}.tmp_skew_dim_category STORED AS PARQUET AS
        SELECT category_id, count(*) AS item_cnt
        FROM {db}.dim_item_full WHERE dt = '{config.WINDOW_END}'
        GROUP BY category_id""")
    dim_rows = spark.sql(f"SELECT count(*) AS c FROM {db}.tmp_skew_dim_category").collect()[0].c

    profile = spark.sql(f"""
        SELECT count(*) AS total,
               max(c) AS max_cat_rows,
               percentile_approx(c, 0.5) AS median_cat_rows,
               max_by(category_id, c) AS hottest_category
        FROM (SELECT category_id, count(*) AS c FROM {db}.dwd_user_behavior_inc GROUP BY category_id) t
    """).collect()[0]
    total_rows = spark.sql(f"SELECT count(*) AS c FROM {db}.dwd_user_behavior_inc").collect()[0].c

    selected = set(args.only.split(",")) if args.only else None
    runs = []
    for name, group, build_sql, conf in EXPERIMENTS:
        if selected and name not in selected:
            continue
        reps = []
        for rep in range(args.repeat):
            r = run_one(spark, api, db, name, group, build_sql, conf, rep)
            hs = heaviest_stage(r["stages"])
            print(f"{name:<24} rep{rep} {r['seconds']:>7.2f}s  heaviest stage tasks={hs['num_tasks']} "
                  f"task_ms(min/p50/p95/max)={hs['task_ms_min_p50_p95_max']} "
                  f"read_records(p50/max)={hs['task_shuffle_read_records_min_p50_p95_max'][1]}/"
                  f"{hs['task_shuffle_read_records_min_p50_p95_max'][3]}", flush=True)
            reps.append(r)
        runs.append({"name": name, "group": group, "conf": conf, "reps": reps})

    # 正确性：加盐前后结果一致
    agg_results = {r["name"]: r["reps"][0]["result"] for r in runs if r["group"] == "agg"}
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "spark_master": config.spark_master(),
        "salt_buckets": SALT,
        "dwd_rows": total_rows,
        "dim_category_rows": dim_rows,
        "hottest_category": profile.hottest_category,
        "hottest_category_rows": profile.max_cat_rows,
        "median_category_rows": profile.median_cat_rows,
        "agg_results_equal": len({json.dumps(v, sort_keys=True) for v in agg_results.values()}) <= 1,
        "agg_results": agg_results,
        "runs": runs,
    }
    out = config.REPORT_DIR / "skew_experiment.json"
    if selected and out.exists():
        # 只重跑了部分实验：其余实验保留上次的结果
        old = json.loads(out.read_text(encoding="utf-8"))
        rerun = {r["name"] for r in runs}
        order = [name for name, *_ in EXPERIMENTS]
        merged = [r for r in old["runs"] if r["name"] not in rerun] + runs
        summary["runs"] = sorted(merged, key=lambda r: order.index(r["name"]))
        summary["agg_results"] = old.get("agg_results", {}) | agg_results
        summary["agg_results_equal"] = len({json.dumps(v, sort_keys=True)
                                            for v in summary["agg_results"].values()}) <= 1
    # stage 名和执行计划里带本机绝对路径，入库前换成相对项目根目录的路径
    out.write_text(config.scrub_local_paths(json.dumps(summary, ensure_ascii=False, indent=1)), encoding="utf-8")
    print(f"写入 {out}")
    spark.stop()


if __name__ == "__main__":
    main()
