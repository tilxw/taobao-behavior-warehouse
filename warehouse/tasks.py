"""任务定义：每个任务产出一张表，声明它要读哪些表的哪些分区（相对当天的偏移）。

调度器据此做两件事：
1. 同一天内按依赖拓扑排序（偏移为 0 的上游必须先跑）；
2. 跑之前检查上游分区是否存在，缺了就报错，不带着空数据往下算。
偏移为负的依赖（如昨天的快照）要求按日期顺序回填，自依赖任务回填某天后要把之后的日期一并重跑。
"""
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import SparkSession

from warehouse import checks as C
from warehouse import config
from warehouse.sql_runner import run_sql_file

ZIP_TABLE = "dim_user_zip"
ZIP_WATERMARK_KEY = "etl.processed_through"


@dataclass(frozen=True)
class Task:
    name: str                       # 任务名 = 产出表名
    layer: str
    sql: tuple[str, ...] = ()       # 依次执行的 SQL 文件
    reads: tuple[tuple[str, int], ...] = ()   # (上游表, 相对当天的日期偏移)
    checks: tuple = ()
    runner: Callable | None = None  # 需要 Python 步骤的任务
    once: bool = False              # True：初始化时跑一次，不按天
    count_partition: bool = True    # 是否按 dt 统计产出行数
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------- 表属性（拉链表处理水位）
def get_table_property(spark: SparkSession, db: str, table: str, key: str) -> str | None:
    for row in spark.sql(f"SHOW TBLPROPERTIES {db}.{table}").collect():
        if row["key"] == key:
            return row["value"]
    return None


def set_table_property(spark: SparkSession, db: str, table: str, key: str, value: str) -> None:
    spark.sql(f"ALTER TABLE {db}.{table} SET TBLPROPERTIES ('{key}' = '{value}')")


def list_partitions(spark: SparkSession, db: str, table: str) -> list[str]:
    return [r[0].split("=", 1)[1] for r in spark.sql(f"SHOW PARTITIONS {db}.{table}").collect()]


# ---------------------------------------------------------------- 需要 Python 步骤的任务
def run_ods(spark: SparkSession, ctx, params: dict) -> None:
    from warehouse.source import register_source_view
    if ctx.source_csv is not None:
        register_source_view(spark, ctx.source_csv)
    run_sql_file(spark, "ods/ods_user_behavior_inc.sql", params)


def run_dim_user_zip(spark: SparkSession, ctx, params: dict) -> None:
    """拉链表：算中间表 → 删掉 dt >= D-1 的旧分区 → 中间表写回 → 更新处理水位。"""
    db, dt, dt_prev = params["db"], params["dt"], params["dt_prev"]
    run_sql_file(spark, "dim/dim_user_zip_stage.sql", params)
    # 中间表已经物化，可以安全删除旧分区；dt >= D-1 的分区会由中间表完整重建
    for part in list_partitions(spark, db, ZIP_TABLE):
        if part >= dt_prev:
            spark.sql(f"ALTER TABLE {db}.{ZIP_TABLE} DROP IF EXISTS PARTITION (dt = '{part}')")
    run_sql_file(spark, "dim/dim_user_zip_load.sql", params)
    set_table_property(spark, db, ZIP_TABLE, ZIP_WATERMARK_KEY, dt)


# ---------------------------------------------------------------- 任务清单
ONCE_TASKS = [
    Task("dim_date", "DIM", sql=("dim/dim_date.sql",), checks=tuple(C.DIM_DATE_CHECKS),
         once=True, count_partition=False),
    Task("dim_user_level", "DIM", sql=("dim/dim_user_level.sql",), checks=tuple(C.DIM_USER_LEVEL_CHECKS),
         once=True, count_partition=False),
    Task("ods_user_behavior_inc", "ODS", runner=run_ods, checks=tuple(C.ODS_CHECKS),
         once=True, count_partition=False),
]

DAILY_TASKS = [
    Task("dwd_user_behavior_inc", "DWD", sql=("dwd/dwd_user_behavior_inc.sql",),
         reads=(("ods_user_behavior_inc", 0),), checks=tuple(C.DWD_CHECKS)),
    Task("dim_item_full", "DIM", sql=("dim/dim_item_full.sql",),
         reads=(("dwd_user_behavior_inc", 0), ("dim_item_full", -1)), checks=tuple(C.DIM_ITEM_CHECKS)),
    Task("dws_user_action_1d", "DWS", sql=("dws/dws_user_action_1d.sql",),
         reads=(("dwd_user_behavior_inc", 0),), checks=tuple(C.DWS_USER_1D_CHECKS)),
    Task("dws_item_action_1d", "DWS", sql=("dws/dws_item_action_1d.sql",),
         reads=(("dwd_user_behavior_inc", 0), ("dim_item_full", 0)), checks=tuple(C.DWS_ITEM_1D_CHECKS)),
    Task("dws_user_milestone_td", "DWS", sql=("dws/dws_user_milestone_td.sql",),
         reads=(("dws_user_action_1d", 0), ("dws_user_milestone_td", -1)), checks=tuple(C.DWS_MILESTONE_CHECKS)),
    Task("dim_user_zip", "DIM", runner=run_dim_user_zip,
         reads=(("dws_user_milestone_td", 0), ("dim_user_zip", -1)), checks=tuple(C.DIM_USER_ZIP_CHECKS),
         count_partition=False),
    Task("ads_traffic_stats_1d", "ADS", sql=("ads/ads_traffic_stats_1d.sql",),
         reads=(("dws_user_action_1d", 0),), checks=tuple(C.ADS_TRAFFIC_CHECKS)),
    Task("ads_conversion_funnel_1d", "ADS", sql=("ads/ads_conversion_funnel_1d.sql",),
         reads=(("dwd_user_behavior_inc", 0), ("ads_traffic_stats_1d", 0)), checks=tuple(C.ADS_FUNNEL_CHECKS)),
    Task("ads_user_retention", "ADS", sql=("ads/ads_user_retention.sql",),
         reads=(("dws_user_action_1d", 0), ("dws_user_action_1d", -1), ("dws_user_action_1d", -3),
                ("dws_user_milestone_td", 0)), checks=tuple(C.ADS_RETENTION_CHECKS)),
    Task("ads_repurchase_rate_td", "ADS", sql=("ads/ads_repurchase_rate_td.sql",),
         reads=(("dws_user_milestone_td", 0),), checks=tuple(C.ADS_REPURCHASE_CHECKS)),
    Task("ads_item_topn_1d", "ADS", sql=("ads/ads_item_topn_1d.sql",),
         reads=(("dws_item_action_1d", 0),), checks=tuple(C.ADS_ITEM_TOPN_CHECKS)),
    Task("ads_category_topn_1d", "ADS", sql=("ads/ads_category_topn_1d.sql",),
         reads=(("dwd_user_behavior_inc", 0),), checks=tuple(C.ADS_CATEGORY_TOPN_CHECKS)),
    Task("ads_user_continuous_active_7d", "ADS", sql=("ads/ads_user_continuous_active_7d.sql",),
         reads=tuple(("dws_user_action_1d", -k) for k in range(7)), checks=tuple(C.ADS_CONTINUOUS_CHECKS)),
    Task("ads_user_level_dist_1d", "ADS", sql=("ads/ads_user_level_dist_1d.sql",),
         reads=(("dim_user_zip", 0), ("dws_user_milestone_td", 0)), checks=tuple(C.ADS_LEVEL_DIST_CHECKS)),
]

TASKS = {t.name: t for t in ONCE_TASKS + DAILY_TASKS}


def daily_order(selected: list[str] | None = None) -> list[Task]:
    """按同一天内的依赖（偏移 0）做拓扑排序。"""
    names = [t.name for t in DAILY_TASKS] if not selected else selected
    unknown = [n for n in names if n not in TASKS or TASKS[n].once]
    if unknown:
        raise ValueError(f"未知的按天任务：{unknown}")
    wanted = set(names)
    order, done, visiting = [], set(), set()

    def visit(name: str):
        if name in done:
            return
        if name in visiting:
            raise ValueError(f"任务依赖成环：{name}")
        visiting.add(name)
        for table, offset in TASKS[name].reads:
            if offset == 0 and table in wanted and table != name:
                visit(table)
        visiting.discard(name)
        done.add(name)
        order.append(TASKS[name])

    for n in [t.name for t in DAILY_TASKS if t.name in wanted]:
        visit(n)
    return order


def build_params(db: str, dt: str | None) -> dict:
    p = {
        "db": db,
        "window_start": config.WINDOW_START,
        "window_end": config.WINDOW_END,
        "window_days": len(config.date_range(config.WINDOW_START, config.WINDOW_END)),
        "open_end_date": config.OPEN_END_DATE,
        "dt": dt or "ALL",
    }
    if dt:
        p.update({
            "dt_prev": config.shift(dt, -1),
            "dt_1d_ago": config.shift(dt, -1),
            "dt_3d_ago": config.shift(dt, -3),
            "dt_6d_ago": config.shift(dt, -6),
            "day_start_ts": config.day_start_ts(dt),
            "day_end_ts": config.day_start_ts(dt) + 86400,
        })
    return p
