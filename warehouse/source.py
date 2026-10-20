"""源数据：无表头 CSV，按固定 schema 读成临时视图 src_user_behavior。"""
from pathlib import Path

from pyspark.sql import SparkSession

SOURCE_SCHEMA = "user_id BIGINT, item_id BIGINT, category_id BIGINT, behavior_type STRING, ts BIGINT"


def register_source_view(spark: SparkSession, csv_path: Path, view: str = "src_user_behavior") -> None:
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"源文件不存在：{csv_path}")
    (spark.read
        .schema(SOURCE_SCHEMA)
        .option("header", "false")
        .option("mode", "PERMISSIVE")   # 解析失败的字段置 NULL，交给 DWD 过滤并计数，不在 ODS 丢行
        .csv(str(csv_path))
        .createOrReplaceTempView(view))
