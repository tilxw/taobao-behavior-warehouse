"""测试夹具：一个测试会话共用一个本地 SparkSession（独立的临时元数据库），每个测试用自己的 Hive 库。

测试数据手工构造，走真实的 ODS 落地 → 按天任务 → 质检闸门，断言 ADS / DIM 的结果。
"""
import uuid
from datetime import datetime

import pytest

from warehouse import config
from warehouse.run import Context, create_tables, run_daily, run_task
from warehouse.source import SOURCE_SCHEMA
from warehouse.spark_session import build_spark
from warehouse.tasks import TASKS


def ts(text: str) -> int:
    """'2017-11-25 10:00:00'（北京时间）→ Unix 秒"""
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=config.BEIJING).timestamp())


def ev(user: int, behavior: str, when: str, item: int = 100, cat: int = 10) -> tuple:
    return (user, item, cat, behavior, ts(when))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    root = tmp_path_factory.mktemp("spark")
    session = build_spark(
        "tbw-tests",
        warehouse_dir=root / "warehouse",
        metastore_dir=root / "metastore_db",
        master="local[2]",
        extra_conf={
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "4",
            "spark.ui.enabled": "false",
        },
    )
    yield session
    session.stop()


class Warehouse:
    def __init__(self, spark, db):
        self.spark = spark
        self.db = db
        self.ctx = Context(db=db, source_csv=None, run_checks=True, quiet=True)

    def load(self, events: list[tuple]) -> None:
        """事件写入 ODS。测试数据只覆盖窗口里的几天，所以 ODS 的"窗口内每天都有数据"检查在这里不跑。"""
        self.spark.createDataFrame(events, SOURCE_SCHEMA).createOrReplaceTempView("src_user_behavior")
        no_check = Context(db=self.db, source_csv=None, run_checks=False, quiet=True)
        run_task(self.spark, no_check, TASKS["ods_user_behavior_inc"], None)

    def run(self, start: str, end: str | None = None, tasks: list[str] | None = None):
        return run_daily(self.spark, self.ctx, start, end or start, tasks)

    def rows(self, sql: str) -> list:
        return self.spark.sql(sql.format(db=self.db)).collect()

    def one(self, sql: str):
        return self.rows(sql)[0]


@pytest.fixture
def wh(spark):
    db = f"t_{uuid.uuid4().hex[:10]}"
    w = Warehouse(spark, db)
    create_tables(spark, w.ctx)
    for name in ("dim_date", "dim_user_level"):
        run_task(spark, w.ctx, TASKS[name], None)
    yield w
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
