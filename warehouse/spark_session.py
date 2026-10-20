"""创建带 Hive 支持的本地 SparkSession。

元数据库用 Spark 自带的 Derby（单机 Hive Metastore），表数据以 Parquet 存在 spark-warehouse/ 下。
"""
import os
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from warehouse import config


def ensure_java_home() -> None:
    """Spark 4 需要 JDK 17/21。没有显式设置 JAVA_HOME 时，在本进程内指向 JDK 21，不改系统默认。"""
    if os.environ.get("JAVA_HOME"):
        return
    try:
        home = subprocess.check_output(["/usr/libexec/java_home", "-v", "21"], text=True).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("找不到 JDK 21，请手动设置 JAVA_HOME") from exc
    os.environ["JAVA_HOME"] = home


def build_spark(
    app_name: str = "taobao-behavior-warehouse",
    *,
    warehouse_dir: Path | None = None,
    metastore_dir: Path | None = None,
    master: str | None = None,
    extra_conf: dict | None = None,
) -> SparkSession:
    ensure_java_home()
    # Python worker 用当前解释器（.venv），否则会落到系统自带的 Python
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    warehouse_dir = Path(warehouse_dir or config.PROJECT_ROOT / "spark-warehouse")
    metastore_dir = Path(metastore_dir or config.PROJECT_ROOT / "metastore_db")
    derby_home = metastore_dir.parent

    conf = {
        "spark.master": master or config.spark_master(),
        "spark.driver.memory": config.driver_memory(),
        "spark.sql.session.timeZone": config.TIMEZONE,
        "spark.sql.warehouse.dir": str(warehouse_dir),
        "spark.hadoop.javax.jdo.option.ConnectionURL":
            f"jdbc:derby:;databaseName={metastore_dir};create=true",
        # derby.log 写到元数据库旁边；JVM 默认时区也设成北京时间，避免 Hive SerDe 与会话时区不一致
        "spark.driver.extraJavaOptions":
            f"-Dderby.system.home={derby_home} -Duser.timezone={config.TIMEZONE}",
        "spark.hadoop.hive.exec.dynamic.partition": "true",
        "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.adaptive.enabled": "true",
        # AQE 合并 shuffle 分区时按 64MB 目标大小合并，而不是优先保并行度，减少小文件
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
        "spark.ui.showConsoleProgress": "false",
    }
    conf.update(extra_conf or {})

    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
