"""读取 .sql 文件、替换 ${参数}、按分号拆成多条语句依次执行。"""
import re
from pathlib import Path
from string import Template

from pyspark.sql import DataFrame, SparkSession

from warehouse import config

_LINE_COMMENT = re.compile(r"--[^\n]*")


def render(sql: str, params: dict) -> str:
    # Template.substitute 遇到缺失的参数直接报错，防止带着 ${xxx} 原样跑到 Spark
    return Template(sql).substitute(params)


def split_statements(sql: str) -> list[str]:
    """去掉 -- 行注释后按分号拆分。本项目的 SQL 里不在字符串常量中使用 -- 和 ;"""
    body = _LINE_COMMENT.sub("", sql)
    return [s.strip() for s in body.split(";") if s.strip()]


def load_sql(rel_path: str) -> str:
    return (config.SQL_DIR / rel_path).read_text(encoding="utf-8")


def run_sql_text(spark: SparkSession, sql: str, params: dict) -> DataFrame | None:
    # 先去注释再替换参数，注释里出现 $ 也不会影响
    last = None
    for stmt in split_statements(sql):
        last = spark.sql(render(stmt, params))
    return last


def run_sql_file(spark: SparkSession, rel_path: str, params: dict) -> DataFrame | None:
    return run_sql_text(spark, load_sql(rel_path), params)


def sql_path(rel_path: str) -> Path:
    return config.SQL_DIR / rel_path
