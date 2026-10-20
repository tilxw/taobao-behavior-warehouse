"""全局配置：路径、数据时间窗口、运行环境。"""
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SQL_DIR = PROJECT_ROOT / "sql"
LOG_DIR = PROJECT_ROOT / "logs"
REPORT_DIR = PROJECT_ROOT / "reports"

TIMEZONE = "Asia/Shanghai"
BEIJING = timezone(timedelta(hours=8))

# 官方说明的数据范围：北京时间 2017-11-25 00:00:00 ~ 2017-12-03 23:59:59
WINDOW_START = "2017-11-25"
WINDOW_END = "2017-12-03"

# 拉链表里"当前有效"记录的结束日期
OPEN_END_DATE = "9999-12-31"

BEHAVIOR_TYPES = ("pv", "fav", "cart", "buy")


@dataclass(frozen=True)
class Env:
    """一套独立的运行环境：源文件 + Hive 库。样本和全量互不覆盖。"""
    name: str
    source_csv: Path
    database: str


ENVS = {
    "sample": Env("sample", DATA_DIR / "sample" / "UserBehavior_1m.csv", "tb_sample"),
    "full": Env("full", DATA_DIR / "raw" / "UserBehavior.csv", "tb_full"),
}


def day_start_ts(dt: str) -> int:
    """北京时间某天 00:00:00 对应的 Unix 秒。"""
    d = datetime.strptime(dt, "%Y-%m-%d").replace(tzinfo=BEIJING)
    return int(d.timestamp())


WINDOW_START_TS = day_start_ts(WINDOW_START)                 # 1511539200
WINDOW_END_TS = day_start_ts(WINDOW_END) + 86400             # 1512316800，开区间


def shift(dt: str, days: int) -> str:
    return (date.fromisoformat(dt) + timedelta(days=days)).isoformat()


def date_range(start: str, end: str) -> list[str]:
    out, cur = [], start
    while cur <= end:
        out.append(cur)
        cur = shift(cur, 1)
    return out


def spark_master() -> str:
    return os.environ.get("TBW_SPARK_MASTER", "local[8]")


def driver_memory() -> str:
    return os.environ.get("TBW_DRIVER_MEMORY", "6g")
