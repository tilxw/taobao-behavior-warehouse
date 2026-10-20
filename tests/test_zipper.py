"""用户等级拉链表：等级变化的开链/闭链、回溯任意一天、重跑回滚后结果不变。"""
import pytest

from warehouse.run import MissingUpstreamError
from tests.conftest import ev

TASKS = ["dwd_user_behavior_inc", "dws_user_action_1d", "dws_user_milestone_td",
         "dim_user_zip", "ads_user_level_dist_1d"]
OPEN = "9999-12-31"


def _events():
    return [
        # u1：11-25 买 1 次(L1) → 11-26 再买 1 次(累计 2，L2) → 11-27 只浏览 → 11-28 买 3 次(累计 5，L3)
        ev(1, "buy", "2017-11-25 10:00:00"), ev(1, "buy", "2017-11-26 10:00:00"),
        ev(1, "pv", "2017-11-27 10:00:00"),
        ev(1, "buy", "2017-11-28 10:00:00", item=101), ev(1, "buy", "2017-11-28 10:00:00", item=102),
        ev(1, "buy", "2017-11-28 10:00:00", item=103),
        # u2：每天只浏览，始终 L0，只有一条记录
        *[ev(2, "pv", f"2017-11-{d} 09:00:00") for d in (25, 26, 27, 28)],
        # u3：11-27 第一次出现就买了 2 件 → 直接 L2
        ev(3, "buy", "2017-11-27 11:00:00", item=201), ev(3, "buy", "2017-11-27 11:00:00", item=202),
        # u4：11-25 只浏览(L0)，11-26 首购(L1)
        ev(4, "pv", "2017-11-25 12:00:00"), ev(4, "buy", "2017-11-26 12:00:00"),
    ]


def _zip(wh):
    return sorted((r.user_id, r.user_level, r.start_date, r.end_date)
                  for r in wh.rows("SELECT * FROM {db}.dim_user_zip"))


def _as_of(wh, day):
    return dict((r.user_id, r.user_level) for r in wh.rows(
        f"SELECT user_id, user_level FROM {{db}}.dim_user_zip WHERE dt >= '{day}' AND start_date <= '{day}'"))


EXPECTED = [
    (1, "L1", "2017-11-25", "2017-11-25"),
    (1, "L2", "2017-11-26", "2017-11-27"),
    (1, "L3", "2017-11-28", OPEN),
    (2, "L0", "2017-11-25", OPEN),
    (3, "L2", "2017-11-27", OPEN),
    (4, "L0", "2017-11-25", "2017-11-25"),
    (4, "L1", "2017-11-26", OPEN),
]


def test_zipper_history_and_backtrack(wh):
    wh.load(_events())
    wh.run("2017-11-25", "2017-11-28", tasks=TASKS)

    assert _zip(wh) == EXPECTED
    # 按 end_date 分区：当前有效的都在 9999-12-31，已关闭的在各自 end_date 分区
    parts = {r[0] for r in wh.rows("SHOW PARTITIONS {db}.dim_user_zip")}
    assert parts == {"dt=2017-11-25", "dt=2017-11-27", f"dt={OPEN}"}

    assert _as_of(wh, "2017-11-25") == {1: "L1", 2: "L0", 4: "L0"}          # u3 还没出现
    assert _as_of(wh, "2017-11-27") == {1: "L2", 2: "L0", 3: "L2", 4: "L1"}
    dist = {r.user_level: r.user_cnt for r in wh.rows(
        "SELECT * FROM {db}.ads_user_level_dist_1d WHERE dt = '2017-11-26'")}
    assert dist == {"L0": 1, "L1": 1, "L2": 1}


def test_zipper_rerun_rolls_back_then_replays(wh):
    wh.load(_events())
    wh.run("2017-11-25", "2017-11-28", tasks=TASKS)

    # 只重跑 11-26：拉链表回滚到 11-26 收盘的状态（u3 还没出现，u1 的 L2 仍是开链）
    wh.run("2017-11-26", tasks=["dim_user_zip"])
    assert _zip(wh) == [
        (1, "L1", "2017-11-25", "2017-11-25"),
        (1, "L2", "2017-11-26", OPEN),
        (2, "L0", "2017-11-25", OPEN),
        (4, "L0", "2017-11-25", "2017-11-25"),
        (4, "L1", "2017-11-26", OPEN),
    ]
    # 依赖拉链表当天状态的任务，在水位落后时会拒绝执行
    with pytest.raises(MissingUpstreamError):
        wh.run("2017-11-28", tasks=["ads_user_level_dist_1d"])

    # 接着把 11-27、11-28 重跑一遍，结果和第一次完全一致；同一天连跑两次也不会重复
    wh.run("2017-11-27", "2017-11-28", tasks=["dim_user_zip"])
    wh.run("2017-11-28", tasks=["dim_user_zip"])
    assert _zip(wh) == EXPECTED


def test_zipper_refuses_to_skip_days(wh):
    wh.load(_events())
    wh.run("2017-11-25", "2017-11-26", tasks=TASKS[:3])
    wh.run("2017-11-25", tasks=["dim_user_zip"])
    # 11-26 没跑就直接跑 11-27：上游缺失，拒绝执行
    wh.run("2017-11-27", tasks=TASKS[:3])
    with pytest.raises(MissingUpstreamError):
        wh.run("2017-11-27", tasks=["dim_user_zip"])
