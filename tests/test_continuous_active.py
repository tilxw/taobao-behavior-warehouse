"""近 7 日连续 3 天活跃：日期减行号分组；窗口边界截断；同一天多次行为只算一天。"""
from warehouse import config
from tests.conftest import ev

TASKS = ["dwd_user_behavior_inc", "dws_user_action_1d", "ads_user_continuous_active_7d"]


def _days(user, days, behavior="pv"):
    return [ev(user, behavior, f"{d} 12:00:00") for d in days]


def test_continuous_active_with_window_boundary(wh):
    window = config.date_range("2017-11-25", "2017-12-03")
    events = (
        _days(1, ["2017-11-25", "2017-11-26", "2017-11-27"])                      # 连续 3 天
        + _days(2, ["2017-11-29", "2017-11-30", "2017-12-01"])                    # 连续 3 天
        + _days(3, ["2017-11-28", "2017-11-30", "2017-12-01"])                    # 中间断了一天
        + _days(4, ["2017-11-26", "2017-11-27"]) + _days(4, ["2017-11-26"], "cart")  # 只有 2 天（同日多次）
        + _days(5, window)                                                        # 每天都活跃
        + _days(6, ["2017-11-25", "2017-11-26", "2017-11-27", "2017-11-28"])      # 连续 4 天
    )
    wh.load(events)
    wh.run("2017-11-25", "2017-12-03", tasks=TASKS)

    got = {r.dt: (r.window_start, r.active_user_cnt, r.continuous_3d_user_cnt)
           for r in wh.rows("SELECT * FROM {db}.ads_user_continuous_active_7d")}
    assert got["2017-11-26"] == ("2017-11-25", 4, 0)       # 数据才 2 天
    assert got["2017-11-27"] == ("2017-11-25", 4, 3)       # u1 u5 u6
    assert got["2017-12-01"] == ("2017-11-25", 6, 4)       # u1 u2 u5 u6；u3 断档、u4 只有 2 天
    # 12-03 的窗口是 11-27 ~ 12-03：u1 只剩 11-27 一天、u6 只剩 11-27/28 两天，被窗口截断后不算
    assert got["2017-12-03"] == ("2017-11-27", 6, 2)       # u2 u5
