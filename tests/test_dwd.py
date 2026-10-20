"""DWD 清洗：去重、越界时间、非法枚举、空值、北京时间换算。"""
from tests.conftest import ev, ts


def test_dwd_cleaning_and_timezone(wh):
    events = [
        ev(1, "pv", "2017-11-25 10:00:00"),
        ev(1, "pv", "2017-11-25 10:00:00"),                # 完全重复 → 去掉
        (2, 100, 10, "pv", 1511539200),                    # 北京时间 11-25 00:00:00，窗口第一秒 → 保留
        (3, 100, 10, "pv", 1511539199),                    # 北京时间 11-24 23:59:59 → 落 ODS 的 11-24 分区，不进 DWD
        (4, 100, 10, "pv", 1512316800),                    # 北京时间 12-04 00:00:00 → 越界
        ev(5, "click", "2017-11-25 11:00:00"),             # 非法行为类型 → 去掉
        (6, None, 10, "pv", ts("2017-11-25 12:00:00")),    # 商品为空 → 去掉
        ev(7, "buy", "2017-11-25 23:59:59"),               # 当天最后一秒 → 保留
    ]
    wh.load(events)

    ods_parts = {r[0] for r in wh.rows("SHOW PARTITIONS {db}.ods_user_behavior_inc")}
    assert {"dt=2017-11-24", "dt=2017-11-25", "dt=2017-12-04"} <= ods_parts
    assert wh.one("SELECT count(*) AS c FROM {db}.ods_user_behavior_inc").c == len(events)   # ODS 不丢行

    wh.run("2017-11-25", tasks=["dwd_user_behavior_inc"])

    rows = wh.rows("""
        SELECT user_id, behavior_type, date_format(event_time, 'yyyy-MM-dd HH:mm:ss') AS t, event_hour
        FROM {db}.dwd_user_behavior_inc WHERE dt = '2017-11-25' ORDER BY user_id""")
    assert [(r.user_id, r.behavior_type, r.t, r.event_hour) for r in rows] == [
        (1, "pv", "2017-11-25 10:00:00", 10),
        (2, "pv", "2017-11-25 00:00:00", 0),
        (7, "buy", "2017-11-25 23:59:59", 23),
    ]

    # 质检结果里记录了守恒明细：ODS 当天 6 行 = DWD 3 + 非法 2 + 重复 1
    dq = wh.one("""SELECT detail, passed FROM {db}.dq_check_result
                   WHERE dt = '2017-11-25' AND check_name = 'dwd_user_behavior_inc.row_conservation'""")
    assert dq.passed
    assert dq.detail == "ods=6 invalid=2 duplicate=1 dwd=3"


def test_rerun_same_day_does_not_duplicate(wh):
    wh.load([ev(1, "pv", "2017-11-25 10:00:00"), ev(2, "buy", "2017-11-25 11:00:00")])
    tasks = ["dwd_user_behavior_inc", "dws_user_action_1d", "ads_traffic_stats_1d"]
    wh.run("2017-11-25", tasks=tasks)
    wh.run("2017-11-25", tasks=tasks)   # 重跑：INSERT OVERWRITE 覆盖分区
    assert wh.one("SELECT count(*) AS c FROM {db}.dwd_user_behavior_inc").c == 2
    assert wh.one("SELECT count(*) AS c FROM {db}.dws_user_action_1d").c == 2
    t = wh.one("SELECT pv_cnt, uv, active_user_cnt, buy_user_cnt FROM {db}.ads_traffic_stats_1d")
    assert (t.pv_cnt, t.uv, t.active_user_cnt, t.buy_user_cnt) == (1, 1, 2, 1)
    # 质检结果按 (dt, task) 覆盖，重跑后不会多出一份
    assert wh.one("""SELECT count(*) AS c FROM {db}.dq_check_result
                     WHERE task = 'dwd_user_behavior_inc'""").c == 5
