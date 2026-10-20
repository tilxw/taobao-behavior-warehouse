"""复购率的两种口径、TopN 的并列处理。"""
from tests.conftest import ev


def test_repurchase_by_day_vs_by_times(wh):
    wh.load([
        ev(1, "buy", "2017-11-25 10:00:00", item=1), ev(1, "buy", "2017-11-25 10:00:05", item=2),  # 同一天买两件
        ev(2, "buy", "2017-11-25 10:00:00"), ev(2, "buy", "2017-11-26 10:00:00"),                  # 两天各买一次
        ev(3, "buy", "2017-11-26 10:00:00"),                                                        # 只买一次
        ev(4, "pv", "2017-11-25 10:00:00"), ev(4, "pv", "2017-11-26 10:00:00"),                    # 没买过
    ])
    wh.run("2017-11-25", "2017-11-26", tasks=["dwd_user_behavior_inc", "dws_user_action_1d",
                                               "dws_user_milestone_td", "ads_repurchase_rate_td"])
    r = wh.one("SELECT * FROM {db}.ads_repurchase_rate_td WHERE dt = '2017-11-26'")
    assert r.buyer_cnt == 3
    assert (r.repurchase_user_cnt_by_day, r.repurchase_rate_by_day) == (1, 0.3333)      # 只有 u2
    assert (r.repurchase_user_cnt_by_times, r.repurchase_rate_by_times) == (2, 0.6667)  # u1、u2


def test_item_topn_tie_break_and_limit(wh):
    events = []
    # 12 个商品：101~112，浏览次数 101 最多；105 和 103 并列，按 item_id 升序
    pv_counts = {101: 9, 102: 7, 103: 5, 104: 6, 105: 5, 106: 4, 107: 3, 108: 3, 109: 2, 110: 2, 111: 1, 112: 1}
    for item, n in pv_counts.items():
        events += [ev(u, "pv", "2017-11-25 10:00:00", item=item) for u in range(1, n + 1)]
    wh.load(events)
    wh.run("2017-11-25", tasks=["dwd_user_behavior_inc", "dim_item_full", "dws_item_action_1d", "ads_item_topn_1d"])
    got = [(r.rk, r.item_id, r.pv_cnt) for r in wh.rows("SELECT * FROM {db}.ads_item_topn_1d ORDER BY rk")]
    assert got == [(1, 101, 9), (2, 102, 7), (3, 104, 6), (4, 103, 5), (5, 105, 5),
                   (6, 106, 4), (7, 107, 3), (8, 108, 3), (9, 109, 2), (10, 110, 2)]


def test_item_dim_takes_latest_category(wh):
    wh.load([
        ev(1, "pv", "2017-11-25 10:00:00", item=7, cat=70),
        ev(2, "pv", "2017-11-25 11:00:00", item=7, cat=71),     # 当天晚些时候换了类目
        ev(3, "pv", "2017-11-26 09:00:00", item=8, cat=80),     # 11-26 只出现商品 8
    ])
    wh.run("2017-11-25", "2017-11-26", tasks=["dwd_user_behavior_inc", "dim_item_full"])
    got = {(r.dt, r.item_id): (r.category_id, r.first_seen_date, r.last_seen_date)
           for r in wh.rows("SELECT * FROM {db}.dim_item_full")}
    assert got == {
        ("2017-11-25", 7): (71, "2017-11-25", "2017-11-25"),
        ("2017-11-26", 7): (71, "2017-11-25", "2017-11-25"),   # 快照保留昨天的商品
        ("2017-11-26", 8): (80, "2017-11-26", "2017-11-26"),
    }
