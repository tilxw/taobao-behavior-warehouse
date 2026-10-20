"""有序漏斗：当天 浏览 → 加购/收藏 → 购买，步骤必须按时间先后完成（同一秒算先后成立）。"""
from tests.conftest import ev

D1, D2 = "2017-11-25", "2017-11-26"


def test_ordered_funnel_edge_cases(wh):
    events = [
        # u1 标准路径：浏览 → 加购 → 购买 → 走完 3 步
        ev(1, "pv", f"{D1} 10:00:00"), ev(1, "cart", f"{D1} 10:05:00"), ev(1, "buy", f"{D1} 10:10:00"),
        # u2 浏览 → 收藏，没买 → 2 步
        ev(2, "pv", f"{D1} 10:00:00"), ev(2, "fav", f"{D1} 10:01:00"),
        # u3 先加购后浏览，浏览之后没有加购/收藏 → 只有第 1 步
        ev(3, "cart", f"{D1} 09:00:00"), ev(3, "pv", f"{D1} 10:00:00"), ev(3, "buy", f"{D1} 11:00:00"),
        # u4 没浏览直接买 → 一步都不算
        ev(4, "buy", f"{D1} 09:00:00"),
        # u5 浏览 → 购买 → 加购：购买发生在加购之前 → 2 步
        ev(5, "pv", f"{D1} 10:00:00"), ev(5, "buy", f"{D1} 10:30:00"), ev(5, "cart", f"{D1} 11:00:00"),
        # u6 三个行为在同一秒 → 3 步（含等号）
        ev(6, "pv", f"{D1} 10:00:00"), ev(6, "cart", f"{D1} 10:00:00"), ev(6, "buy", f"{D1} 10:00:00"),
        # u7 浏览前的加购不算，浏览后的收藏算，再购买 → 3 步
        ev(7, "cart", f"{D1} 08:00:00"), ev(7, "pv", f"{D1} 09:00:00"),
        ev(7, "fav", f"{D1} 09:30:00"), ev(7, "buy", f"{D1} 09:45:00"),
        # u8 浏览在 D1 深夜，加购在 D2 凌晨 → D1 只有第 1 步，D2 没有浏览一步都不算
        ev(8, "pv", f"{D1} 23:59:00"), ev(8, "cart", f"{D2} 00:01:00"),
        # u9 加购前买过一次、加购后又买了一次 → 3 步
        ev(9, "pv", f"{D1} 10:00:00"), ev(9, "buy", f"{D1} 10:01:00"),
        ev(9, "cart", f"{D1} 10:05:00"), ev(9, "buy", f"{D1} 10:10:00"),
    ]
    wh.load(events)
    wh.run(D1, D2, tasks=["dwd_user_behavior_inc", "dws_user_action_1d",
                          "ads_traffic_stats_1d", "ads_conversion_funnel_1d"])

    f1 = wh.one(f"SELECT * FROM {{db}}.ads_conversion_funnel_1d WHERE dt = '{D1}'")
    assert (f1.step1_pv_users, f1.step2_cart_fav_users, f1.step3_buy_users) == (8, 6, 4)
    assert f1.pv_to_cart_fav_rate == 0.75 and f1.cart_fav_to_buy_rate == round(4 / 6, 4)
    assert f1.pv_to_buy_rate == 0.5

    f2 = wh.one(f"SELECT * FROM {{db}}.ads_conversion_funnel_1d WHERE dt = '{D2}'")
    assert (f2.step1_pv_users, f2.step2_cart_fav_users, f2.step3_buy_users) == (0, 0, 0)
    assert f2.pv_to_cart_fav_rate is None       # 分母为 0 时比率为 NULL，不报错
