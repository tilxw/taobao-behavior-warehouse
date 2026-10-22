"""转化漏斗的两种口径：当天 浏览 → 加购/收藏 → 购买。

strict（有序 / 严格）：步骤必须按时间先后完成，同一秒算先后成立。
non_strict（无序 / 非严格）：当天做过就算，不看先后；每一步仍是上一步的子集。
"""
from tests.conftest import ev

D1, D2 = "2017-11-25", "2017-11-26"
TASKS = ["dwd_user_behavior_inc", "dws_user_action_1d", "ads_traffic_stats_1d", "ads_conversion_funnel_1d"]


def _funnel(wh, dt):
    return {r.funnel_type: r for r in wh.rows(f"SELECT * FROM {{db}}.ads_conversion_funnel_1d WHERE dt = '{dt}'")}


def test_strict_and_non_strict_funnel_edge_cases(wh):
    events = [
        # u1 标准路径：浏览 → 加购 → 购买。strict 3 步，non_strict 3 步
        ev(1, "pv", f"{D1} 10:00:00"), ev(1, "cart", f"{D1} 10:05:00"), ev(1, "buy", f"{D1} 10:10:00"),
        # u2 浏览 → 收藏，没买。两种口径都是 2 步
        ev(2, "pv", f"{D1} 10:00:00"), ev(2, "fav", f"{D1} 10:01:00"),
        # u3 先加购、后浏览、再购买。strict 只有第 1 步（浏览之后没有加购/收藏）；non_strict 不看顺序，3 步
        ev(3, "cart", f"{D1} 09:00:00"), ev(3, "pv", f"{D1} 10:00:00"), ev(3, "buy", f"{D1} 11:00:00"),
        # u4 没浏览直接买。两种口径都一步不算
        ev(4, "buy", f"{D1} 09:00:00"),
        # u5 浏览 → 购买 → 加购。strict 2 步（购买在加购之前）；non_strict 3 步
        ev(5, "pv", f"{D1} 10:00:00"), ev(5, "buy", f"{D1} 10:30:00"), ev(5, "cart", f"{D1} 11:00:00"),
        # u6 三个行为在同一秒。两种口径都是 3 步（strict 含等号）
        ev(6, "pv", f"{D1} 10:00:00"), ev(6, "cart", f"{D1} 10:00:00"), ev(6, "buy", f"{D1} 10:00:00"),
        # u7 浏览前的加购不算，浏览后的收藏算，再购买。两种口径都是 3 步
        ev(7, "cart", f"{D1} 08:00:00"), ev(7, "pv", f"{D1} 09:00:00"),
        ev(7, "fav", f"{D1} 09:30:00"), ev(7, "buy", f"{D1} 09:45:00"),
        # u8 浏览在 D1 深夜，加购在 D2 凌晨。两种口径在 D1 都只有第 1 步；D2 没有浏览，一步都不算
        ev(8, "pv", f"{D1} 23:59:00"), ev(8, "cart", f"{D2} 00:01:00"),
        # u9 加购前买过一次、加购后又买了一次。两种口径都是 3 步
        ev(9, "pv", f"{D1} 10:00:00"), ev(9, "buy", f"{D1} 10:01:00"),
        ev(9, "cart", f"{D1} 10:05:00"), ev(9, "buy", f"{D1} 10:10:00"),
    ]
    wh.load(events)
    wh.run(D1, D2, tasks=TASKS)

    f1 = _funnel(wh, D1)
    assert set(f1) == {"strict", "non_strict"}
    s, n = f1["strict"], f1["non_strict"]
    # strict：第 1 步 u1 u2 u3 u5 u6 u7 u8 u9；第 2 步 u1 u2 u5 u6 u7 u9；第 3 步 u1 u6 u7 u9
    assert (s.step1_pv_users, s.step2_cart_fav_users, s.step3_buy_users) == (8, 6, 4)
    assert s.pv_to_cart_fav_rate == 0.75 and s.cart_fav_to_buy_rate == round(4 / 6, 4)
    assert s.pv_to_buy_rate == 0.5
    # non_strict：第 2 步比 strict 多 u3；第 3 步比 strict 多 u3、u5
    assert (n.step1_pv_users, n.step2_cart_fav_users, n.step3_buy_users) == (8, 7, 6)
    assert n.pv_to_cart_fav_rate == 0.875 and n.cart_fav_to_buy_rate == round(6 / 7, 4)
    assert n.pv_to_buy_rate == 0.75

    f2 = _funnel(wh, D2)
    for row in f2.values():
        assert (row.step1_pv_users, row.step2_cart_fav_users, row.step3_buy_users) == (0, 0, 0)
        assert row.pv_to_cart_fav_rate is None       # 分母为 0 时比率为 NULL，不报错


def test_order_only_matters_for_strict(wh):
    # 同一组行为，只是先后顺序不同：两种口径在"按顺序"时一致，"打乱顺序"时只有 strict 变少
    wh.load([
        ev(1, "pv", f"{D1} 10:00:00"), ev(1, "fav", f"{D1} 10:10:00"), ev(1, "buy", f"{D1} 10:20:00"),
        ev(2, "buy", f"{D1} 10:00:00"), ev(2, "fav", f"{D1} 10:10:00"), ev(2, "pv", f"{D1} 10:20:00"),
    ])
    wh.run(D1, tasks=TASKS)
    f = _funnel(wh, D1)
    s, n = f["strict"], f["non_strict"]
    assert (s.step1_pv_users, s.step2_cart_fav_users, s.step3_buy_users) == (2, 1, 1)   # 只有 u1
    assert (n.step1_pv_users, n.step2_cart_fav_users, n.step3_buy_users) == (2, 2, 2)   # u1、u2 都算
