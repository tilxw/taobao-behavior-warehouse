"""不需要 Spark 的纯 Python 单元测试。"""
import pytest

from warehouse import config
from warehouse.sql_runner import render, split_statements
from warehouse.tasks import daily_order


def test_window_boundaries_are_beijing_midnight():
    assert config.day_start_ts("2017-11-25") == 1511539200      # 2017-11-24 16:00:00 UTC
    assert config.WINDOW_END_TS == 1512316800                   # 2017-12-04 00:00:00 北京时间
    assert config.date_range("2017-11-30", "2017-12-02") == ["2017-11-30", "2017-12-01", "2017-12-02"]


def test_split_statements_strips_comments():
    sql = "-- 注释里的 ; 不算\nSELECT 1;\n\nSELECT 2 -- 行尾注释\n;"
    assert split_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_render_fails_on_missing_param():
    assert render("SELECT * FROM ${db}.t WHERE dt = '${dt}'", {"db": "x", "dt": "d"}) == \
        "SELECT * FROM x.t WHERE dt = 'd'"
    with pytest.raises(KeyError):
        render("SELECT ${missing}", {})


def test_daily_order_respects_same_day_dependencies():
    order = [t.name for t in daily_order()]
    pos = {n: i for i, n in enumerate(order)}
    assert pos["dwd_user_behavior_inc"] < pos["dim_item_full"] < pos["dws_item_action_1d"]
    assert pos["dws_user_action_1d"] < pos["dws_user_milestone_td"] < pos["dim_user_zip"]
    assert pos["dim_user_zip"] < pos["ads_user_level_dist_1d"]
    assert pos["ads_traffic_stats_1d"] < pos["ads_conversion_funnel_1d"]


def test_daily_order_subset_keeps_order():
    names = [t.name for t in daily_order(["ads_traffic_stats_1d", "dws_user_action_1d"])]
    assert names == ["dws_user_action_1d", "ads_traffic_stats_1d"]
