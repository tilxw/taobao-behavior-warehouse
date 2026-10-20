"""质检闸门：error 级规则不通过会抛异常并阻断下游；warn 只记录；结果写进 dq_check_result。"""
import dataclasses

import pytest

from warehouse import run as runner
from warehouse.checks import custom
from warehouse.quality import QualityGateError
from warehouse.tasks import TASKS
from tests.conftest import ev


def test_failing_check_blocks_downstream(wh, monkeypatch):
    wh.load([ev(1, "pv", "2017-11-25 10:00:00")])
    always_fail = custom("always_fail", "consistency", "dwd_user_behavior_inc",
                         "SELECT 3 AS bad, 'injected failure' AS detail")
    broken_dwd = dataclasses.replace(TASKS["dwd_user_behavior_inc"],
                                     checks=TASKS["dwd_user_behavior_inc"].checks + (always_fail,))
    monkeypatch.setattr(runner, "daily_order",
                        lambda selected=None: [broken_dwd, TASKS["dws_user_action_1d"]])

    with pytest.raises(QualityGateError) as exc:
        wh.run("2017-11-25")
    assert "always_fail" in str(exc.value)

    # 失败的规则记进了结果表；下游 dws 没有执行，分区不存在
    r = wh.one("""SELECT bad_cnt, passed FROM {db}.dq_check_result
                  WHERE check_name = 'dwd_user_behavior_inc.always_fail'""")
    assert (r.bad_cnt, r.passed) == (3, False)
    assert wh.rows("SHOW PARTITIONS {db}.dws_user_action_1d") == []


def test_warn_check_does_not_block(wh, monkeypatch):
    wh.load([ev(1, "pv", "2017-11-25 10:00:00")])
    warn_only = custom("warn_only", "consistency", "dwd_user_behavior_inc",
                       "SELECT 1 AS bad, 'just a warning' AS detail", severity="warn")
    task = dataclasses.replace(TASKS["dwd_user_behavior_inc"], checks=(warn_only,))
    monkeypatch.setattr(runner, "daily_order", lambda selected=None: [task, TASKS["dws_user_action_1d"]])
    wh.run("2017-11-25")
    assert wh.one("SELECT count(*) AS c FROM {db}.dws_user_action_1d").c == 1


def test_missing_upstream_partition_is_refused(wh):
    wh.load([ev(1, "pv", "2017-11-25 10:00:00"), ev(1, "pv", "2017-11-26 10:00:00")])
    wh.run("2017-11-26", tasks=["dwd_user_behavior_inc", "dws_user_action_1d"])
    # 累积快照依赖昨天的快照：11-25 没跑过，11-26 不能直接算
    with pytest.raises(runner.MissingUpstreamError):
        wh.run("2017-11-26", tasks=["dws_user_milestone_td"])


def test_date_outside_window_is_refused(wh):
    with pytest.raises(ValueError):
        wh.run("2017-11-24")
