"""质检闸门：执行规则、把结果写进 dq_check_result，error 级别不通过就抛异常阻断下游。"""
from dataclasses import dataclass

from pyspark.sql import SparkSession

from warehouse.checks import Check
from warehouse.sql_runner import render

_RESULT_SCHEMA = ("check_name STRING, check_type STRING, severity STRING, target_table STRING, "
                  "bad_cnt BIGINT, detail STRING, passed BOOLEAN")


@dataclass
class CheckResult:
    check: Check
    bad: int | None
    detail: str

    @property
    def passed(self) -> bool:
        return self.bad == 0

    @property
    def blocking(self) -> bool:
        return not self.passed and self.check.severity == "error"


class QualityGateError(RuntimeError):
    def __init__(self, task: str, dt: str, failed: list[CheckResult]):
        self.task, self.dt, self.failed = task, dt, failed
        lines = [f"质检不通过：{task} dt={dt}"]
        lines += [f"  - {r.check.name}: bad={r.bad} {r.detail}" for r in failed]
        super().__init__("\n".join(lines))


def run_checks(spark: SparkSession, db: str, task: str, dt: str,
               checks: list[Check], params: dict) -> list[CheckResult]:
    results = []
    for check in checks:
        row = spark.sql(render(check.sql, params)).collect()[0]
        bad = None if row["bad"] is None else int(row["bad"])   # NULL 视为不通过
        results.append(CheckResult(check, bad, row["detail"] or ""))

    if results:
        rows = [(r.check.name, r.check.check_type, r.check.severity, r.check.target,
                 r.bad, r.detail, r.passed) for r in results]
        spark.createDataFrame(rows, _RESULT_SCHEMA).createOrReplaceTempView("dq_tmp")
        spark.sql(f"""
            INSERT OVERWRITE TABLE {db}.dq_check_result PARTITION (dt = '{dt}', task = '{task}')
            SELECT check_name, check_type, severity, target_table, bad_cnt, detail, passed, current_timestamp()
            FROM dq_tmp""")

    failed = [r for r in results if r.blocking]
    if failed:
        raise QualityGateError(task, dt, failed)
    return results
