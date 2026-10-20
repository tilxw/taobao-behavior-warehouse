"""质检规则。

每条规则是一段 SQL，返回一行两列：bad（违规行数或差值，0 为通过）和 detail（给人看的明细）。
规则里的 ${db} ${dt} 等在运行时替换；$table $keys 这类是构造规则时就填好的。
"""
from dataclasses import dataclass
from string import Template


@dataclass(frozen=True)
class Check:
    name: str
    check_type: str   # row_conservation / pk_unique / enum / time_range / consistency / not_empty
    target: str
    sql: str
    severity: str = "error"   # error：不通过就阻断下游；warn：只记录


def _t(sql: str, **kw) -> str:
    return Template(sql).safe_substitute(**kw)


_DAY = "dt = '${dt}'"


def not_empty(table: str) -> Check:
    return Check(f"{table}.not_empty", "not_empty", table, _t("""
        SELECT IF(count(*) = 0, 1, 0) AS bad, concat('rows=', count(*)) AS detail
        FROM ${db}.$table WHERE dt = '${dt}'""", table=table))


def one_row(table: str) -> Check:
    return Check(f"{table}.one_row", "pk_unique", table, _t("""
        SELECT abs(count(*) - 1) AS bad, concat('rows=', count(*)) AS detail
        FROM ${db}.$table WHERE dt = '${dt}'""", table=table))


def pk_unique(table: str, keys: list[str], where: str = _DAY, name: str = "pk_unique") -> Check:
    return Check(f"{table}.{name}", "pk_unique", table, _t("""
        SELECT dup_keys + null_keys AS bad,
               concat('key=($keys) duplicate_keys=', dup_keys, ' null_keys=', null_keys) AS detail
        FROM (
            SELECT (SELECT count(*) FROM (
                        SELECT $keys FROM ${db}.$table WHERE $where GROUP BY $keys HAVING count(*) > 1) x
                   ) AS dup_keys,
                   (SELECT count(*) FROM ${db}.$table WHERE ($where) AND ($null_cond)) AS null_keys
        ) t""", table=table, keys=", ".join(keys), where=where,
        null_cond=" OR ".join(f"{k} IS NULL" for k in keys)))


def custom(name: str, check_type: str, target: str, sql: str, severity: str = "error") -> Check:
    return Check(f"{target}.{name}", check_type, target, sql, severity)


BEHAVIOR_ENUM = "('pv', 'fav', 'cart', 'buy')"

# ---------------------------------------------------------------- ODS / 静态维表
ODS_CHECKS = [
    custom("row_conservation", "row_conservation", "ods_user_behavior_inc", """
        SELECT abs(s.c - o.c) AS bad, concat('source_rows=', s.c, ' ods_rows=', o.c) AS detail
        FROM (SELECT count(*) AS c FROM src_user_behavior) s,
             (SELECT count(*) AS c FROM ${db}.ods_user_behavior_inc) o"""),
    custom("window_partitions", "time_range", "ods_user_behavior_inc", """
        SELECT ${window_days} - count(DISTINCT dt) AS bad,
               concat('window_partitions_with_data=', count(DISTINCT dt)) AS detail
        FROM ${db}.ods_user_behavior_inc
        WHERE dt BETWEEN '${window_start}' AND '${window_end}'"""),
    # 越界数据是已知问题：只记录数量，不阻断（DWD 只读窗口内分区）
    custom("out_of_window_rows", "time_range", "ods_user_behavior_inc", """
        SELECT count(*) AS bad,
               concat('out_of_window_rows=', count(*), ' partitions=', count(DISTINCT dt)) AS detail
        FROM ${db}.ods_user_behavior_inc
        WHERE dt < '${window_start}' OR dt > '${window_end}' OR dt IS NULL""", severity="warn"),
]

DIM_DATE_CHECKS = [
    pk_unique("dim_date", ["date_id"], where="1 = 1"),
    custom("covers_window", "time_range", "dim_date", """
        SELECT ${window_days} - count(*) AS bad, concat('window_days_in_dim=', count(*)) AS detail
        FROM ${db}.dim_date WHERE date_id BETWEEN '${window_start}' AND '${window_end}'"""),
]

DIM_USER_LEVEL_CHECKS = [
    pk_unique("dim_user_level", ["user_level"], where="1 = 1"),
    # 0~1000 次购买，每个次数必须恰好落在一个等级区间里：区间不重叠、不遗漏
    custom("ranges_cover_once", "consistency", "dim_user_level", """
        SELECT count(*) AS bad, concat('buy_cnt_values_not_in_exactly_one_level=', count(*)) AS detail
        FROM (
            SELECT n, count(l.user_level) AS c
            FROM (SELECT explode(sequence(0, 1000)) AS n) x
            LEFT JOIN ${db}.dim_user_level l
              ON x.n >= l.min_buy_cnt AND (l.max_buy_cnt IS NULL OR x.n <= l.max_buy_cnt)
            GROUP BY n
            HAVING c <> 1
        ) t"""),
]

# ---------------------------------------------------------------- DWD
DWD_CHECKS = [
    not_empty("dwd_user_behavior_inc"),
    # 层间守恒：ODS 当天行数 = DWD 行数 + 非法行 + 重复行
    custom("row_conservation", "row_conservation", "dwd_user_behavior_inc", """
        WITH o AS (
            SELECT *, coalesce(user_id IS NOT NULL AND item_id IS NOT NULL AND category_id IS NOT NULL
                               AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
                               AND ts >= ${day_start_ts} AND ts < ${day_end_ts}, false) AS valid
            FROM ${db}.ods_user_behavior_inc WHERE dt = '${dt}'
        ),
        s AS (SELECT count(*) AS ods_cnt, count_if(NOT valid) AS invalid_cnt, count_if(valid) AS valid_cnt FROM o),
        u AS (SELECT count(*) AS distinct_cnt FROM (
                  SELECT DISTINCT user_id, item_id, category_id, behavior_type, ts FROM o WHERE valid) x),
        d AS (SELECT count(*) AS dwd_cnt FROM ${db}.dwd_user_behavior_inc WHERE dt = '${dt}')
        SELECT abs(s.ods_cnt - s.invalid_cnt - (s.valid_cnt - u.distinct_cnt) - d.dwd_cnt) AS bad,
               concat('ods=', s.ods_cnt, ' invalid=', s.invalid_cnt,
                      ' duplicate=', s.valid_cnt - u.distinct_cnt, ' dwd=', d.dwd_cnt) AS detail
        FROM s, u, d"""),
    pk_unique("dwd_user_behavior_inc", ["user_id", "item_id", "category_id", "behavior_type", "event_ts"]),
    custom("behavior_enum", "enum", "dwd_user_behavior_inc", _t("""
        SELECT count(*) AS bad, concat('illegal_behavior_rows=', count(*)) AS detail
        FROM ${db}.dwd_user_behavior_inc
        WHERE dt = '${dt}' AND (behavior_type IS NULL OR behavior_type NOT IN $enum)""", enum=BEHAVIOR_ENUM)),
    custom("time_range", "time_range", "dwd_user_behavior_inc", """
        SELECT count(*) AS bad, concat('rows_outside_day=', count(*)) AS detail
        FROM ${db}.dwd_user_behavior_inc
        WHERE dt = '${dt}'
          AND (event_ts < ${day_start_ts} OR event_ts >= ${day_end_ts}
               OR date_format(event_time, 'yyyy-MM-dd') <> '${dt}'
               OR event_hour <> hour(event_time))"""),
]

# ---------------------------------------------------------------- DIM 商品
DIM_ITEM_CHECKS = [
    not_empty("dim_item_full"),
    pk_unique("dim_item_full", ["item_id"]),
    # 快照守恒：今天的商品数 = 昨天的商品数 + 今天新出现的商品数
    custom("row_conservation", "row_conservation", "dim_item_full", """
        WITH cur AS (SELECT count(*) AS c FROM ${db}.dim_item_full WHERE dt = '${dt}'),
        prev AS (SELECT count(*) AS c FROM ${db}.dim_item_full WHERE dt = '${dt_prev}'),
        new AS (
            SELECT count(*) AS c FROM (SELECT DISTINCT item_id FROM ${db}.dwd_user_behavior_inc WHERE dt = '${dt}') a
            LEFT ANTI JOIN (SELECT item_id FROM ${db}.dim_item_full WHERE dt = '${dt_prev}') b
            ON a.item_id = b.item_id
        )
        SELECT abs(cur.c - prev.c - new.c) AS bad,
               concat('today=', cur.c, ' yesterday=', prev.c, ' new_items=', new.c) AS detail
        FROM cur, prev, new"""),
    custom("covers_dwd_items", "consistency", "dim_item_full", """
        SELECT count(*) AS bad, concat('dwd_items_missing_in_dim=', count(*)) AS detail
        FROM (SELECT DISTINCT item_id FROM ${db}.dwd_user_behavior_inc WHERE dt = '${dt}') a
        LEFT ANTI JOIN (SELECT item_id FROM ${db}.dim_item_full WHERE dt = '${dt}') b
        ON a.item_id = b.item_id"""),
]

# ---------------------------------------------------------------- DWS
DWS_USER_1D_CHECKS = [
    not_empty("dws_user_action_1d"),
    pk_unique("dws_user_action_1d", ["user_id"]),
    # 聚合守恒：四类行为次数之和 = DWD 行数；用户数 = DWD 去重用户数
    custom("row_conservation", "row_conservation", "dws_user_action_1d", """
        SELECT abs(a.events - d.events) + abs(a.users - d.users) AS bad,
               concat('dws_events=', a.events, ' dwd_events=', d.events,
                      ' dws_users=', a.users, ' dwd_users=', d.users) AS detail
        FROM (SELECT coalesce(sum(pv_cnt + fav_cnt + cart_cnt + buy_cnt), 0) AS events, count(*) AS users
              FROM ${db}.dws_user_action_1d WHERE dt = '${dt}') a,
             (SELECT count(*) AS events, count(DISTINCT user_id) AS users
              FROM ${db}.dwd_user_behavior_inc WHERE dt = '${dt}') d"""),
]

DWS_ITEM_1D_CHECKS = [
    not_empty("dws_item_action_1d"),
    pk_unique("dws_item_action_1d", ["item_id"]),
    custom("row_conservation", "row_conservation", "dws_item_action_1d", """
        SELECT abs(a.events - d.events) + abs(a.items - d.items) AS bad,
               concat('dws_events=', a.events, ' dwd_events=', d.events,
                      ' dws_items=', a.items, ' dwd_items=', d.items) AS detail
        FROM (SELECT coalesce(sum(pv_cnt + fav_cnt + cart_cnt + buy_cnt), 0) AS events, count(*) AS items
              FROM ${db}.dws_item_action_1d WHERE dt = '${dt}') a,
             (SELECT count(*) AS events, count(DISTINCT item_id) AS items
              FROM ${db}.dwd_user_behavior_inc WHERE dt = '${dt}') d"""),
    custom("category_not_null", "consistency", "dws_item_action_1d", """
        SELECT count(*) AS bad, concat('items_without_category=', count(*)) AS detail
        FROM ${db}.dws_item_action_1d WHERE dt = '${dt}' AND category_id IS NULL"""),
]

DWS_MILESTONE_CHECKS = [
    not_empty("dws_user_milestone_td"),
    pk_unique("dws_user_milestone_td", ["user_id"]),
    # 用户数守恒：今天 = 昨天 + 今天新出现的用户
    custom("row_conservation", "row_conservation", "dws_user_milestone_td", """
        WITH cur AS (SELECT count(*) AS c FROM ${db}.dws_user_milestone_td WHERE dt = '${dt}'),
        prev AS (SELECT count(*) AS c FROM ${db}.dws_user_milestone_td WHERE dt = '${dt_prev}'),
        new AS (
            SELECT count(*) AS c FROM (SELECT user_id FROM ${db}.dws_user_action_1d WHERE dt = '${dt}') a
            LEFT ANTI JOIN (SELECT user_id FROM ${db}.dws_user_milestone_td WHERE dt = '${dt_prev}') b
            ON a.user_id = b.user_id
        )
        SELECT abs(cur.c - prev.c - new.c) AS bad,
               concat('today=', cur.c, ' yesterday=', prev.c, ' new_users=', new.c) AS detail
        FROM cur, prev, new"""),
    # 累计值守恒：今天的累计 = 昨天的累计 + 今天的发生额
    custom("cumulative_conservation", "row_conservation", "dws_user_milestone_td", """
        WITH cur AS (SELECT coalesce(sum(cum_pv_cnt), 0) AS pv, coalesce(sum(cum_buy_cnt), 0) AS buy
                     FROM ${db}.dws_user_milestone_td WHERE dt = '${dt}'),
        prev AS (SELECT coalesce(sum(cum_pv_cnt), 0) AS pv, coalesce(sum(cum_buy_cnt), 0) AS buy
                 FROM ${db}.dws_user_milestone_td WHERE dt = '${dt_prev}'),
        today AS (SELECT coalesce(sum(pv_cnt), 0) AS pv, coalesce(sum(buy_cnt), 0) AS buy
                  FROM ${db}.dws_user_action_1d WHERE dt = '${dt}')
        SELECT abs(cur.pv - prev.pv - today.pv) + abs(cur.buy - prev.buy - today.buy) AS bad,
               concat('cum_buy_today=', cur.buy, ' cum_buy_yesterday=', prev.buy, ' buy_today=', today.buy) AS detail
        FROM cur, prev, today"""),
    custom("milestone_sanity", "consistency", "dws_user_milestone_td", """
        SELECT count(*) AS bad, concat('rows_breaking_rules=', count(*)) AS detail
        FROM ${db}.dws_user_milestone_td
        WHERE dt = '${dt}'
          AND (first_active_date > last_active_date
               OR active_days < 1
               OR active_days > datediff(to_date('${dt}'), to_date(first_active_date)) + 1
               OR buy_days > active_days
               OR (cum_buy_cnt > 0) <> (first_buy_time IS NOT NULL))"""),
]

# ---------------------------------------------------------------- DIM 用户拉链
_ZIP_LEVEL_FROM_MILESTONE = """
    SELECT m.user_id, l.user_level
    FROM ${db}.dws_user_milestone_td m
    JOIN ${db}.dim_user_level l
      ON m.cum_buy_cnt >= l.min_buy_cnt AND (l.max_buy_cnt IS NULL OR m.cum_buy_cnt <= l.max_buy_cnt)
    WHERE m.dt = '$day'"""

_ZIP_BACKTRACK = """
    WITH z AS (SELECT user_id, user_level FROM ${db}.dim_user_zip WHERE dt >= '$day' AND start_date <= '$day'),
    m AS ($milestone)
    SELECT count(*) AS bad, concat('as_of=$day level_mismatch_users=', count(*)) AS detail
    FROM z FULL OUTER JOIN m ON z.user_id = m.user_id
    WHERE z.user_id IS NULL OR m.user_id IS NULL OR z.user_level <> m.user_level"""


def _zip_backtrack(day: str) -> str:
    return _t(_ZIP_BACKTRACK, day=day, milestone=_t(_ZIP_LEVEL_FROM_MILESTONE, day=day))


DIM_USER_ZIP_CHECKS = [
    # 每个用户最多一条当前有效记录
    pk_unique("dim_user_zip", ["user_id"], where="dt = '${open_end_date}'"),
    custom("record_key_unique", "pk_unique", "dim_user_zip", """
        SELECT count(*) AS bad, concat('duplicate_user_start_date=', count(*)) AS detail
        FROM (SELECT user_id, start_date FROM ${db}.dim_user_zip GROUP BY user_id, start_date HAVING count(*) > 1) t"""),
    # 同一用户的区间首尾相接：下一段的 start_date = 上一段的 end_date + 1，且 start_date <= end_date
    custom("intervals_contiguous", "consistency", "dim_user_zip", """
        SELECT count(*) AS bad, concat('broken_intervals=', count(*)) AS detail
        FROM (
            SELECT start_date, end_date,
                   lead(start_date) OVER (PARTITION BY user_id ORDER BY start_date) AS next_start
            FROM ${db}.dim_user_zip
        ) t
        WHERE end_date < start_date
           OR (next_start IS NOT NULL AND datediff(to_date(next_start), to_date(end_date)) <> 1)"""),
    custom("level_enum", "enum", "dim_user_zip", """
        SELECT count(*) AS bad, concat('illegal_level_rows=', count(*)) AS detail
        FROM ${db}.dim_user_zip z LEFT ANTI JOIN ${db}.dim_user_level l ON z.user_level = l.user_level"""),
    custom("open_users_match", "row_conservation", "dim_user_zip", """
        SELECT abs(z.c - m.c) AS bad, concat('open_records=', z.c, ' milestone_users=', m.c) AS detail
        FROM (SELECT count(*) AS c FROM ${db}.dim_user_zip WHERE dt = '${open_end_date}') z,
             (SELECT count(*) AS c FROM ${db}.dws_user_milestone_td WHERE dt = '${dt}') m"""),
    # 回溯校验：从拉链表还原出的"当天等级"和"前一天等级"，都要和累积快照算出来的一致
    custom("backtrack_today", "consistency", "dim_user_zip", _zip_backtrack("${dt}")),
    custom("backtrack_prev_day", "consistency", "dim_user_zip", _zip_backtrack("${dt_prev}")),
]

# ---------------------------------------------------------------- ADS
ADS_TRAFFIC_CHECKS = [
    one_row("ads_traffic_stats_1d"),
    custom("consistency_with_dws", "consistency", "ads_traffic_stats_1d", """
        SELECT abs(a.pv_cnt - s.pv) + abs(a.active_user_cnt - s.users)
               + IF(a.uv > a.active_user_cnt OR a.buy_user_cnt > a.active_user_cnt, 1, 0) AS bad,
               concat('ads_pv=', a.pv_cnt, ' dws_pv=', s.pv, ' uv=', a.uv, ' active=', a.active_user_cnt) AS detail
        FROM (SELECT * FROM ${db}.ads_traffic_stats_1d WHERE dt = '${dt}') a,
             (SELECT sum(pv_cnt) AS pv, count(*) AS users FROM ${db}.dws_user_action_1d WHERE dt = '${dt}') s"""),
]

ADS_FUNNEL_CHECKS = [
    one_row("ads_conversion_funnel_1d"),
    # 漏斗逐级不增；第 1 步（浏览用户）走 DWD 明细算，和走 DWS 汇总算的 UV 必须相等
    custom("monotonic_and_cross_check", "consistency", "ads_conversion_funnel_1d", """
        SELECT IF(f.step2_cart_fav_users > f.step1_pv_users OR f.step3_buy_users > f.step2_cart_fav_users, 1, 0)
               + abs(f.step1_pv_users - t.uv) AS bad,
               concat('step1=', f.step1_pv_users, ' step2=', f.step2_cart_fav_users,
                      ' step3=', f.step3_buy_users, ' traffic_uv=', t.uv) AS detail
        FROM (SELECT * FROM ${db}.ads_conversion_funnel_1d WHERE dt = '${dt}') f,
             (SELECT uv FROM ${db}.ads_traffic_stats_1d WHERE dt = '${dt}') t"""),
]

ADS_RETENTION_CHECKS = [
    pk_unique("ads_user_retention", ["cohort_date", "cohort_type", "retention_day"]),
    custom("value_range", "consistency", "ads_user_retention", """
        SELECT count(*) AS bad, concat('rows_breaking_rules=', count(*)) AS detail
        FROM ${db}.ads_user_retention
        WHERE dt = '${dt}'
          AND (retained_cnt > cohort_size OR retention_rate < 0 OR retention_rate > 1
               OR retention_day NOT IN (1, 3)
               OR datediff(to_date(dt), to_date(cohort_date)) <> retention_day
               OR cohort_type NOT IN ('new', 'active'))"""),
    # 活跃同期群人数 = 同期群当天的活跃用户数
    custom("active_cohort_size", "consistency", "ads_user_retention", """
        SELECT count(*) AS bad, concat('active_cohort_size_mismatch=', count(*)) AS detail
        FROM (SELECT cohort_date, cohort_size FROM ${db}.ads_user_retention
              WHERE dt = '${dt}' AND cohort_type = 'active') r
        LEFT JOIN (SELECT dt, count(*) AS c FROM ${db}.dws_user_action_1d
                   WHERE dt IN ('${dt_1d_ago}', '${dt_3d_ago}') GROUP BY dt) a
          ON r.cohort_date = a.dt
        WHERE a.c IS NULL OR a.c <> r.cohort_size"""),
]

ADS_REPURCHASE_CHECKS = [
    one_row("ads_repurchase_rate_td"),
    custom("ordering", "consistency", "ads_repurchase_rate_td", """
        SELECT count(*) AS bad,
               concat('by_day=', max(repurchase_user_cnt_by_day), ' by_times=', max(repurchase_user_cnt_by_times),
                      ' buyers=', max(buyer_cnt)) AS detail
        FROM ${db}.ads_repurchase_rate_td
        WHERE dt = '${dt}'
          AND (repurchase_user_cnt_by_day > repurchase_user_cnt_by_times
               OR repurchase_user_cnt_by_times > buyer_cnt)"""),
]


def _topn_checks(table: str, key: str, metric: str) -> list[Check]:
    return [
        pk_unique(table, ["rk"]),
        custom("rank_rules", "consistency", table, _t("""
            SELECT IF(count(*) > 10, 1, 0)
                   + IF(count(*) > 0 AND (min(rk) <> 1 OR max(rk) <> count(*)), 1, 0)
                   + count_if(prev_metric < $metric) AS bad,
                   concat('rows=', count(*), ' order_violations=', count_if(prev_metric < $metric)) AS detail
            FROM (
                SELECT rk, $metric, lag($metric) OVER (ORDER BY rk) AS prev_metric
                FROM ${db}.$table WHERE dt = '${dt}'
            ) t""", table=table, metric=metric)),
        pk_unique(table, [key], name=f"{key}_unique"),
    ]


ADS_ITEM_TOPN_CHECKS = _topn_checks("ads_item_topn_1d", "item_id", "pv_cnt")
ADS_CATEGORY_TOPN_CHECKS = _topn_checks("ads_category_topn_1d", "category_id", "buy_cnt")

ADS_CONTINUOUS_CHECKS = [
    one_row("ads_user_continuous_active_7d"),
    custom("ordering", "consistency", "ads_user_continuous_active_7d", """
        SELECT count(*) AS bad, concat('rows_breaking_rules=', count(*)) AS detail
        FROM ${db}.ads_user_continuous_active_7d
        WHERE dt = '${dt}' AND continuous_3d_user_cnt > active_user_cnt"""),
]

ADS_LEVEL_DIST_CHECKS = [
    pk_unique("ads_user_level_dist_1d", ["user_level"]),
    custom("total_matches_users", "row_conservation", "ads_user_level_dist_1d", """
        SELECT abs(a.c - m.c) AS bad, concat('level_dist_total=', a.c, ' milestone_users=', m.c) AS detail
        FROM (SELECT coalesce(sum(user_cnt), 0) AS c FROM ${db}.ads_user_level_dist_1d WHERE dt = '${dt}') a,
             (SELECT count(*) AS c FROM ${db}.dws_user_milestone_td WHERE dt = '${dt}') m"""),
]
