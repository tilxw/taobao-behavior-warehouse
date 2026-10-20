-- 用户留存。第 N 日留存 = 同期群里在 cohort_date + N 当天仍有任意行为的人数 / 同期群人数（只看第 N 天当天，不是 N 天内）。
-- 同期群两种：new = 当天新增（窗口内首次出现）；active = 当天活跃。
-- 计算日 D 产出两组：cohort_date = D-1 的次日留存、cohort_date = D-3 的第 3 日留存；早于数据起始日的同期群不产出。
INSERT OVERWRITE TABLE ${db}.ads_user_retention PARTITION (dt = '${dt}')
WITH today AS (
    SELECT user_id FROM ${db}.dws_user_action_1d WHERE dt = '${dt}'
),
cohort AS (
    SELECT first_active_date AS cohort_date, 'new' AS cohort_type, user_id
    FROM ${db}.dws_user_milestone_td
    WHERE dt = '${dt}'
      AND first_active_date IN ('${dt_1d_ago}', '${dt_3d_ago}')
      AND first_active_date >= '${window_start}'
    UNION ALL
    SELECT dt AS cohort_date, 'active' AS cohort_type, user_id
    FROM ${db}.dws_user_action_1d
    WHERE dt IN ('${dt_1d_ago}', '${dt_3d_ago}')
      AND dt >= '${window_start}'
)
SELECT
    c.cohort_date,
    c.cohort_type,
    datediff(to_date('${dt}'), to_date(c.cohort_date))   AS retention_day,
    count(*)                                             AS cohort_size,
    count(t.user_id)                                     AS retained_cnt,
    round(count(t.user_id) / count(*), 4)                AS retention_rate
FROM cohort c
LEFT JOIN today t
  ON c.user_id = t.user_id
GROUP BY c.cohort_date, c.cohort_type;
