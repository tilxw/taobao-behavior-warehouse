-- 近 7 日（D-6 ~ D，不早于数据起始日）内出现过"连续 >= 3 天活跃"的用户数。
-- 经典做法：同一用户的活跃日期按顺序编号 rn，日期 - rn 相同的就是一段连续日期（日期和编号同步 +1）。
INSERT OVERWRITE TABLE ${db}.ads_user_continuous_active_7d PARTITION (dt = '${dt}')
WITH d AS (
    SELECT user_id, to_date(dt) AS active_date
    FROM ${db}.dws_user_action_1d
    WHERE dt BETWEEN greatest('${dt_6d_ago}', '${window_start}') AND '${dt}'
),
g AS (
    SELECT user_id,
           date_sub(active_date, row_number() OVER (PARTITION BY user_id ORDER BY active_date)) AS grp
    FROM d
),
streak AS (
    SELECT user_id, count(*) AS streak_days
    FROM g
    GROUP BY user_id, grp
),
s AS (
    SELECT (SELECT count(DISTINCT user_id) FROM d)                         AS active_user_cnt,
           count(DISTINCT IF(streak_days >= 3, user_id, NULL))             AS continuous_3d_user_cnt
    FROM streak
)
SELECT
    greatest('${dt_6d_ago}', '${window_start}'),
    active_user_cnt,
    continuous_3d_user_cnt,
    round(continuous_3d_user_cnt / nullif(active_user_cnt, 0), 4)
FROM s;
