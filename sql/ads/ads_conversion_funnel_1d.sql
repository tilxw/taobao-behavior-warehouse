-- 当日有序转化漏斗（用户粒度）。口径参考常见行为分析工具的"有序漏斗"：窗口期（当天）内按顺序完成每一步。
--   第 1 步：当天有浏览（t1 = 当天首次浏览时间）
--   第 2 步：在 t1 之后（含同一秒）有加购或收藏（t2 = 满足条件的最早一次）
--   第 3 步：在 t2 之后（含同一秒）有购买
-- 先买后看、只加购没浏览的用户不计入后续步骤；跨天的行为各自归属自己那一天。
INSERT OVERWRITE TABLE ${db}.ads_conversion_funnel_1d PARTITION (dt = '${dt}')
WITH e1 AS (
    SELECT user_id, behavior_type, event_ts,
           min(IF(behavior_type = 'pv', event_ts, NULL)) OVER (PARTITION BY user_id) AS t1
    FROM ${db}.dwd_user_behavior_inc
    WHERE dt = '${dt}'
),
e2 AS (
    SELECT user_id, behavior_type, event_ts, t1,
           min(IF(behavior_type IN ('cart', 'fav') AND event_ts >= t1, event_ts, NULL))
               OVER (PARTITION BY user_id) AS t2
    FROM e1
),
u AS (
    SELECT user_id,
           max(t1) AS t1,
           max(t2) AS t2,
           max(IF(behavior_type = 'buy' AND event_ts >= t2, 1, 0)) AS reached_buy
    FROM e2
    GROUP BY user_id
),
s AS (
    SELECT count_if(t1 IS NOT NULL)  AS step1,
           count_if(t2 IS NOT NULL)  AS step2,
           count_if(reached_buy = 1) AS step3
    FROM u
)
SELECT
    step1,
    step2,
    step3,
    round(step2 / nullif(step1, 0), 4),
    round(step3 / nullif(step2, 0), 4),
    round(step3 / nullif(step1, 0), 4)
FROM s;
