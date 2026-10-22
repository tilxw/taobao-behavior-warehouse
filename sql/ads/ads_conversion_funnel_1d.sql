-- 当日转化漏斗（用户粒度），同时给出两种口径，每天两行：
--
-- strict（有序 / 严格漏斗）：参考常见行为分析工具的"有序漏斗"，窗口期（当天）内按顺序完成每一步。
--   第 1 步：当天有浏览（t1 = 当天首次浏览时间）
--   第 2 步：在 t1 之后（含同一秒）有加购或收藏（t2 = 满足条件的最早一次）
--   第 3 步：在 t2 之后（含同一秒）有购买
--   先加购后浏览、先买后加购的用户不计入后续步骤。
--
-- non_strict（无序 / 非严格漏斗）：当天做过就算，不看先后。
--   第 1 步：当天有浏览；第 2 步：第 1 步的用户里当天有加购或收藏；第 3 步：第 2 步的用户里当天有购买。
--   每一步仍是上一步的子集，所以同一天里 strict 的每一步都不会多于 non_strict。
--
-- 两种口径都只看当天，跨天的行为各自归属自己那一天；只有购买、当天没浏览的用户两种口径都不计入。
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
           max(IF(behavior_type = 'buy' AND event_ts >= t2, 1, 0)) AS strict_buy,
           max(IF(behavior_type = 'pv', 1, 0))                     AS has_pv,
           max(IF(behavior_type IN ('cart', 'fav'), 1, 0))         AS has_cart_fav,
           max(IF(behavior_type = 'buy', 1, 0))                    AS has_buy
    FROM e2
    GROUP BY user_id
),
s AS (
    -- 一次扫描同时算出两种口径
    SELECT count_if(t1 IS NOT NULL)                                       AS strict_step1,
           count_if(t2 IS NOT NULL)                                       AS strict_step2,
           count_if(strict_buy = 1)                                       AS strict_step3,
           count_if(has_pv = 1)                                           AS loose_step1,
           count_if(has_pv = 1 AND has_cart_fav = 1)                      AS loose_step2,
           count_if(has_pv = 1 AND has_cart_fav = 1 AND has_buy = 1)      AS loose_step3
    FROM u
),
f AS (
    -- stack：把一行拆成两行（行转列），一行一种口径
    SELECT stack(2,
                 'strict',     strict_step1, strict_step2, strict_step3,
                 'non_strict', loose_step1,  loose_step2,  loose_step3)
           AS (funnel_type, step1, step2, step3)
    FROM s
)
SELECT
    funnel_type,
    step1,
    step2,
    step3,
    round(step2 / nullif(step1, 0), 4),
    round(step3 / nullif(step2, 0), 4),
    round(step3 / nullif(step1, 0), 4)
FROM f;
