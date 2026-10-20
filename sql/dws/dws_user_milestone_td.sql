-- 用户转化里程碑（累积快照事实表）：一个用户一行。
-- 今天的快照 = 昨天的快照 FULL JOIN 今天的用户日汇总：
--   里程碑（首次浏览/收藏/加购/购买）一旦有值就不再改写；累计指标在昨天基础上累加。
-- 同一张表读昨天、写今天，先写中间表再覆盖当天分区。
INSERT OVERWRITE TABLE ${db}.tmp_dws_user_milestone_td
SELECT
    coalesce(p.user_id, t.user_id)                              AS user_id,
    coalesce(p.first_active_date, '${dt}')                      AS first_active_date,
    IF(t.user_id IS NOT NULL, '${dt}', p.last_active_date)      AS last_active_date,
    coalesce(p.active_days, 0) + IF(t.user_id IS NOT NULL, 1, 0) AS active_days,
    coalesce(p.first_pv_time, t.first_pv_time)                  AS first_pv_time,
    coalesce(p.first_fav_time, t.first_fav_time)                AS first_fav_time,
    coalesce(p.first_cart_time, t.first_cart_time)              AS first_cart_time,
    coalesce(p.first_buy_time, t.first_buy_time)                AS first_buy_time,
    coalesce(p.cum_pv_cnt, 0) + coalesce(t.pv_cnt, 0)           AS cum_pv_cnt,
    coalesce(p.cum_buy_cnt, 0) + coalesce(t.buy_cnt, 0)         AS cum_buy_cnt,
    coalesce(p.buy_days, 0) + IF(coalesce(t.buy_cnt, 0) > 0, 1, 0) AS buy_days
FROM (
    SELECT * FROM ${db}.dws_user_milestone_td WHERE dt = '${dt_prev}'
) p
FULL OUTER JOIN (
    SELECT * FROM ${db}.dws_user_action_1d WHERE dt = '${dt}'
) t
ON p.user_id = t.user_id;

INSERT OVERWRITE TABLE ${db}.dws_user_milestone_td PARTITION (dt = '${dt}')
SELECT * FROM ${db}.tmp_dws_user_milestone_td;
