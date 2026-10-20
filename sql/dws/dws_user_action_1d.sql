-- 用户日汇总（周期快照事实表）：粒度 用户×天
INSERT OVERWRITE TABLE ${db}.dws_user_action_1d PARTITION (dt = '${dt}')
SELECT
    user_id,
    count_if(behavior_type = 'pv')                              AS pv_cnt,
    count_if(behavior_type = 'fav')                             AS fav_cnt,
    count_if(behavior_type = 'cart')                            AS cart_cnt,
    count_if(behavior_type = 'buy')                             AS buy_cnt,
    count(DISTINCT IF(behavior_type = 'pv', item_id, NULL))     AS pv_item_cnt,
    count(DISTINCT IF(behavior_type = 'buy', item_id, NULL))    AS buy_item_cnt,
    min(event_time)                                             AS first_event_time,
    max(event_time)                                             AS last_event_time,
    min(IF(behavior_type = 'pv', event_time, NULL))             AS first_pv_time,
    min(IF(behavior_type = 'fav', event_time, NULL))            AS first_fav_time,
    min(IF(behavior_type = 'cart', event_time, NULL))           AS first_cart_time,
    min(IF(behavior_type = 'buy', event_time, NULL))            AS first_buy_time
FROM ${db}.dwd_user_behavior_inc
WHERE dt = '${dt}'
GROUP BY user_id;
