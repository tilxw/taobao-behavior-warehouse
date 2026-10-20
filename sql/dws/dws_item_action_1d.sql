-- 商品日汇总（周期快照事实表）：粒度 商品×天；类目取当日商品维表快照
INSERT OVERWRITE TABLE ${db}.dws_item_action_1d PARTITION (dt = '${dt}')
SELECT
    a.item_id,
    d.category_id,
    a.pv_cnt,
    a.pv_user_cnt,
    a.fav_cnt,
    a.cart_cnt,
    a.buy_cnt,
    a.buy_user_cnt
FROM (
    SELECT
        item_id,
        count_if(behavior_type = 'pv')                           AS pv_cnt,
        count(DISTINCT IF(behavior_type = 'pv', user_id, NULL))  AS pv_user_cnt,
        count_if(behavior_type = 'fav')                          AS fav_cnt,
        count_if(behavior_type = 'cart')                         AS cart_cnt,
        count_if(behavior_type = 'buy')                          AS buy_cnt,
        count(DISTINCT IF(behavior_type = 'buy', user_id, NULL)) AS buy_user_cnt
    FROM ${db}.dwd_user_behavior_inc
    WHERE dt = '${dt}'
    GROUP BY item_id
) a
LEFT JOIN (
    SELECT item_id, category_id FROM ${db}.dim_item_full WHERE dt = '${dt}'
) d
ON a.item_id = d.item_id;
