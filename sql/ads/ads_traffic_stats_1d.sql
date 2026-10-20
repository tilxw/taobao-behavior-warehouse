-- 每日流量与行为概况
INSERT OVERWRITE TABLE ${db}.ads_traffic_stats_1d PARTITION (dt = '${dt}')
SELECT
    d.day_name,
    d.is_weekend,
    s.pv_cnt,
    s.uv,
    s.active_user_cnt,
    s.fav_cnt,
    s.cart_cnt,
    s.buy_cnt,
    s.buy_user_cnt,
    round(s.pv_cnt / nullif(s.uv, 0), 2)                     AS pv_per_uv,
    round(s.buy_user_cnt / nullif(s.active_user_cnt, 0), 4)  AS buy_user_rate
FROM (
    SELECT
        sum(pv_cnt)            AS pv_cnt,
        count_if(pv_cnt > 0)   AS uv,
        count(*)               AS active_user_cnt,
        sum(fav_cnt)           AS fav_cnt,
        sum(cart_cnt)          AS cart_cnt,
        sum(buy_cnt)           AS buy_cnt,
        count_if(buy_cnt > 0)  AS buy_user_cnt
    FROM ${db}.dws_user_action_1d
    WHERE dt = '${dt}'
) s
LEFT JOIN ${db}.dim_date d
  ON d.date_id = '${dt}';
