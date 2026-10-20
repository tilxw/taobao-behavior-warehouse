-- 当日购买次数 Top10 类目。类目取行为发生时记录的类目（DWD 原值）；购买次数相同按 category_id 升序。
INSERT OVERWRITE TABLE ${db}.ads_category_topn_1d PARTITION (dt = '${dt}')
SELECT
    row_number() OVER (ORDER BY buy_cnt DESC, category_id ASC) AS rk,
    category_id,
    buy_cnt,
    buy_user_cnt,
    pv_cnt,
    pv_user_cnt,
    round(buy_user_cnt / nullif(pv_user_cnt, 0), 4) AS buy_user_rate
FROM (
    SELECT
        category_id,
        count_if(behavior_type = 'buy')                          AS buy_cnt,
        count(DISTINCT IF(behavior_type = 'buy', user_id, NULL)) AS buy_user_cnt,
        count_if(behavior_type = 'pv')                           AS pv_cnt,
        count(DISTINCT IF(behavior_type = 'pv', user_id, NULL))  AS pv_user_cnt
    FROM ${db}.dwd_user_behavior_inc
    WHERE dt = '${dt}'
    GROUP BY category_id
    HAVING buy_cnt > 0
    ORDER BY buy_cnt DESC, category_id ASC
    LIMIT 10
) t;
