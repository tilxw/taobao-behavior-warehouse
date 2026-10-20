-- 截至当天的累计复购率，给出两种口径：
--   按天：购买天数 >= 2 的用户 / 有购买的用户（同一天买多件算一次，更接近"回头客"）
--   按次数：购买次数 >= 2 的用户 / 有购买的用户（一次下单买多件也会被算成复购，数值偏高）
INSERT OVERWRITE TABLE ${db}.ads_repurchase_rate_td PARTITION (dt = '${dt}')
SELECT
    buyer_cnt,
    by_day_cnt,
    round(by_day_cnt / nullif(buyer_cnt, 0), 4),
    by_times_cnt,
    round(by_times_cnt / nullif(buyer_cnt, 0), 4)
FROM (
    SELECT count_if(cum_buy_cnt > 0)  AS buyer_cnt,
           count_if(buy_days >= 2)    AS by_day_cnt,
           count_if(cum_buy_cnt >= 2) AS by_times_cnt
    FROM ${db}.dws_user_milestone_td
    WHERE dt = '${dt}'
) s;
