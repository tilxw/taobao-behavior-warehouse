-- 当天的用户等级分布：从拉链表回溯 D 当天有效的记录（start_date <= D <= end_date）。
-- 拉链表按 end_date 分区，dt >= D 可以直接裁剪掉 D 之前就关闭的分区。
INSERT OVERWRITE TABLE ${db}.ads_user_level_dist_1d PARTITION (dt = '${dt}')
SELECT
    user_level,
    level_name,
    count(*)                                         AS user_cnt,
    round(count(*) / sum(count(*)) OVER (), 4)       AS user_ratio
FROM ${db}.dim_user_zip
WHERE dt >= '${dt}'
  AND start_date <= '${dt}'
GROUP BY user_level, level_name;
