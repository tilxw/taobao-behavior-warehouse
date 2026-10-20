-- 当日浏览量 Top10 商品。先 ORDER BY + LIMIT（Spark 会改写成各分区先取 TopK 再合并），再在 10 行上编号；
-- 浏览次数相同按 item_id 升序，保证名次确定。
INSERT OVERWRITE TABLE ${db}.ads_item_topn_1d PARTITION (dt = '${dt}')
SELECT
    row_number() OVER (ORDER BY pv_cnt DESC, item_id ASC) AS rk,
    item_id,
    category_id,
    pv_cnt,
    pv_user_cnt,
    buy_cnt
FROM (
    SELECT item_id, category_id, pv_cnt, pv_user_cnt, buy_cnt
    FROM ${db}.dws_item_action_1d
    WHERE dt = '${dt}' AND pv_cnt > 0
    ORDER BY pv_cnt DESC, item_id ASC
    LIMIT 10
) t;
