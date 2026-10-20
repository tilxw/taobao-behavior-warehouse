-- 商品维表（每日全量快照）：今天的快照 = 昨天的快照 + 今天出现的商品。
-- 类目按"最近一次行为里的类目"覆盖（同一秒多个类目时取类目ID大的，保证结果确定）。
-- 读昨天分区、写今天分区是同一张表，Spark 不允许直接 INSERT OVERWRITE，先写中间表再覆盖。
INSERT OVERWRITE TABLE ${db}.tmp_dim_item_full
SELECT
    coalesce(t.item_id, p.item_id)                      AS item_id,
    coalesce(t.category_id, p.category_id)              AS category_id,
    coalesce(p.first_seen_date, '${dt}')                AS first_seen_date,
    IF(t.item_id IS NOT NULL, '${dt}', p.last_seen_date) AS last_seen_date
FROM (
    SELECT item_id, category_id, first_seen_date, last_seen_date
    FROM ${db}.dim_item_full
    WHERE dt = '${dt_prev}'
) p
FULL OUTER JOIN (
    SELECT item_id,
           max_by(category_id, named_struct('ts', event_ts, 'cid', category_id)) AS category_id
    FROM ${db}.dwd_user_behavior_inc
    WHERE dt = '${dt}'
    GROUP BY item_id
) t
ON p.item_id = t.item_id;

INSERT OVERWRITE TABLE ${db}.dim_item_full PARTITION (dt = '${dt}')
SELECT item_id, category_id, first_seen_date, last_seen_date
FROM ${db}.tmp_dim_item_full;
