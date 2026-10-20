-- DWD 行为明细（事务事实表）：只读 ODS 当天分区。
-- 1) 剔除非法记录：主键字段为空、行为类型不在枚举内、时间戳不在当天 [00:00, 24:00) 北京时间内
-- 2) 去重：五列完全相同视为重复上报，保留一条
-- 3) 时间戳转成北京时间（会话时区为 Asia/Shanghai）
INSERT OVERWRITE TABLE ${db}.dwd_user_behavior_inc PARTITION (dt = '${dt}')
SELECT
    user_id,
    item_id,
    category_id,
    behavior_type,
    ts                           AS event_ts,
    timestamp_seconds(ts)        AS event_time,
    hour(timestamp_seconds(ts))  AS event_hour
FROM (
    SELECT DISTINCT user_id, item_id, category_id, behavior_type, ts
    FROM ${db}.ods_user_behavior_inc
    WHERE dt = '${dt}'
      AND user_id IS NOT NULL
      AND item_id IS NOT NULL
      AND category_id IS NOT NULL
      AND behavior_type IN ('pv', 'fav', 'cart', 'buy')
      AND ts >= ${day_start_ts} AND ts < ${day_end_ts}
) t;
