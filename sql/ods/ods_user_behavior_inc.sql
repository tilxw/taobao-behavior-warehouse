-- ODS：源 CSV 原样落地成按天分区的 Parquet。
-- 源文件只有一个，所以 ODS 用动态分区一次性写入全部日期；不去重、不过滤，越界时间戳按真实日期进各自的分区。
-- DISTRIBUTE BY dt：同一天的数据进同一个 reduce，每个分区只产出一个文件，避免小文件。
INSERT OVERWRITE TABLE ${db}.ods_user_behavior_inc PARTITION (dt)
SELECT
    user_id,
    item_id,
    category_id,
    behavior_type,
    ts,
    date_format(timestamp_seconds(ts), 'yyyy-MM-dd') AS dt
FROM src_user_behavior
DISTRIBUTE BY dt;
