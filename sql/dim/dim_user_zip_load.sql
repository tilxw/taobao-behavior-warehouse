-- 用户等级拉链表 第 2 步：调度器已删掉 dt >= D-1 的旧分区，这里把中间表按 end_date 动态分区写回。
INSERT OVERWRITE TABLE ${db}.dim_user_zip PARTITION (dt)
SELECT user_id, user_level, level_name, start_date, end_date, end_date AS dt
FROM ${db}.tmp_dim_user_zip;
