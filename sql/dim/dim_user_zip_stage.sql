-- 用户等级拉链表 第 1 步：算出处理完 D 之后，dt >= D-1 这些分区应有的全部记录，先落中间表。
-- 拉链表按 end_date 分区：当前有效记录在 9999-12-31 分区，D 当天被关闭的旧记录进 D-1 分区。
--
-- 重跑 / 回填安全：先把表"回滚"到 D-1 收盘时的状态再合并当天变化——
--   end_date >= D-1 的记录在 D-1 当天都还有效，统一视为开链；start_date >= D 的记录是之前跑 D 及以后产生的，直接丢弃。
--   正常按天往前跑时，dt >= D-1 只会命中 9999-12-31 分区，等价于经典的"开链 + 当天变化"合并。
INSERT OVERWRITE TABLE ${db}.tmp_dim_user_zip
WITH base AS (
    SELECT user_id, user_level, level_name, start_date
    FROM ${db}.dim_user_zip
    WHERE dt >= '${dt_prev}'
      AND start_date <= '${dt_prev}'
),
today AS (
    -- 截至 D 收盘的累计购买次数 → 等级
    SELECT m.user_id, l.user_level, l.level_name
    FROM ${db}.dws_user_milestone_td m
    JOIN ${db}.dim_user_level l
      ON m.cum_buy_cnt >= l.min_buy_cnt
     AND (l.max_buy_cnt IS NULL OR m.cum_buy_cnt <= l.max_buy_cnt)
    WHERE m.dt = '${dt}'
),
j AS (
    SELECT b.user_id    AS b_user_id,
           b.user_level AS b_level,
           b.level_name AS b_level_name,
           b.start_date AS b_start_date,
           t.user_id    AS t_user_id,
           t.user_level AS t_level,
           t.level_name AS t_level_name
    FROM base b
    FULL OUTER JOIN today t
      ON b.user_id = t.user_id
)
-- ① 等级没变（或当天没出现）：原记录继续有效
SELECT b_user_id, b_level, b_level_name, b_start_date, '${open_end_date}'
FROM j
WHERE b_user_id IS NOT NULL AND (t_user_id IS NULL OR t_level = b_level)
UNION ALL
-- ② 等级变了：旧记录在 D-1 关闭
SELECT b_user_id, b_level, b_level_name, b_start_date, '${dt_prev}'
FROM j
WHERE b_user_id IS NOT NULL AND t_user_id IS NOT NULL AND t_level <> b_level
UNION ALL
-- ③ 等级变了或新用户：新记录从 D 开始生效
SELECT t_user_id, t_level, t_level_name, '${dt}', '${open_end_date}'
FROM j
WHERE t_user_id IS NOT NULL AND (b_user_id IS NULL OR t_level <> b_level);
