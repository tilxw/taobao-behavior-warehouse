-- 全部表结构。所有表都是 Hive 表（STORED AS PARQUET），元数据在本机 Derby 元数据库里。
-- 命名：层_主题_内容_后缀；后缀 inc=每日增量，full=每日全量快照，zip=拉链，1d=当日汇总，td=历史至今累积，7d=近 7 日

CREATE DATABASE IF NOT EXISTS ${db};

-- ================= ODS =================
CREATE TABLE IF NOT EXISTS ${db}.ods_user_behavior_inc (
    user_id       BIGINT COMMENT '用户ID',
    item_id       BIGINT COMMENT '商品ID',
    category_id   BIGINT COMMENT '商品类目ID',
    behavior_type STRING COMMENT '行为类型 pv/fav/cart/buy',
    ts            BIGINT COMMENT '行为时间，Unix 秒'
) COMMENT 'ODS 用户行为日志：CSV 原样落地，不去重不过滤'
PARTITIONED BY (dt STRING COMMENT '行为日期（北京时间），越界时间戳也按真实日期落分区')
STORED AS PARQUET;

-- ================= DIM =================
CREATE TABLE IF NOT EXISTS ${db}.dim_date (
    date_id      STRING  COMMENT '日期 yyyy-MM-dd',
    year         INT,
    month        INT,
    day          INT,
    week_of_year INT     COMMENT 'ISO 周',
    day_of_week  INT     COMMENT '1=周一 … 7=周日',
    day_name     STRING  COMMENT '周一…周日',
    is_weekend   BOOLEAN
) COMMENT '日期维表（静态）'
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.dim_user_level (
    user_level  STRING COMMENT '等级编码',
    level_name  STRING COMMENT '等级名称',
    min_buy_cnt BIGINT COMMENT '累计购买次数下限（含）',
    max_buy_cnt BIGINT COMMENT '累计购买次数上限（含），NULL 表示不封顶'
) COMMENT '用户等级码表（静态），拉链表按它把累计购买次数映射成等级'
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.dim_item_full (
    item_id         BIGINT COMMENT '商品ID',
    category_id     BIGINT COMMENT '类目ID；同一商品出现过多个类目时取最近一次行为里的类目',
    first_seen_date STRING COMMENT '首次出现日期',
    last_seen_date  STRING COMMENT '最近出现日期'
) COMMENT '商品维表，每日全量快照'
PARTITIONED BY (dt STRING COMMENT '快照日期')
STORED AS PARQUET;

-- 自依赖任务（今天 = 昨天 + 今天增量）的中间表：Spark 不允许 INSERT OVERWRITE 读写同一张表，
-- 所以先把结果写进中间表，再覆盖正式表的当天分区
CREATE TABLE IF NOT EXISTS ${db}.tmp_dim_item_full (
    item_id         BIGINT,
    category_id     BIGINT,
    first_seen_date STRING,
    last_seen_date  STRING
) COMMENT '商品快照中间表'
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.dim_user_zip (
    user_id    BIGINT COMMENT '用户ID',
    user_level STRING COMMENT '用户等级编码，见 dim_user_level',
    level_name STRING COMMENT '用户等级名称',
    start_date STRING COMMENT '该等级生效日期',
    end_date   STRING COMMENT '该等级失效日期（含），9999-12-31 表示当前有效'
) COMMENT '用户等级拉链表：等级随累计购买次数变化，可回溯任意一天的等级'
PARTITIONED BY (dt STRING COMMENT '= end_date；当前有效记录都在 9999-12-31 分区')
STORED AS PARQUET;

-- 拉链表每日计算的中间结果
CREATE TABLE IF NOT EXISTS ${db}.tmp_dim_user_zip (
    user_id    BIGINT,
    user_level STRING,
    level_name STRING,
    start_date STRING,
    end_date   STRING
) COMMENT '拉链表中间表'
STORED AS PARQUET;

-- ================= DWD =================
CREATE TABLE IF NOT EXISTS ${db}.dwd_user_behavior_inc (
    user_id       BIGINT    COMMENT '用户ID',
    item_id       BIGINT    COMMENT '商品ID',
    category_id   BIGINT    COMMENT '行为发生时记录的类目ID',
    behavior_type STRING    COMMENT 'pv/fav/cart/buy',
    event_ts      BIGINT    COMMENT '行为时间，Unix 秒',
    event_time    TIMESTAMP COMMENT '行为时间（会话时区 Asia/Shanghai 下显示为北京时间）',
    event_hour    INT       COMMENT '北京时间小时 0-23'
) COMMENT 'DWD 用户行为明细（事务事实表）：去重、剔除越界和非法记录，一行一次行为'
PARTITIONED BY (dt STRING COMMENT '行为日期（北京时间）')
STORED AS PARQUET;

-- ================= DWS =================
CREATE TABLE IF NOT EXISTS ${db}.dws_user_action_1d (
    user_id          BIGINT,
    pv_cnt           BIGINT    COMMENT '浏览次数',
    fav_cnt          BIGINT    COMMENT '收藏次数',
    cart_cnt         BIGINT    COMMENT '加购次数',
    buy_cnt          BIGINT    COMMENT '购买次数',
    pv_item_cnt      BIGINT    COMMENT '浏览的不同商品数',
    buy_item_cnt     BIGINT    COMMENT '购买的不同商品数',
    first_event_time TIMESTAMP COMMENT '当天第一次行为时间',
    last_event_time  TIMESTAMP COMMENT '当天最后一次行为时间',
    first_pv_time    TIMESTAMP,
    first_fav_time   TIMESTAMP,
    first_cart_time  TIMESTAMP,
    first_buy_time   TIMESTAMP
) COMMENT '用户日汇总（周期快照事实表），粒度：用户×天'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.dws_item_action_1d (
    item_id      BIGINT,
    category_id  BIGINT COMMENT '来自当日 dim_item_full 快照',
    pv_cnt       BIGINT,
    pv_user_cnt  BIGINT COMMENT '浏览用户数（去重）',
    fav_cnt      BIGINT,
    cart_cnt     BIGINT,
    buy_cnt      BIGINT,
    buy_user_cnt BIGINT COMMENT '购买用户数（去重）'
) COMMENT '商品日汇总（周期快照事实表），粒度：商品×天'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.dws_user_milestone_td (
    user_id           BIGINT,
    first_active_date STRING    COMMENT '窗口内首次活跃日期',
    last_active_date  STRING    COMMENT '最近活跃日期',
    active_days       INT       COMMENT '累计活跃天数',
    first_pv_time     TIMESTAMP COMMENT '里程碑：首次浏览',
    first_fav_time    TIMESTAMP COMMENT '里程碑：首次收藏',
    first_cart_time   TIMESTAMP COMMENT '里程碑：首次加购',
    first_buy_time    TIMESTAMP COMMENT '里程碑：首次购买',
    cum_pv_cnt        BIGINT    COMMENT '累计浏览次数',
    cum_buy_cnt       BIGINT    COMMENT '累计购买次数',
    buy_days          INT       COMMENT '有购买的天数'
) COMMENT '用户转化里程碑（累积快照事实表）：一个用户一行，里程碑达成后不再改写；按天保留全量快照'
PARTITIONED BY (dt STRING COMMENT '快照日期，含截至当天出现过的全部用户')
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.tmp_dws_user_milestone_td (
    user_id           BIGINT,
    first_active_date STRING,
    last_active_date  STRING,
    active_days       INT,
    first_pv_time     TIMESTAMP,
    first_fav_time    TIMESTAMP,
    first_cart_time   TIMESTAMP,
    first_buy_time    TIMESTAMP,
    cum_pv_cnt        BIGINT,
    cum_buy_cnt       BIGINT,
    buy_days          INT
) COMMENT '累积快照中间表'
STORED AS PARQUET;

-- ================= ADS =================
CREATE TABLE IF NOT EXISTS ${db}.ads_traffic_stats_1d (
    day_name        STRING,
    is_weekend      BOOLEAN,
    pv_cnt          BIGINT COMMENT 'PV：浏览行为次数',
    uv              BIGINT COMMENT 'UV：有浏览行为的去重用户数',
    active_user_cnt BIGINT COMMENT '活跃用户数：有任意行为的去重用户数',
    fav_cnt         BIGINT,
    cart_cnt        BIGINT,
    buy_cnt         BIGINT,
    buy_user_cnt    BIGINT COMMENT '购买用户数',
    pv_per_uv       DOUBLE COMMENT '人均浏览次数 = pv_cnt / uv',
    buy_user_rate   DOUBLE COMMENT '购买用户占比 = buy_user_cnt / active_user_cnt'
) COMMENT '每日流量与行为概况'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.ads_conversion_funnel_1d (
    step1_pv_users       BIGINT COMMENT '第 1 步：当天有浏览的用户数',
    step2_cart_fav_users BIGINT COMMENT '第 2 步：首次浏览之后又加购或收藏的用户数',
    step3_buy_users      BIGINT COMMENT '第 3 步：第 2 步之后又购买的用户数',
    pv_to_cart_fav_rate  DOUBLE,
    cart_fav_to_buy_rate DOUBLE,
    pv_to_buy_rate       DOUBLE
) COMMENT '当日有序转化漏斗（用户粒度，步骤须按时间先后完成）'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.ads_user_retention (
    cohort_date    STRING COMMENT '同期群日期',
    cohort_type    STRING COMMENT 'new=当天新增用户，active=当天活跃用户',
    retention_day  INT    COMMENT 'N：1=次日留存，3=第 3 日留存',
    cohort_size    BIGINT COMMENT '同期群人数',
    retained_cnt   BIGINT COMMENT 'cohort_date + N 当天仍活跃的人数',
    retention_rate DOUBLE
) COMMENT '用户留存'
PARTITIONED BY (dt STRING COMMENT '计算日 = cohort_date + retention_day')
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.ads_repurchase_rate_td (
    buyer_cnt                    BIGINT COMMENT '截至当天有购买的用户数',
    repurchase_user_cnt_by_day   BIGINT COMMENT '购买天数 >= 2 的用户数',
    repurchase_rate_by_day       DOUBLE COMMENT '复购率（按天口径）',
    repurchase_user_cnt_by_times BIGINT COMMENT '购买次数 >= 2 的用户数',
    repurchase_rate_by_times     DOUBLE COMMENT '复购率（按次数口径）'
) COMMENT '截至当天的累计复购率'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.ads_item_topn_1d (
    rk          INT    COMMENT '名次，浏览次数相同按 item_id 升序',
    item_id     BIGINT,
    category_id BIGINT,
    pv_cnt      BIGINT,
    pv_user_cnt BIGINT,
    buy_cnt     BIGINT
) COMMENT '当日浏览量 Top10 商品'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.ads_category_topn_1d (
    rk           INT    COMMENT '名次，购买次数相同按 category_id 升序',
    category_id  BIGINT,
    buy_cnt      BIGINT,
    buy_user_cnt BIGINT,
    pv_cnt       BIGINT,
    pv_user_cnt  BIGINT,
    buy_user_rate DOUBLE COMMENT '购买用户数 / 浏览用户数'
) COMMENT '当日购买次数 Top10 类目'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.ads_user_continuous_active_7d (
    window_start           STRING COMMENT '统计窗口起始日（不早于数据起始日）',
    active_user_cnt        BIGINT COMMENT '窗口内活跃过的用户数',
    continuous_3d_user_cnt BIGINT COMMENT '窗口内出现过连续 >= 3 天活跃的用户数',
    continuous_3d_ratio    DOUBLE
) COMMENT '近 7 日连续 3 天活跃用户'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ${db}.ads_user_level_dist_1d (
    user_level STRING,
    level_name STRING,
    user_cnt   BIGINT,
    user_ratio DOUBLE
) COMMENT '当天的用户等级分布（从拉链表回溯）'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- ================= 质检结果 =================
CREATE TABLE IF NOT EXISTS ${db}.dq_check_result (
    check_name   STRING  COMMENT '检查项',
    check_type   STRING  COMMENT 'row_conservation / pk_unique / enum / time_range / consistency / not_empty',
    severity     STRING  COMMENT 'error 不通过则阻断下游；warn 只记录',
    target_table STRING,
    bad_cnt      BIGINT  COMMENT '违规行数或差值，0 为通过',
    detail       STRING,
    passed       BOOLEAN,
    checked_at   TIMESTAMP
) COMMENT '质检闸门结果，按 (dt, task) 覆盖写，重跑不重复'
PARTITIONED BY (dt STRING, task STRING)
STORED AS PARQUET;
