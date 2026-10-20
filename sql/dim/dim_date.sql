-- 日期维表：静态生成 2017 全年
INSERT OVERWRITE TABLE ${db}.dim_date
SELECT
    date_format(d, 'yyyy-MM-dd')                  AS date_id,
    year(d)                                       AS year,
    month(d)                                      AS month,
    day(d)                                        AS day,
    weekofyear(d)                                 AS week_of_year,
    weekday(d) + 1                                AS day_of_week,
    element_at(array('周一', '周二', '周三', '周四', '周五', '周六', '周日'), weekday(d) + 1) AS day_name,
    weekday(d) >= 5                               AS is_weekend
FROM (
    SELECT explode(sequence(DATE'2017-01-01', DATE'2017-12-31', INTERVAL 1 DAY)) AS d
) t;
