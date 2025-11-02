{{
  config(
    materialized='table',
    order_by='(date_key)',
    engine='MergeTree()'
  )
}}

SELECT
    toUInt32(toYYYYMMDD(started_at)) AS date_key,
    toDate(started_at) AS full_date,
    CASE toDayOfWeek(started_at)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week,
    toDayOfWeek(started_at) AS day_of_week_num,
    (
        toDayOfWeek(started_at) IN (6, 7)
        OR formatDateTime(started_at, '%m-%d') IN (
            '01-01', '06-19', '07-04', '09-11', '10-31', '11-11', '12-25', '12-31'
        )
        OR (toMonth(started_at) = 1 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 15 AND 21)
        OR (toMonth(started_at) = 2 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 15 AND 21)
        OR (toMonth(started_at) = 5 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 25 AND 31)
        OR (toMonth(started_at) = 9 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 1 AND 7)
        OR (toMonth(started_at) = 10 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 8 AND 14)
        OR (toMonth(started_at) = 11 AND toDayOfWeek(started_at) = 2 AND toDayOfMonth(started_at) BETWEEN 1 AND 7)
        OR (toMonth(started_at) = 11 AND toDayOfWeek(started_at) = 4 AND toDayOfMonth(started_at) BETWEEN 22 AND 28)
    ) AS is_holiday,
    CASE
        WHEN toMonth(started_at) IN (12, 1, 2) THEN 'Winter'
        WHEN toMonth(started_at) IN (3, 4, 5) THEN 'Spring'
        WHEN toMonth(started_at) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    toMonth(started_at) AS month,
    toYear(started_at) AS year
FROM {{ ref('stg_citibike_trips') }}
GROUP BY 
    toUInt32(toYYYYMMDD(started_at)),
    toDate(started_at),
    toDayOfWeek(started_at),
    toMonth(started_at),
    toYear(started_at),
    (
        toDayOfWeek(started_at) IN (6, 7)
        OR formatDateTime(started_at, '%m-%d') IN (
            '01-01', '06-19', '07-04', '09-11', '10-31', '11-11', '12-25', '12-31'
        )
        OR (toMonth(started_at) = 1 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 15 AND 21)
        OR (toMonth(started_at) = 2 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 15 AND 21)
        OR (toMonth(started_at) = 5 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 25 AND 31)
        OR (toMonth(started_at) = 9 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 1 AND 7)
        OR (toMonth(started_at) = 10 AND toDayOfWeek(started_at) = 1 AND toDayOfMonth(started_at) BETWEEN 8 AND 14)
        OR (toMonth(started_at) = 11 AND toDayOfWeek(started_at) = 2 AND toDayOfMonth(started_at) BETWEEN 1 AND 7)
        OR (toMonth(started_at) = 11 AND toDayOfWeek(started_at) = 4 AND toDayOfMonth(started_at) BETWEEN 22 AND 28)
    ),
    CASE
        WHEN toMonth(started_at) IN (12, 1, 2) THEN 'Winter'
        WHEN toMonth(started_at) IN (3, 4, 5) THEN 'Spring'
        WHEN toMonth(started_at) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END