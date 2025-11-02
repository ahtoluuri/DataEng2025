{{
  config(
    materialized='table',
    order_by='(time_key)',
    engine='MergeTree()'
  )
}}

SELECT
    (h * 10000 + m * 100 + s) AS time_key,
    h AS hour,
    m AS minute,
    s AS second,
    CASE
        WHEN h BETWEEN 5 AND 11 THEN 'Morning'
        WHEN h BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN h BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS part_of_day
FROM (SELECT arrayJoin(range(24)) AS h) AS hours
CROSS JOIN (SELECT arrayJoin(range(60)) AS m) AS minutes
CROSS JOIN (SELECT arrayJoin(range(60)) AS s) AS seconds