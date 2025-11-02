{{
  config(
    materialized='table',
    order_by='(station_key)',
    engine='MergeTree()'
  )
}}

SELECT
    row_number() OVER () AS station_key,
    start_station_id AS station_id,
    start_station_name AS station_name,
    any(start_lat) AS latitude,
    any(start_lng) AS longitude,
    1 AS current_flag,
    toDate('2020-01-01') AS valid_from,
    toDate('2099-12-31') AS valid_to
FROM {{ ref('stg_citibike_trips') }}
WHERE start_station_id != ''
GROUP BY start_station_id, start_station_name