{{
  config(
    materialized='table',
    order_by='(trip_id)',
    engine='MergeTree()'
  )
}}

WITH numbered_trips AS (
  SELECT
    row_number() OVER (ORDER BY started_at, ride_id) AS trip_id,
    started_at,
    toUInt32(toYYYYMMDD(started_at)) AS start_date_key,
    toUInt32(toYYYYMMDD(ended_at)) AS end_date_key,
    toUInt32(toHour(started_at) * 10000 + toMinute(started_at) * 100 + toSecond(started_at)) AS start_time_key,
    toUInt32(toHour(ended_at) * 10000 + toMinute(ended_at) * 100 + toSecond(ended_at)) AS stop_time_key,
    start_station_id,
    end_station_id,
    member_casual AS rider_type,
    rideable_type AS bike_type,
    dateDiff('second', started_at, ended_at) / 60 AS trip_duration,
    sqrt(pow(start_lat - end_lat, 2) + pow(start_lng - end_lng, 2)) * 111 AS trip_distance_km
  FROM {{ ref('stg_citibike_trips') }}
  {% if target.name == 'dev' %}
  LIMIT 100000
  {% endif %}
)

SELECT
  t.trip_id,
  t.start_date_key,
  t.end_date_key,
  t.start_time_key,
  t.stop_time_key,
  coalesce(st_start.station_key, 0) AS start_station_key,
  coalesce(st_end.station_key, 0) AS end_station_key,
  toUInt32(toYYYYMMDD(started_at) * 100 + toHour(started_at)) AS weather_key,  
  t.rider_type,
  t.bike_type,
  t.trip_duration,
  t.trip_distance_km
FROM numbered_trips AS t
LEFT JOIN {{ ref('Dim_Station') }} AS st_start
  ON t.start_station_id = st_start.station_id
LEFT JOIN {{ ref('Dim_Station') }} AS st_end
  ON t.end_station_id = st_end.station_id
