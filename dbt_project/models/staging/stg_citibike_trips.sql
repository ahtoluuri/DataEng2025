{{
  config(
    materialized='view'
  )
}}

SELECT
    ride_id,
    started_at,
    ended_at,
    start_station_id,
    start_station_name,
    start_lat,
    start_lng,
    end_station_id,
    end_station_name,
    end_lat,
    end_lng,
    member_casual,
    rideable_type
FROM {{ source('citibike', 'raw_citibike_trips') }}