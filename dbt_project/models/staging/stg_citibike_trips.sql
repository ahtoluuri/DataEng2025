{{ config(materialized='view') }}

select
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat as latitude_start,
    start_lng as longitude_start,
    end_lat as latitude_end,
    end_lng as longitude_end,
    member_casual as rider_type
from {{ source('citibike', 'raw_citibike_trips') }}
where ride_id is not null
