{{ config(materialized='table') }}

select distinct
    *
from {{ ref('stg_citibike_trips') }}
where start_station_id != ''
