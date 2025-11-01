{{ config(materialized='table') }}

select
    *
from {{ ref('stg_citibike_trips') }}
where ride_id is not null
