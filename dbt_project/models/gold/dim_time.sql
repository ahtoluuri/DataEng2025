{{ config(materialized='table') }}

with times as (
    select number as seconds_since_midnight
    from system.numbers
    limit 24*60*60
)
select
    (intDiv(seconds_since_midnight, 3600) * 10000)
    + (intDiv(seconds_since_midnight % 3600, 60) * 100)
    + (seconds_since_midnight % 60) as time_key,
    intDiv(seconds_since_midnight, 3600) as hour,
    intDiv(seconds_since_midnight % 3600, 60) as minute,
    seconds_since_midnight % 60 as second,
    case
        when hour between 5 and 11 then 'Morning'
        when hour between 12 and 17 then 'Afternoon'
        when hour between 18 and 21 then 'Evening'
        else 'Night'
    end as part_of_day
from times
