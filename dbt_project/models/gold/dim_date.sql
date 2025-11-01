{{ config(materialized='table') }}

with dates as (
    select toDate('2022-01-01') + number as full_date
    from system.numbers
    limit 3650
)
select
    toYYYYMMDD(full_date) as date_key,
    full_date,
    toDayOfWeek(full_date) as day_of_week_num,
    case toDayOfWeek(full_date)
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
        when 7 then 'Sunday'
    end as day_of_week,
    toMonth(full_date) as month,
    toYear(full_date) as year,
    toDayOfMonth(full_date) as day
from dates
