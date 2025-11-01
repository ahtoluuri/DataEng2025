{{ config(materialised='table')}}

select * from {{ ref('stg_weather') }}
