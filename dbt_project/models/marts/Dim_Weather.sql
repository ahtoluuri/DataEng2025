{{
  config(
    materialized='table',
    order_by='(weather_key)',
    engine='MergeTree()'
  )
}}

SELECT
    toUInt32(toYYYYMMDD(observation_time) * 100 + toHour(observation_time)) AS weather_key,
    CASE
        WHEN apparent_temperature < 0 THEN 'Freezing'
        WHEN apparent_temperature BETWEEN 0 AND 10 THEN 'Cold'
        WHEN apparent_temperature BETWEEN 10 AND 20 THEN 'Mild'
        WHEN apparent_temperature BETWEEN 20 AND 30 THEN 'Warm'
        ELSE 'Hot'
    END AS apparent_temp,
    CASE
        WHEN relative_humidity_2m < 40 THEN 'Dry'
        WHEN relative_humidity_2m BETWEEN 40 AND 70 THEN 'Comfortable'
        ELSE 'Humid'
    END AS humidity_category,
    toInt32(precipitation_probability) AS precipitation_probability,
    CASE
        WHEN precipitation_probability = 0 THEN 'No Rain'
        WHEN precipitation_probability < 50 THEN 'Light Rain'
        ELSE 'Heavy Rain'
    END AS precipitation_category,
    toInt32(cloud_coverage) AS cloud_coverage,
    CASE
        WHEN wind_speed_10m < 5 THEN 'Calm'
        WHEN wind_speed_10m < 15 THEN 'Breezy'
        ELSE 'Windy'
    END AS wind_category
FROM {{ ref('stg_weather') }}
GROUP BY 
    toUInt32(toYYYYMMDD(observation_time) * 100 + toHour(observation_time)),
    apparent_temp, 
    humidity_category, 
    precipitation_probability, 
    precipitation_category, 
    cloud_coverage, 
    wind_category