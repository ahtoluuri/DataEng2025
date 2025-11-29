-- How do weather conditions impact daily and hourly ridership volumes? 
-- Daily:
SELECT
    d.full_date,
    COALESCE(w.apparent_temp, 'Unknown') AS apparent_temp,
    COALESCE(w.precipitation_category, 'Unknown') AS precipitation_category,
    COALESCE(w.wind_category, 'Unknown') AS wind_category,
    COUNT(f.trip_id) AS total_trips
FROM "dataeng"."Fact_Bike_Trip" AS f
JOIN "dataeng"."Dim_Date" AS d ON f.start_date_key = d.date_key
LEFT JOIN "dataeng"."Dim_Weather" AS w ON f.weather_key = w.weather_key
GROUP BY d.full_date, w.apparent_temp, w.precipitation_category, w.wind_category
ORDER BY d.full_date, apparent_temp, precipitation_category, wind_category;

-- Hourly:
SELECT
    d.full_date,
    t.hour,
    COALESCE(w.apparent_temp, 'Unknown') AS apparent_temp,
    COALESCE(w.precipitation_category, 'Unknown') AS precipitation_category,
    COALESCE(w.wind_category, 'Unknown') AS wind_category,
    COUNT(f.trip_id) AS hourly_trips
FROM "dataeng"."Fact_Bike_Trip" AS f
JOIN "dataeng"."Dim_Date" AS d ON f.start_date_key = d.date_key
JOIN "dataeng"."Dim_Time" AS t ON f.start_time_key = t.time_key
LEFT JOIN "dataeng"."Dim_Weather" AS w ON f.weather_key = w.weather_key
GROUP BY d.full_date, t.hour, w.apparent_temp, w.precipitation_category, w.wind_category
ORDER BY d.full_date, t.hour, apparent_temp, precipitation_category, wind_category;

-- What is the average trip duration under different precipitation categories? 
SELECT
    COALESCE(w.precipitation_category, 'Unknown') AS precipitation_category,
    ROUND(AVG(f.trip_duration), 2) AS avg_trip_duration_minutes
FROM "dataeng"."Fact_Bike_Trip" AS f
LEFT JOIN "dataeng"."Dim_Weather" AS w ON f.weather_key = w.weather_key
GROUP BY w.precipitation_category
ORDER BY avg_trip_duration_minutes DESC;

-- Are certain stations more popular as starting points during adverse weather (e.g., heavy rain) compared to dry hours? 
SELECT
    s.station_name,
    w.precipitation_category,
    COUNT(f.trip_id) AS trip_count
FROM "dataeng"."Fact_Bike_Trip" AS f
JOIN "dataeng"."Dim_Station" AS s ON f.start_station_key = s.station_key
JOIN "dataeng"."Dim_Weather" AS w ON f.weather_key = w.weather_key
WHERE w.precipitation_category IN ('Heavy Rain', 'No Rain')
GROUP BY s.station_name, w.precipitation_category
ORDER BY s.station_name, trip_count DESC;

-- Do casual riders and annual members exhibit different ridership patterns in response to precipitation changes? 
SELECT
    f.rider_type,
    COALESCE(w.precipitation_category, 'Unknown') AS precipitation_category,
    COUNT(f.trip_id) AS total_trips,
    ROUND(AVG(f.trip_duration), 1) AS avg_duration_minutes
FROM "dataeng"."Fact_Bike_Trip" AS f
LEFT JOIN "dataeng"."Dim_Weather" AS w ON f.weather_key = w.weather_key
GROUP BY f.rider_type, w.precipitation_category
ORDER BY f.rider_type, total_trips DESC;

-- What is the peak ridership hour, and how does this peak shift based on season and apparent temperature?

-- Peak ridership hour per season:
WITH hourly_ridership AS (
    SELECT
        COALESCE(d.season, 'Unknown') AS season,
        t.hour AS hour_of_day,
        COUNT(f.trip_id) AS total_trips
    FROM "dataeng"."Fact_Bike_Trip" AS f
    JOIN "dataeng"."Dim_Date" AS d ON f.start_date_key = d.date_key
    JOIN "dataeng"."Dim_Time" AS t ON f.start_time_key = t.time_key
    GROUP BY COALESCE(d.season, 'Unknown'), t.hour
)
SELECT
    season,
    hour_of_day AS peak_hour,
    total_trips
FROM (
    SELECT
        season,
        hour_of_day,
        total_trips,
        RANK() OVER (PARTITION BY season ORDER BY total_trips DESC) AS rnk
    FROM hourly_ridership
) AS ranked
WHERE rnk = 1
ORDER BY season;

-- Peak ridership hour by apparent temperature category:
WITH temp_hourly AS (
    SELECT
        COALESCE(w.apparent_temp, 'Unknown') AS apparent_temp,
        t.hour AS hour_of_day,
        COUNT(f.trip_id) AS total_trips
    FROM "dataeng"."Fact_Bike_Trip" AS f
    JOIN "dataeng"."Dim_Time" AS t ON f.start_time_key = t.time_key
    LEFT JOIN "dataeng"."Dim_Weather" AS w ON f.weather_key = w.weather_key
    GROUP BY COALESCE(w.apparent_temp, 'Unknown'), t.hour
)
SELECT
    apparent_temp,
    hour_of_day AS peak_hour,
    total_trips
FROM (
    SELECT
        apparent_temp,
        hour_of_day,
        total_trips,
        RANK() OVER (PARTITION BY apparent_temp ORDER BY total_trips DESC) AS rnk
    FROM temp_hourly
) AS ranked
WHERE rnk = 1
ORDER BY apparent_temp;
