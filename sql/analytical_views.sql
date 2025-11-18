CREATE OR REPLACE VIEW dataeng.vw_trip_summary_full AS
SELECT
    f.trip_id,
    f.start_date_key,
    f.end_date_key,
    s.station_name AS start_station_name,
    e.station_name AS end_station_name,
    f.rider_type,
    f.bike_type,
    f.trip_duration,
    w.apparent_temp,
    w.humidity_category,
    w.wind_category
FROM dataeng.Fact_Bike_Trip f
LEFT JOIN dataeng.Dim_Station s ON f.start_station_key = s.station_key
LEFT JOIN dataeng.Dim_Station e ON f.end_station_key = e.station_key
LEFT JOIN dataeng.Dim_Weather w ON f.weather_key = w.weather_key;

CREATE OR REPLACE VIEW dataeng.vw_trip_summary_masked AS
SELECT
    f.trip_id,
    f.start_date_key,
    f.end_date_key,
    s.station_name AS start_station_name_masked,   
    e.station_name AS end_station_name_masked,
    f.rider_type AS rider_type_masked,               
    f.bike_type as bike_type_masked,
    f.trip_duration,
    w.apparent_temp,
    w.humidity_category,
    w.wind_category
FROM dataeng.Fact_Bike_Trip f
LEFT JOIN dataeng.Dim_Station s ON f.start_station_key = s.station_key
LEFT JOIN dataeng.Dim_Station e ON f.end_station_key = e.station_key
LEFT JOIN dataeng.Dim_Weather w ON f.weather_key = w.weather_key;
-- TODO: pseudonymization