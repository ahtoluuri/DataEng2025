CREATE OR REPLACE VIEW dataeng.vw_trip_summary_full AS
SELECT
    f.trip_id,
    d_s.full_date as startdate,
    d_e.full_date AS enddate,
    t_s.part_of_day AS trip_start_part_of_day,
    t_e.part_of_day AS trip_end_part_of_day,
    s.station_name AS start_station_name,
    e.station_name AS end_station_name,
    f.rider_type,
    f.bike_type,
    f.trip_duration,
    w.apparent_temp,
    w.humidity_category,
    w.wind_category,
    w.precipitation_category
FROM dataeng.Fact_Bike_Trip f
LEFT JOIN dataeng.Dim_Station s ON f.start_station_key = s.station_key
LEFT JOIN dataeng.Dim_Station e ON f.end_station_key = e.station_key
LEFT JOIN dataeng.Dim_Weather w ON f.weather_key = w.weather_key
LEFT JOIN dataeng.Dim_Date d_s ON f.start_date_key = d_s.date_key
LEFT JOIN dataeng.Dim_Date d_e ON f.end_date_key = d_e.date_key
LEFT JOIN dataeng.Dim_Time t_s ON f.start_time_key = t_s.time_key
LEFT JOIN dataeng.Dim_Time t_e ON f.stop_time_key = t_e.time_key;


CREATE OR REPLACE VIEW dataeng.vw_trip_summary_limited
SQL SECURITY DEFINER
AS
SELECT
    f.trip_id,
    d_s.full_date as startdate,
    d_e.full_date AS enddate,
    t_s.part_of_day AS trip_start_part_of_day,
    t_e.part_of_day AS trip_end_part_of_day,
    SHA256(s.station_name) AS start_station_name,
    SHA256(e.station_name) AS end_station_name,
    SHA256(f.rider_type) AS rider_type,               
    SHA256(f.bike_type) AS bike_type,
    f.trip_duration,
    w.apparent_temp,
    w.humidity_category,
    w.wind_category,
    w.precipitation_category
FROM dataeng.Fact_Bike_Trip f
LEFT JOIN dataeng.Dim_Station s ON f.start_station_key = s.station_key
LEFT JOIN dataeng.Dim_Station e ON f.end_station_key = e.station_key
LEFT JOIN dataeng.Dim_Weather w ON f.weather_key = w.weather_key
LEFT JOIN dataeng.Dim_Date d_s ON f.start_date_key = d_s.date_key
LEFT JOIN dataeng.Dim_Date d_e ON f.end_date_key = d_e.date_key
LEFT JOIN dataeng.Dim_Time t_s ON f.start_time_key = t_s.time_key
LEFT JOIN dataeng.Dim_Time t_e ON f.stop_time_key = t_e.time_key;