
-- Insert data from specified citibike_tripdata.csv file
-- Assumes that the JC-202509-citibike-tripdata.csv is in the sample_data/ directory.
INSERT INTO citibike.raw_citibike_trips
(
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual
)
SELECT toString(ride_id), toString(rideable_type), toDateTime(started_at), toDateTime(ended_at), toString(start_station_name),
       toString(start_station_id), toString(end_station_name), toString(end_station_id), 
       toDecimal32(start_lat, 6), toDecimal32(start_lng, 6), toDecimal32(end_lat, 6), toDecimal32(end_lng, 6), toString(member_casual)
FROM file('JC-202509-citibike-tripdata.csv', 'CSVWithNames');

-- Verify that specified trip data has been loaded
SELECT 'raw_citibike_trips' AS table_name, count() AS rows FROM citibike.raw_citibike_trips;