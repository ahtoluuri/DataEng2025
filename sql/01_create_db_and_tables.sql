DROP DATABASE IF EXISTS citibike;
CREATE DATABASE IF NOT EXISTS citibike;

CREATE TABLE IF NOT EXISTS citibike.raw_citibike_trips
(
    ride_id String,
    rideable_type String,
    started_at DateTime,
    ended_at DateTime,
    start_station_name String,
    start_station_id String,
    end_station_name String,
    end_station_id String,
    start_lat Decimal(9,6),
    start_lng Decimal(9,6),
    end_lat Decimal(9,6),
    end_lng Decimal(9,6),
    member_casual String,
    loaded_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (ride_id);

-- CREATE TABLE IF NOT EXISTS citibike.raw_weather