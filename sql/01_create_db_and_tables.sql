DROP DATABASE IF EXISTS citibike;
CREATE DATABASE IF NOT EXISTS citibike;

CREATE TABLE IF NOT EXISTS citibike.raw_citibike_trips
(
    ride_id String,
    rideable_type String,
    started_at DateTime64(3),
    ended_at DateTime64(3),
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

CREATE TABLE IF NOT EXISTS citibike.raw_weather
(
    observation_time DateTime,
    temperature Float32,
    apparent_temperature Float32,
    relative_humidity_2m Float32,
    precipitation_probability Float32,
    precipitation_mm Float32,
    cloud_coverage Float32,
    uv_index Float32,
    wind_speed_10m Float32,
    wind_direction_deg Float32,
    loaded_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(observation_time)
ORDER BY (observation_time)