ATTACH TABLE _ UUID '404f66db-1d4c-41b6-b72b-5ea5d28d686d'
(
    `ride_id` String,
    `rideable_type` String,
    `started_at` DateTime64(3),
    `ended_at` DateTime64(3),
    `start_station_name` String,
    `start_station_id` String,
    `end_station_name` String,
    `end_station_id` String,
    `start_lat` Decimal(9, 6),
    `start_lng` Decimal(9, 6),
    `end_lat` Decimal(9, 6),
    `end_lng` Decimal(9, 6),
    `member_casual` String,
    `loaded_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY ride_id
SETTINGS index_granularity = 8192
