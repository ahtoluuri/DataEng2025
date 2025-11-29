ATTACH TABLE _ UUID '93370c31-69af-4d7d-8993-65dd1f45ada6'
(
    `observation_time` DateTime,
    `temperature` Float32,
    `apparent_temperature` Float32,
    `relative_humidity_2m` Float32,
    `precipitation_probability` Float32,
    `precipitation_mm` Float32,
    `cloud_coverage` Float32,
    `uv_index` Float32,
    `wind_speed_10m` Float32,
    `wind_direction_deg` Float32,
    `loaded_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(observation_time)
ORDER BY observation_time
SETTINGS index_granularity = 8192
