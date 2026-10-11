SELECT
    event_type,
    device_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS users,
    MIN(event_time) AS first_event_ts,
    MAX(event_time) AS latest_event_ts
FROM read_parquet(
    'data/silver/user_behaviour/**/*.parquet',
    hive_partitioning = true
)
GROUP BY
    event_type,
    device_type
ORDER BY
    event_count DESC;
