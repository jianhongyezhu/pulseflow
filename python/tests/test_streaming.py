import json
from datetime import UTC, datetime
from urllib.request import urlopen
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pulseflow.streaming import (
    SILVER_SCHEMA,
    USER_EVENT_SCHEMA,
    WATERMARK_DELAY,
    PipelineConfig,
    StreamingMetricsCollector,
    _write_parquet,
    aggregate_deduplicated_events,
    bronze_from_kafka,
    build_gold_counts,
    classify_events,
    configure_spark_session,
    deduplicate_events,
    parse_events,
    start_stream_metrics_exporter,
    stop_stream_metrics_exporter,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pulseflow-streaming-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    configure_spark_session(session)
    yield session
    session.stop()


def event_payload(**updates):
    event = {
        "event_id": str(uuid4()),
        "user_id": "user-00001",
        "session_id": "session-00001",
        "event_type": "PLAY",
        "content_id": "track-midnight-drive",
        "device_type": "mobile",
        "country_code": "GB",
        "event_ts": "2026-10-11T12:00:00Z",
        "attributes": {"source": "search", "subscription_tier": "premium"},
    }
    event.update(updates)
    return event


def kafka_frame(spark, payloads, keys=None):
    schema = StructType(
        [
            StructField("key", BinaryType(), True),
            StructField("value", BinaryType(), True),
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", LongType(), False),
            StructField("timestamp", TimestampType(), False),
        ]
    )
    keys = keys or [b"user-00001"] * len(payloads)
    kafka_timestamp = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    rows = [
        (
            key,
            payload if isinstance(payload, bytes) else payload.encode(),
            "user-behaviour.v1",
            2,
            index,
            kafka_timestamp,
        )
        for index, (key, payload) in enumerate(zip(keys, payloads, strict=True))
    ]
    return spark.createDataFrame(rows, schema)


def classify_payloads(spark, payloads, keys=None):
    bronze = bronze_from_kafka(kafka_frame(spark, payloads, keys))
    return classify_events(parse_events(bronze))


def utc_timestamp_string(dataframe, column):
    formatted = dataframe.select(
        F.date_format(column, "yyyy-MM-dd HH:mm:ss").alias("utc_time")
    )
    return formatted.first().utc_time


def test_event_schema_matches_v1_shape():
    assert USER_EVENT_SCHEMA.fieldNames() == [
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "content_id",
        "device_type",
        "country_code",
        "event_ts",
        "attributes",
    ]
    assert USER_EVENT_SCHEMA["attributes"].dataType.simpleString() == "map<string,string>"
    assert SILVER_SCHEMA["processed_at"].dataType.simpleString() == "timestamp"
    assert "processed_at" in SILVER_SCHEMA.fieldNames()


def test_pipeline_config_reads_environment_and_resolves_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
    monkeypatch.setenv("PULSEFLOW_TOPIC", "custom-topic")
    monkeypatch.setenv("PULSEFLOW_DATA_ROOT", str(tmp_path / "state"))

    config = PipelineConfig.from_environment()

    assert config.bootstrap_servers == "broker:19092"
    assert config.topic == "custom-topic"
    assert config.output_path("bronze", "user_behaviour") == (
        tmp_path / "state" / "bronze" / "user_behaviour"
    )
    assert config.checkpoint_path("gold") == tmp_path / "state" / "checkpoints" / "gold"


def test_pipeline_config_metrics_exporter_is_opt_in_and_explicit(monkeypatch):
    monkeypatch.delenv("PULSEFLOW_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("PULSEFLOW_METRICS_HOST", raising=False)
    monkeypatch.delenv("PULSEFLOW_METRICS_PORT", raising=False)

    disabled = PipelineConfig.from_environment()
    assert disabled.metrics_enabled is False
    assert disabled.metrics_host == "127.0.0.1"
    assert disabled.metrics_port == 9108

    monkeypatch.setenv("PULSEFLOW_METRICS_ENABLED", "true")
    monkeypatch.setenv("PULSEFLOW_METRICS_HOST", "192.168.1.4")
    monkeypatch.setenv("PULSEFLOW_METRICS_PORT", "19108")
    enabled = PipelineConfig.from_environment()
    assert enabled.metrics_enabled is True
    assert enabled.metrics_host == "192.168.1.4"
    assert enabled.metrics_port == 19108


@pytest.mark.parametrize("host", ["0.0.0.0", "::"])
def test_pipeline_config_rejects_wildcard_metrics_bind_addresses(monkeypatch, host):
    monkeypatch.setenv("PULSEFLOW_METRICS_HOST", host)

    with pytest.raises(ValueError, match="must not be a wildcard address"):
        PipelineConfig.from_environment()


def test_pipeline_config_rejects_public_metrics_bind_address(monkeypatch):
    monkeypatch.setenv("PULSEFLOW_METRICS_HOST", "8.8.8.8")

    with pytest.raises(ValueError, match="must be localhost or a private IP address"):
        PipelineConfig.from_environment()


def test_pipeline_config_rejects_empty_metrics_bind_address(monkeypatch):
    monkeypatch.setenv("PULSEFLOW_METRICS_HOST", " ")

    with pytest.raises(ValueError, match="must not be empty"):
        PipelineConfig.from_environment()


def test_silver_metrics_map_progress_and_omit_missing_fields():
    collector = StreamingMetricsCollector()
    collector.update(None)
    assert list(collector.registry.collect()) == []

    collector.update(
        {
            "sources": [{"inputRowsPerSecond": 2.5}, {"inputRowsPerSecond": 1.5}],
            "processedRowsPerSecond": 3.0,
            "durationMs": {"triggerExecution": 125},
        }
    )
    samples = {
        metric.name: metric.samples[0].value
        for metric in collector.registry.collect()
    }
    assert samples == {
        "pulseflow_stream_input_rows_per_second": 4.0,
        "pulseflow_stream_processed_rows_per_second": 3.0,
        "pulseflow_stream_batch_duration_ms": 125.0,
    }

    collector.update({"sources": [{}], "durationMs": {}})
    assert list(collector.registry.collect()) == []


def test_silver_metrics_ignore_non_finite_or_invalid_progress_values():
    collector = StreamingMetricsCollector()
    collector.update(
        {
            "sources": [{"inputRowsPerSecond": float("nan")}],
            "processedRowsPerSecond": "unknown",
            "durationMs": {"triggerExecution": -1},
        }
    )

    assert list(collector.registry.collect()) == []


def test_silver_metrics_exporter_serves_mocked_progress_over_http():
    collector, server, thread = start_stream_metrics_exporter("127.0.0.1", 0)
    try:
        collector.update(
            {
                "sources": [{"inputRowsPerSecond": 2.0}],
                "processedRowsPerSecond": 1.5,
                "durationMs": {"triggerExecution": 75},
            }
        )
        port = server.server_address[1]
        with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
            metrics = response.read().decode("utf-8")

        assert "pulseflow_stream_input_rows_per_second 2.0" in metrics
        assert "pulseflow_stream_processed_rows_per_second 1.5" in metrics
        assert "pulseflow_stream_batch_duration_ms 75.0" in metrics
    finally:
        stop_stream_metrics_exporter(server, thread)


def test_bronze_preserves_raw_kafka_fields_and_adds_utc_ingestion_time(spark):
    raw = json.dumps(event_payload()).encode()
    bronze = bronze_from_kafka(kafka_frame(spark, [raw]))
    row = bronze.first()

    assert row.kafka_key == b"user-00001"
    assert row.kafka_value == raw
    assert row.raw_json == raw.decode()
    assert row.topic == "user-behaviour.v1"
    assert row.partition == 2
    assert row.offset == 0
    assert utc_timestamp_string(bronze, "kafka_timestamp") == "2026-10-11 12:00:00"
    assert row.ingestion_timestamp is not None
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


@pytest.mark.parametrize(
    ("payload", "key", "expected_error"),
    [
        ("{malformed", b"user-00001", "malformed_json"),
        (json.dumps({}), b"user-00001", "missing_user_id"),
        (json.dumps(event_payload(user_id="  ")), b"user-00001", "blank_user_id"),
        (json.dumps(event_payload(event_type="SHARE")), b"user-00001", "invalid_event_type"),
        (json.dumps(event_payload(device_type="console")), b"user-00001", "invalid_device_type"),
        (json.dumps(event_payload(country_code="gbr")), b"user-00001", "invalid_country_code"),
        (json.dumps(event_payload(event_id="not-a-uuid")), b"user-00001", "invalid_uuid"),
        (json.dumps(event_payload(event_ts="not-a-timestamp")), b"user-00001", "invalid_event_ts"),
        (
            json.dumps(event_payload(unexpected="field")),
            b"user-00001",
            "unknown_fields",
        ),
        (json.dumps(event_payload()), b"another-user", "kafka_key_user_id_mismatch"),
    ],
)
def test_invalid_records_are_quarantined_with_reason(spark, payload, key, expected_error):
    _, quarantine = classify_payloads(spark, [payload], [key])
    row = quarantine.first()
    assert expected_error in row.quality_errors
    if expected_error == "malformed_json":
        assert row.json_parse_failed is True
        assert row.quality_errors == ["malformed_json"]
    else:
        assert row.json_parse_failed is False
    if expected_error == "missing_user_id":
        assert {
            "missing_event_id",
            "missing_user_id",
            "missing_session_id",
            "missing_event_type",
            "missing_content_id",
            "missing_device_type",
            "missing_country_code",
            "missing_event_ts",
        }.issubset(row.quality_errors)


def test_valid_records_are_typed_and_optional_attributes_are_supported(spark):
    event = event_payload()
    event.pop("attributes")
    valid, quarantine = classify_payloads(spark, [json.dumps(event)])

    row = valid.first()
    assert row.event_id == event["event_id"]
    assert row.user_id == "user-00001"
    assert row.session_id == "session-00001"
    assert row.event_type == "PLAY"
    assert row.content_id == "track-midnight-drive"
    assert row.device_type == "mobile"
    assert row.country_code == "GB"
    assert row.attributes is None
    assert row.json_parse_failed is False
    assert valid.schema["event_id"].dataType.simpleString() == "string"
    assert valid.schema["event_time"].dataType.simpleString() == "timestamp"
    assert valid.schema["kafka_key"].dataType.simpleString() == "binary"
    assert valid.schema["offset"].dataType.simpleString() == "bigint"
    assert utc_timestamp_string(valid, "event_time") == "2026-10-11 12:00:00"
    assert quarantine.count() == 0


def test_explicit_null_attributes_are_quarantined(spark):
    valid, quarantine = classify_payloads(
        spark,
        [json.dumps(event_payload(attributes=None))],
    )

    assert valid.count() == 0
    assert "invalid_attributes" in quarantine.first().quality_errors


def test_spark_rejects_blank_user_id_allowed_by_schema_min_length(spark):
    event = event_payload(user_id=" ")
    valid, quarantine = classify_payloads(spark, [json.dumps(event)])

    assert valid.count() == 0
    assert "blank_user_id" in quarantine.first().quality_errors


def test_static_deduplication_and_gold_never_count_same_event_twice(spark):
    duplicate_id = "00000000-0000-0000-0000-000000000001"
    events = [
        event_payload(event_id=duplicate_id),
        event_payload(event_id=duplicate_id),
        event_payload(
            event_id="00000000-0000-0000-0000-000000000002",
            event_type="PAUSE",
            device_type="desktop",
            event_ts="2026-10-11T12:00:11Z",
        ),
        event_payload(
            event_id="00000000-0000-0000-0000-000000000003",
            event_type="PLAY",
            device_type="mobile",
            event_ts="2026-10-11T12:00:01Z",
        ),
    ]
    valid, quarantine = classify_payloads(spark, [json.dumps(event) for event in events])
    assert quarantine.count() == 0

    deduplicated = deduplicate_events(valid)
    gold = build_gold_counts(valid)
    assert deduplicated.count() == 3
    counts = {
        (row.event_type, row.device_type, row.window_start): row.event_count
        for row in gold.select(
            "event_type",
            "device_type",
            "event_count",
            F.date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        ).collect()
    }
    assert counts == {
        ("PLAY", "mobile", "2026-10-11 12:00:00"): 2,
        ("PAUSE", "desktop", "2026-10-11 12:00:10"): 1,
    }
    assert utc_timestamp_string(gold, "window.start") == "2026-10-11 12:00:00"


def test_classification_preserves_all_records_and_quarantine_context(spark):
    valid_event = event_payload(event_id="00000000-0000-0000-0000-000000000011")
    invalid_json = "{not-json"
    invalid_timestamp = json.dumps(
        event_payload(
            event_id="00000000-0000-0000-0000-000000000012",
            event_ts="not-a-timestamp",
        )
    )
    bronze = bronze_from_kafka(
        kafka_frame(
            spark,
            [json.dumps(valid_event), invalid_json, invalid_timestamp],
            [b"user-00001", b"broken-key", b"user-00001"],
        )
    )
    valid, quarantine = classify_events(parse_events(bronze))

    assert valid.count() + quarantine.count() == bronze.count() == 3
    assert valid.count() == 1
    quarantine_rows = quarantine.orderBy("offset").collect()
    assert [row.offset for row in quarantine_rows] == [1, 2]
    assert [row.topic for row in quarantine_rows] == [
        "user-behaviour.v1",
        "user-behaviour.v1",
    ]
    assert [row.partition for row in quarantine_rows] == [2, 2]
    assert [row.raw_json for row in quarantine_rows] == [invalid_json, invalid_timestamp]
    assert "malformed_json" in quarantine_rows[0].quality_errors
    assert "invalid_event_ts" in quarantine_rows[1].quality_errors
    assert all(row.kafka_value is not None for row in quarantine_rows)
    assert all(row.kafka_timestamp is not None for row in quarantine_rows)


def test_streaming_transformations_build_without_starting_query(spark):
    source = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .load()
        .selectExpr(
            "CAST('user-00001' AS BINARY) AS key",
            "CAST('{\"event_id\":\"00000000-0000-0000-0000-000000000001\","
            "\"user_id\":\"user-00001\",\"session_id\":\"s1\","
            "\"event_type\":\"PLAY\",\"content_id\":\"c1\","
            "\"device_type\":\"mobile\",\"country_code\":\"GB\","
            "\"event_ts\":\"2026-10-11T12:00:00Z\"}' AS BINARY) AS value",
            "'user-behaviour.v1' AS topic",
            "CAST(0 AS INT) AS partition",
            "CAST(value AS BIGINT) AS offset",
            "timestamp",
        )
    )
    bronze = bronze_from_kafka(source)
    valid, _ = classify_events(parse_events(bronze))
    silver = deduplicate_events(valid)
    gold = build_gold_counts(valid)

    assert silver.isStreaming
    assert gold.isStreaming
    assert WATERMARK_DELAY == "30 seconds"
    assert gold.columns == ["window", "event_type", "device_type", "event_count"]


def test_watermarked_silver_dedup_and_gold_aggregation_support_append_mode(spark):
    source = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    silver = source.select(
        F.lit("event-id").alias("event_id"),
        F.col("timestamp").alias("event_time"),
        F.lit("PLAY").alias("event_type"),
        F.lit("mobile").alias("device_type"),
    )
    gold_source = source.select(
        F.col("timestamp").alias("event_time"),
        F.lit("PLAY").alias("event_type"),
        F.lit("mobile").alias("device_type"),
    )
    queries = []
    try:
        queries.append(
            deduplicate_events(silver)
            .writeStream.format("memory")
            .queryName("pulseflow_test_silver_append")
            .outputMode("append")
            .start()
        )
        queries.append(
            aggregate_deduplicated_events(gold_source)
            .writeStream.format("memory")
            .queryName("pulseflow_test_gold_append")
            .outputMode("append")
            .start()
        )
        assert all(query.isActive for query in queries)
    finally:
        for query in queries:
            query.stop()


def test_all_parquet_sink_shapes_start_in_append_mode(spark, tmp_path):
    source = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    bronze = source.select(
        F.col("value"),
        F.to_date("timestamp").alias("kafka_date"),
    )
    silver_events = source.select(
        (F.col("value") % 2).cast("string").alias("event_id"),
        F.col("timestamp").alias("event_time"),
        F.lit("PLAY").alias("event_type"),
        F.lit("mobile").alias("device_type"),
    )
    quarantine = source.filter(F.col("value") < 0)
    gold = aggregate_deduplicated_events(
        source.select(
            F.col("timestamp").alias("event_time"),
            F.lit("PLAY").alias("event_type"),
            F.lit("mobile").alias("device_type"),
        )
    )
    definitions = (
        ("bronze", bronze, "kafka_date"),
        (
            "silver",
            deduplicate_events(silver_events).withColumn(
                "event_date", F.to_date("event_time")
            ),
            "event_date",
        ),
        ("quarantine", quarantine, None),
        ("gold", gold, None),
    )
    queries = []
    try:
        for name, dataframe, partition_by in definitions:
            queries.append(
                _write_parquet(
                    dataframe,
                    f"pulseflow_test_parquet_{name}",
                    tmp_path / name,
                    tmp_path / "checkpoints" / name,
                    partition_by,
                )
            )
        assert all(query.isActive for query in queries)
    finally:
        for query in queries:
            query.stop()
