"""Real Kafka-to-Parquet integration coverage; run explicitly via the docs command."""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pyspark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pulseflow.streaming import (
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    PipelineConfig,
    configure_spark_session,
    start_pipeline,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPOSITORY_ROOT / "infra" / "compose" / "docker-compose.yml"
KAFKA_CLI = "/opt/kafka/bin"
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
WAIT_TIMEOUT_SECONDS = 150
BROKER_START_TIMEOUT_SECONDS = 120


def _kafka_cli(*arguments: str, timeout: int = 15) -> str:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "broker",
            f"{KAFKA_CLI}/kafka-topics.sh",
            "--bootstrap-server",
            "broker:19092",
            *arguments,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            "Kafka admin command failed; ensure the existing Compose broker is healthy. "
            f"Command output: {result.stdout}\n{result.stderr}"
        )
    return result.stdout


def _publish(topic: str, keyed_payloads: list[tuple[str, str]]) -> None:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "broker",
            f"{KAFKA_CLI}/kafka-console-producer.sh",
            "--bootstrap-server",
            "broker:19092",
            "--topic",
            topic,
            "--property",
            "parse.key=true",
            "--property",
            f"key.separator={chr(9)}",
            "--producer-property",
            "acks=all",
        ],
        input="".join(f"{key}\t{payload}\n" for key, payload in keyed_payloads),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"Kafka producer failed for topic {topic}: {result.stdout}\n{result.stderr}"
        )


def _parquet_rows(spark: SparkSession, path: Path, schema=None):
    files = sorted(path.rglob("*.parquet"))
    if not files:
        return []
    reader = spark.read
    if schema is not None:
        reader = reader.schema(schema)
    return reader.parquet(str(path)).collect()


def _query_by_name(queries):
    return {query.name: query for query in queries}


def _query_status(queries) -> str:
    summaries = []
    for query in queries:
        progress = query.lastProgress or {}
        summaries.append(
            f"{query.name}: active={query.isActive}, "
            f"status={query.status}, progress={progress}"
        )
    return "; ".join(summaries)


def _ensure_queries_healthy(queries) -> None:
    for query in queries:
        if not query.isActive:
            raise AssertionError(
                f"Streaming query {query.name} stopped unexpectedly: "
                f"{query.exception()}; {_query_status(queries)}"
            )


def _total_input_rows(query) -> int:
    return sum(
        source.get("numInputRows", 0)
        for progress in query.recentProgress
        for source in progress.get("sources", [])
    )


def _watermark(query) -> datetime | None:
    progress = query.lastProgress or {}
    value = progress.get("eventTime", {}).get("watermark")
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _wait_for_broker() -> None:
    deadline = time.monotonic() + BROKER_START_TIMEOUT_SECONDS
    last_error = None
    while time.monotonic() < deadline:
        try:
            _kafka_cli("--list", timeout=8)
            return
        except (OSError, subprocess.SubprocessError, RuntimeError) as error:
            last_error = error
            time.sleep(2)
    pytest.fail(
        f"Compose Kafka did not become reachable within {BROKER_START_TIMEOUT_SECONDS}s. "
        "Start it with `docker compose -f infra/compose/docker-compose.yml up -d broker`; "
        "the test will not remove its named volume. "
        f"Last broker error: {last_error}"
    )


def _wait_for(description: str, condition, queries, timeout=WAIT_TIMEOUT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _ensure_queries_healthy(queries)
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError(
        f"Timed out after {timeout}s waiting for {description}. "
        f"Streaming progress: {_query_status(queries)}"
    )


def _event(event_id: str, timestamp: datetime, user_id="integration-user") -> dict:
    return {
        "event_id": event_id,
        "user_id": user_id,
        "session_id": "integration-session",
        "event_type": "PLAY",
        "content_id": "integration-track",
        "device_type": "desktop",
        "country_code": "GB",
        "event_ts": timestamp.isoformat().replace("+00:00", "Z"),
        "attributes": {"source": "streaming-integration"},
    }


def test_kafka_streaming_writes_bronze_silver_gold_and_quarantine(tmp_path, monkeypatch):
    if pyspark.__version__ != "4.2.0":
        pytest.fail(f"The streaming integration requires PySpark 4.2.0, got {pyspark.__version__}.")

    topic = f"pulseflow-it-{uuid4().hex}"
    data_root = (tmp_path / "isolated-pulseflow-data").resolve()
    assert REPOSITORY_ROOT not in data_root.parents, (
        f"Integration data root must be outside the repository: {data_root}"
    )
    spark = None
    queries = []
    topic_created = False

    try:
        _wait_for_broker()

        _kafka_cli(
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        )
        topic_created = True

        monkeypatch.setenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        )
        monkeypatch.setenv("PULSEFLOW_TOPIC", topic)
        monkeypatch.setenv("PULSEFLOW_DATA_ROOT", str(data_root))
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("PulseFlow Kafka-to-Parquet integration test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", KAFKA_CONNECTOR)
            .getOrCreate()
        )
        configure_spark_session(spark)
        config = PipelineConfig.from_environment()
        assert config.data_root == data_root
        assert config.topic == topic
        queries = start_pipeline(spark, config)
        named_queries = _query_by_name(queries)
        base_time = datetime.now(UTC).replace(second=0, microsecond=0)
        event_id = str(uuid4())
        first_event = _event(event_id, base_time)
        first_payload = json.dumps(first_event, separators=(",", ":"))

        _publish(topic, [("integration-user", first_payload)])
        _wait_for(
            "first Bronze and Silver record",
            lambda: len(_parquet_rows(
                spark, config.output_path("bronze", "user_behaviour"), BRONZE_SCHEMA
            )) >= 1
            and len(_parquet_rows(
                spark, config.output_path("silver", "user_behaviour"), SILVER_SCHEMA
            )) >= 1,
            queries,
        )

        _publish(topic, [("integration-user", first_payload)])
        _wait_for(
            "duplicate to reach Silver deduplication",
            lambda: len(_parquet_rows(
                spark, config.output_path("bronze", "user_behaviour"), BRONZE_SCHEMA
            )) >= 2
            and _total_input_rows(named_queries["pulseflow_silver"]) >= 2,
            queries,
        )
        silver_path = config.output_path("silver", "user_behaviour")
        assert [row.event_id for row in _parquet_rows(spark, silver_path, SILVER_SCHEMA)] == [
            event_id
        ], "A duplicate event_id within the active watermark must not inflate Silver."

        malformed_json = "{not-json"
        _publish(topic, [("invalid-user", malformed_json)])
        quarantine_path = config.output_path("quarantine", "user_behaviour")
        _wait_for(
            "malformed Kafka record to reach Quarantine",
            lambda: any(
                row.raw_json == malformed_json
                for row in _parquet_rows(spark, quarantine_path)
            ),
            queries,
        )

        future_event = _event(str(uuid4()), base_time + timedelta(seconds=120))
        future_payload = json.dumps(future_event, separators=(",", ":"))
        _publish(
            topic,
            [("integration-user", future_payload)],
        )
        late_event_time = base_time + timedelta(seconds=10)
        _wait_for(
            "Silver watermark to pass the late-event timestamp",
            lambda: (
                _watermark(named_queries["pulseflow_silver"]) is not None
                and _watermark(named_queries["pulseflow_silver"]) > late_event_time
            ),
            queries,
        )

        late_event_id = str(uuid4())
        late_event = _event(late_event_id, late_event_time, user_id="late-user")
        late_payload = json.dumps(late_event, separators=(",", ":"))
        _publish(
            topic,
            [("late-user", late_payload)],
        )
        _wait_for(
            "late event to be consumed and filtered by the Silver watermark",
            lambda: _total_input_rows(named_queries["pulseflow_silver"]) >= 5,
            queries,
        )
        assert late_event_id not in {
            row.event_id for row in _parquet_rows(spark, silver_path, SILVER_SCHEMA)
        }, "Silver must drop an event older than its effective event-time watermark."

        closing_event = _event(str(uuid4()), base_time + timedelta(seconds=180))
        closing_payload = json.dumps(closing_event, separators=(",", ":"))
        _publish(
            topic,
            [("integration-user", closing_payload)],
        )
        _wait_for(
            "closing event in Silver",
            lambda: len(_parquet_rows(spark, silver_path, SILVER_SCHEMA)) == 3,
            queries,
        )
        gold_path = config.output_path("gold", "event_counts")
        _wait_for(
            "Gold windows to close after deterministic event-time advancement",
            lambda: (
                _watermark(named_queries["pulseflow_gold"]) is not None
                and _watermark(named_queries["pulseflow_gold"])
                > base_time + timedelta(seconds=130)
                and len(_parquet_rows(spark, gold_path)) >= 2
            ),
            queries,
        )

        bronze_rows = _parquet_rows(
            spark, config.output_path("bronze", "user_behaviour"), BRONZE_SCHEMA
        )
        assert len(bronze_rows) == 6
        assert all(row.topic == topic for row in bronze_rows)
        assert len({row.offset for row in bronze_rows}) == 6
        assert all(
            row.partition == 0
            and row.kafka_timestamp is not None
            and row.kafka_key is not None
            and row.kafka_value is not None
            for row in bronze_rows
        )
        expected_key_by_payload = {
            first_payload: b"integration-user",
            future_payload: b"integration-user",
            closing_payload: b"integration-user",
            late_payload: b"late-user",
            malformed_json: b"invalid-user",
        }
        assert all(
            row.kafka_key == expected_key_by_payload[row.raw_json] for row in bronze_rows
        )
        assert sum(row.raw_json == first_payload for row in bronze_rows) == 2
        bronze_path = config.output_path("bronze", "user_behaviour")
        bronze_dates = {
            row.kafka_date
            for row in spark.read.schema(BRONZE_SCHEMA)
            .parquet(str(bronze_path))
            .select(F.date_format("kafka_timestamp", "yyyy-MM-dd").alias("kafka_date"))
            .distinct()
            .collect()
        }
        bronze_partitions = {
            path.name.removeprefix("kafka_date=") for path in bronze_path.glob("kafka_date=*")
        }
        assert bronze_partitions == bronze_dates

        silver_rows = _parquet_rows(spark, silver_path, SILVER_SCHEMA)
        assert all(row.processed_at is not None for row in silver_rows)
        assert {row.event_id for row in silver_rows} == {
            event_id,
            future_event["event_id"],
            closing_event["event_id"],
        }
        expected_event_dates = {
            datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
            .date()
            .isoformat()
            for event in (first_event, future_event, closing_event)
        }
        assert {row.event_date.isoformat() for row in silver_rows} == expected_event_dates
        actual_event_partitions = {
            path.name.removeprefix("event_date=")
            for path in silver_path.glob("event_date=*")
        }
        assert actual_event_partitions == expected_event_dates

        quarantine_rows = _parquet_rows(spark, quarantine_path)
        rejected = [row for row in quarantine_rows if row.raw_json == malformed_json]
        assert len(rejected) == 1
        assert rejected[0].quality_errors == ["malformed_json"]
        assert rejected[0].topic == topic
        assert rejected[0].kafka_value == malformed_json.encode()

        gold_rows = (
            spark.read.parquet(str(gold_path))
            .select(
                F.date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
                "event_type",
                "device_type",
                "event_count",
            )
            .collect()
        )
        observed_gold = {
            (row.window_start, row.event_type, row.device_type): row.event_count
            for row in gold_rows
        }
        assert observed_gold == {
            (base_time.strftime("%Y-%m-%d %H:%M:%S"), "PLAY", "desktop"): 1,
            (
                (base_time + timedelta(seconds=120)).strftime("%Y-%m-%d %H:%M:%S"),
                "PLAY",
                "desktop",
            ): 1,
        }
    finally:
        cleanup_errors = []
        for query in queries:
            if query.isActive:
                try:
                    query.stop()
                except Exception as error:
                    cleanup_errors.append(f"stopping {query.name}: {error}")
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                cleanup_errors.append(f"stopping Spark: {error}")
        if topic_created:
            try:
                _kafka_cli("--delete", "--topic", topic)
            except Exception as error:
                cleanup_errors.append(f"deleting temporary topic {topic}: {error}")
        if cleanup_errors:
            raise RuntimeError("Integration cleanup failed: " + "; ".join(cleanup_errors))
