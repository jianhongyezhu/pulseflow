"""Testable Spark transformations for PulseFlow's v1 Kafka event stream."""

from __future__ import annotations

import logging
import math
import os
import signal
from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path
from threading import Event, Lock
from typing import Mapping

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import GaugeMetricFamily
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BinaryType,
    DateType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WATERMARK_DELAY = "30 seconds"
GOLD_WINDOW_DURATION = "10 seconds"

EVENT_TYPES = (
    "PLAY",
    "PAUSE",
    "SKIP",
    "SEARCH",
    "LIKE",
    "ADD_TO_PLAYLIST",
    "PAGE_VIEW",
)
DEVICE_TYPES = ("mobile", "desktop", "tablet", "tv")
EVENT_FIELDS = (
    "event_id",
    "user_id",
    "session_id",
    "event_type",
    "content_id",
    "device_type",
    "country_code",
    "event_ts",
    "attributes",
)
REQUIRED_FIELDS = EVENT_FIELDS[:-1]
UUID_PATTERN = r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

USER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("content_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
    ]
)
_PARSING_SCHEMA = StructType(
    [*USER_EVENT_SCHEMA.fields, StructField("_corrupt_record", StringType(), True)]
)
_BRONZE_FIELDS = (
    "kafka_key",
    "kafka_value",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "raw_json",
    "ingestion_timestamp",
)
BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", BinaryType(), True),
        StructField("kafka_value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("raw_json", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("kafka_date", DateType(), True),
    ]
)
SILVER_SCHEMA = StructType(
    [
        StructField("kafka_key", BinaryType(), True),
        StructField("kafka_value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("raw_json", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        *USER_EVENT_SCHEMA.fields,
        StructField("event_time", TimestampType(), True),
        StructField("event_date", DateType(), True),
    ]
)
SILVER_FIELDS = tuple(field.name for field in SILVER_SCHEMA.fields)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    bootstrap_servers: str
    topic: str
    data_root: Path
    metrics_enabled: bool = False
    metrics_host: str = "127.0.0.1"
    metrics_port: int = 9108

    @classmethod
    def from_environment(cls) -> PipelineConfig:
        metrics_enabled_value = os.environ.get("PULSEFLOW_METRICS_ENABLED", "false").lower()
        if metrics_enabled_value not in {"true", "false"}:
            raise ValueError("PULSEFLOW_METRICS_ENABLED must be either true or false")
        metrics_port = int(os.environ.get("PULSEFLOW_METRICS_PORT", "9108"))
        if not 1 <= metrics_port <= 65535:
            raise ValueError("PULSEFLOW_METRICS_PORT must be between 1 and 65535")
        metrics_host = os.environ.get("PULSEFLOW_METRICS_HOST", "127.0.0.1").strip()
        if not metrics_host:
            raise ValueError("PULSEFLOW_METRICS_HOST must not be empty")
        if metrics_host != "localhost":
            try:
                host_address = ip_address(metrics_host)
            except ValueError as exc:
                raise ValueError(
                    "PULSEFLOW_METRICS_HOST must be localhost or a private IP address"
                ) from exc
            if host_address.is_unspecified:
                raise ValueError("PULSEFLOW_METRICS_HOST must not be a wildcard address")
            if not (
                host_address.is_loopback
                or host_address.is_private
                or host_address.is_link_local
            ):
                raise ValueError(
                    "PULSEFLOW_METRICS_HOST must be localhost or a private IP address"
                )
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.environ.get("PULSEFLOW_TOPIC", "user-behaviour.v1"),
            data_root=Path(os.environ.get("PULSEFLOW_DATA_ROOT", "data")).expanduser().resolve(),
            metrics_enabled=metrics_enabled_value == "true",
            metrics_host=metrics_host,
            metrics_port=metrics_port,
        )

    def output_path(self, layer: str, name: str) -> Path:
        return self.data_root / layer / name

    def checkpoint_path(self, name: str) -> Path:
        return self.data_root / "checkpoints" / name


class StreamingMetricsCollector:
    """Expose only observed Silver progress fields; missing measurements stay absent."""

    _METRICS = {
        "pulseflow_stream_input_rows_per_second": (
            "Rows per second entering the Silver streaming query.",
        ),
        "pulseflow_stream_processed_rows_per_second": (
            "Rows per second processed by the Silver streaming query.",
        ),
        "pulseflow_stream_batch_duration_ms": (
            "Silver streaming microbatch trigger execution duration in milliseconds.",
        ),
    }

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self._values: dict[str, float] = {}
        self._lock = Lock()
        self.registry.register(self)

    def describe(self):
        for name, (description,) in self._METRICS.items():
            yield GaugeMetricFamily(name, description)

    def collect(self):
        for name, value in self._snapshot().items():
            description = self._METRICS[name][0]
            metric = GaugeMetricFamily(name, description)
            metric.add_metric([], value)
            yield metric

    def update(self, progress: Mapping | None) -> None:
        if not isinstance(progress, Mapping):
            return

        updates: dict[str, float] = {}
        sources = progress.get("sources")
        if isinstance(sources, list):
            rates = [
                self._finite_number(source.get("inputRowsPerSecond"))
                for source in sources
                if isinstance(source, Mapping)
            ]
            available_rates = [rate for rate in rates if rate is not None]
            if available_rates:
                updates["pulseflow_stream_input_rows_per_second"] = sum(available_rates)

        processed_rate = self._finite_number(progress.get("processedRowsPerSecond"))
        if processed_rate is not None:
            updates["pulseflow_stream_processed_rows_per_second"] = processed_rate

        durations = progress.get("durationMs")
        if isinstance(durations, Mapping):
            batch_duration = self._finite_number(durations.get("triggerExecution"))
            if batch_duration is not None:
                updates["pulseflow_stream_batch_duration_ms"] = batch_duration

        if updates:
            with self._lock:
                self._values.update(updates)

    @staticmethod
    def _finite_number(value) -> float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        number = float(value)
        return number if math.isfinite(number) and number >= 0 else None

    def _snapshot(self) -> dict[str, float]:
        with self._lock:
            return self._values.copy()


def start_stream_metrics_exporter(host: str, port: int):
    """Start one process-local metrics server with an isolated registry."""
    collector = StreamingMetricsCollector()
    server, thread = start_http_server(port, addr=host, registry=collector.registry)
    logger.info("Started Spark metrics exporter on %s:%s", host, port)
    return collector, server, thread


def stop_stream_metrics_exporter(server, thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def configure_spark_session(spark: SparkSession) -> None:
    """Use UTC for JSON timestamp parsing, event-time windows, and ingestion timestamps."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")


def bronze_from_kafka(kafka_df: DataFrame) -> DataFrame:
    """Preserve Kafka's original key/value and metadata while adding raw JSON and ingest time."""
    configure_spark_session(kafka_df.sparkSession)
    return kafka_df.select(
        F.col("key").alias("kafka_key"),
        F.col("value").alias("kafka_value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_json"),
        F.current_timestamp().alias("ingestion_timestamp"),
    )


def parse_events(bronze_df: DataFrame) -> DataFrame:
    """Parse v1 event fields and retain parse diagnostics and unknown top-level JSON keys."""
    parsed = bronze_df.withColumn(
        "_parsed_event",
        F.from_json(
            F.col("raw_json"),
            _PARSING_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    ).withColumn("_json_keys", F.json_object_keys(F.col("raw_json")))

    return parsed.select(
        *[F.col(name) for name in _BRONZE_FIELDS],
        *[F.col(f"_parsed_event.{name}").alias(name) for name in EVENT_FIELDS],
        F.col("_parsed_event._corrupt_record").alias("json_parse_error"),
        (
            F.col("_parsed_event._corrupt_record").isNotNull()
            | F.col("_parsed_event").isNull()
        ).alias("json_parse_failed"),
        F.array_except(
            F.col("_json_keys"),
            F.array(*[F.lit(name) for name in EVENT_FIELDS]),
        ).alias("unknown_fields"),
        F.expr("try_to_timestamp(event_ts)").alias("event_time"),
    )


def classify_events(parsed_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split valid records from quarantine using native Spark SQL quality checks.

    Kafka-key/user_id mismatches are quarantined because the ingestion API uses user_id
    as the Kafka key.
    """
    parse_failed = F.col("json_parse_error").isNotNull() | F.col("json_parse_failed")
    errors = [
        *[
            F.when(F.col(field).isNull(), F.lit(f"missing_{field}"))
            for field in REQUIRED_FIELDS
        ],
        F.when(F.length(F.trim(F.col("user_id"))) == 0, F.lit("blank_user_id")),
        F.when(F.length(F.trim(F.col("session_id"))) == 0, F.lit("blank_session_id")),
        F.when(F.length(F.trim(F.col("content_id"))) == 0, F.lit("blank_content_id")),
        F.when(
            F.col("event_id").isNotNull() & ~F.col("event_id").rlike(UUID_PATTERN),
            F.lit("invalid_uuid"),
        ),
        F.when(
            F.col("event_type").isNotNull() & ~F.col("event_type").isin(*EVENT_TYPES),
            F.lit("invalid_event_type"),
        ),
        F.when(
            F.col("device_type").isNotNull() & ~F.col("device_type").isin(*DEVICE_TYPES),
            F.lit("invalid_device_type"),
        ),
        F.when(
            F.col("country_code").isNotNull()
            & ~F.col("country_code").rlike(r"^[A-Z]{2}$"),
            F.lit("invalid_country_code"),
        ),
        F.when(
            F.col("event_ts").isNotNull() & F.col("event_time").isNull(),
            F.lit("invalid_event_ts"),
        ),
        F.when(
            F.size(F.coalesce(F.col("unknown_fields"), F.array().cast("array<string>"))) > 0,
            F.lit("unknown_fields"),
        ),
        F.when(
            F.array_contains(F.json_object_keys(F.col("raw_json")), "attributes")
            & (
                F.col("attributes").isNull()
                | F.exists(F.map_values("attributes"), lambda value: value.isNull())
            ),
            F.lit("invalid_attributes"),
        ),
        F.when(
            F.col("kafka_key").isNull()
            | (F.col("kafka_key").cast("string") != F.col("user_id")),
            F.lit("kafka_key_user_id_mismatch"),
        ),
    ]

    checked = parsed_df.withColumn(
        "quality_errors",
        F.when(
            parse_failed,
            F.array(F.lit("malformed_json")),
        ).otherwise(F.filter(F.array(*errors), lambda error: error.isNotNull())),
    )
    invalid = checked.filter(F.size("quality_errors") > 0)
    valid = checked.filter(F.size("quality_errors") == 0).drop("quality_errors")
    return valid, invalid


def deduplicate_events(
    valid_events_df: DataFrame,
    watermark_delay: str = WATERMARK_DELAY,
) -> DataFrame:
    """Deduplicate on event_id; streams retain state only within the watermark horizon."""
    if valid_events_df.isStreaming:
        watermarked = valid_events_df.withWatermark("event_time", watermark_delay)
        return watermarked.dropDuplicatesWithinWatermark(["event_id"])
    return valid_events_df.dropDuplicates(["event_id"])


def build_gold_counts(
    valid_events_df: DataFrame,
    watermark_delay: str = WATERMARK_DELAY,
) -> DataFrame:
    """Count unique events per 10-second event-time window, event type, and device.

    For streaming input this chains watermark-bounded deduplication and aggregation.
    Start such a query in Append output mode; other modes can reject this stateful chain.
    """
    return aggregate_deduplicated_events(
        deduplicate_events(valid_events_df, watermark_delay),
        watermark_delay,
    )


def aggregate_deduplicated_events(
    deduplicated_events_df: DataFrame,
    watermark_delay: str = WATERMARK_DELAY,
) -> DataFrame:
    """Aggregate already-deduplicated records without adding another deduplication state."""
    watermarked = deduplicated_events_df
    if watermarked.isStreaming:
        watermarked = watermarked.withWatermark("event_time", watermark_delay)
    return watermarked.groupBy(
        F.window(F.col("event_time"), GOLD_WINDOW_DURATION),
        F.col("event_type"),
        F.col("device_type"),
    ).agg(F.count(F.lit(1)).alias("event_count"))


def create_spark_session() -> SparkSession:
    """Create the application session; spark-submit selects the local master."""
    return (
        SparkSession.builder.appName("PulseFlow Structured Streaming")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _write_parquet(
    dataframe: DataFrame,
    name: str,
    path: Path,
    checkpoint: Path,
    partition_by: str | None = None,
) -> StreamingQuery:
    writer = (
        dataframe.writeStream.queryName(name)
        .outputMode("append")
        .format("parquet")
        .option("path", str(path))
        .option("checkpointLocation", str(checkpoint))
    )
    if partition_by is not None:
        writer = writer.partitionBy(partition_by)
    query = writer.start()
    logger.info("Started %s query: %s (checkpoint: %s)", name, path, checkpoint)
    return query


def _stop_queries(queries: list[StreamingQuery]) -> None:
    for query in queries:
        if query.isActive:
            try:
                query.stop()
            except Exception:
                logger.exception("Failed to stop streaming query %s", query.name)


def start_pipeline(spark: SparkSession, config: PipelineConfig) -> list[StreamingQuery]:
    """Start the four append-mode queries in Bronze-to-Gold dependency order."""
    configure_spark_session(spark)
    paths = {
        "bronze": config.output_path("bronze", "user_behaviour"),
        "silver": config.output_path("silver", "user_behaviour"),
        "gold": config.output_path("gold", "event_counts"),
        "quarantine": config.output_path("quarantine", "user_behaviour"),
    }
    checkpoints = {
        name: config.checkpoint_path(name)
        for name in ("bronze", "silver", "gold", "quarantine")
    }
    for path in (*paths.values(), *checkpoints.values()):
        path.mkdir(parents=True, exist_ok=True)

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    bronze = bronze_from_kafka(kafka_stream).withColumn(
        "kafka_date", F.to_date("kafka_timestamp")
    )
    queries: list[StreamingQuery] = []
    try:
        queries.append(
            _write_parquet(
                bronze,
                "pulseflow_bronze",
                paths["bronze"],
                checkpoints["bronze"],
                "kafka_date",
            )
        )

        bronze_files = (
            spark.readStream.schema(BRONZE_SCHEMA)
            .format("parquet")
            .load(str(paths["bronze"]))
        )
        parsed = parse_events(bronze_files)
        valid, quarantine = classify_events(parsed)
        silver = (
            deduplicate_events(valid)
            .withColumn("event_date", F.to_date("event_time"))
            .select(*SILVER_FIELDS)
        )
        queries.append(
            _write_parquet(
                silver,
                "pulseflow_silver",
                paths["silver"],
                checkpoints["silver"],
                "event_date",
            )
        )
        queries.append(
            _write_parquet(
                quarantine,
                "pulseflow_quarantine",
                paths["quarantine"],
                checkpoints["quarantine"],
            )
        )

        silver_files = (
            spark.readStream.schema(SILVER_SCHEMA)
            .format("parquet")
            .load(str(paths["silver"]))
        )
        gold = aggregate_deduplicated_events(silver_files)
        queries.append(
            _write_parquet(
                gold,
                "pulseflow_gold",
                paths["gold"],
                checkpoints["gold"],
            )
        )
        return queries
    except Exception:
        _stop_queries(queries)
        raise


def _monitor_queries(
    queries: list[StreamingQuery],
    stopping: Event,
    metrics_collector: StreamingMetricsCollector | None = None,
) -> None:
    """Wait until shutdown and surface any query that terminates with a failure."""
    while not stopping.is_set():
        for query in queries:
            if not query.isActive:
                failure = query.exception()
                if failure is not None:
                    logger.error("Streaming query %s failed: %s", query.name, failure)
                    raise RuntimeError(f"Streaming query {query.name} failed") from failure
                logger.error("Streaming query %s stopped unexpectedly", query.name)
                raise RuntimeError(f"Streaming query {query.name} stopped unexpectedly")
            if metrics_collector is not None and query.name == "pulseflow_silver":
                metrics_collector.update(query.lastProgress)
        stopping.wait(1)


def run_pipeline(config: PipelineConfig | None = None) -> int:
    """Run and monitor the pipeline until interrupted or a query fails."""
    pipeline_config = config or PipelineConfig.from_environment()
    spark: SparkSession | None = None
    queries: list[StreamingQuery] = []
    metrics_server = None
    metrics_thread = None
    stopping = Event()
    original_sigterm_handler = signal.getsignal(signal.SIGTERM)
    try:
        signal.signal(signal.SIGTERM, lambda _signum, _frame: stopping.set())
        spark = create_spark_session()
        queries = start_pipeline(spark, pipeline_config)
        metrics_collector = None
        if pipeline_config.metrics_enabled:
            metrics_collector, metrics_server, metrics_thread = start_stream_metrics_exporter(
                pipeline_config.metrics_host,
                pipeline_config.metrics_port,
            )
        _monitor_queries(queries, stopping, metrics_collector)
        return 0
    except KeyboardInterrupt:
        logger.info("Shutdown requested; stopping PulseFlow streaming queries")
        return 0
    except Exception:
        logger.exception("PulseFlow streaming pipeline failed")
        return 1
    finally:
        signal.signal(signal.SIGTERM, original_sigterm_handler)
        _stop_queries(queries)
        if metrics_server is not None and metrics_thread is not None:
            try:
                stop_stream_metrics_exporter(metrics_server, metrics_thread)
            except Exception:
                logger.exception("Failed to stop Spark metrics exporter")
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                logger.exception("Failed to stop Spark session")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = PipelineConfig.from_environment()
    logger.info(
        "Starting PulseFlow: broker=%s topic=%s data_root=%s",
        config.bootstrap_servers,
        config.topic,
        config.data_root,
    )
    return run_pipeline(config)


if __name__ == "__main__":
    raise SystemExit(main())
