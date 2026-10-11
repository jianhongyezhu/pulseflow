# PulseFlow Python services

## Structured Streaming

From the repository root, install the streaming extra into the Python environment
used by Spark:

```bash
cd python
python -m pip install -e '.[streaming]'
cd ..
```

From the repository root, start the broker and then run the streaming application
with Spark 4.2.0 and its Kafka connector:

```bash
docker compose -f infra/compose/docker-compose.yml up -d broker
scripts/create-topics.sh
PYSPARK_DIR="$(python -c 'import pyspark; print(pyspark.__path__[0])')"
"$PYSPARK_DIR/bin/spark-submit" \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  python/pulseflow/streaming.py
```

Run these commands with the same Python environment activated that received the
streaming extra. The Kafka topic script creates the default topic; if
`PULSEFLOW_TOPIC` is overridden, create that topic separately. The Kafka
connector is downloaded by Spark on the first run. In WSL2, use the default
`localhost:9092` broker address when running Spark on the host. If Spark itself
runs in a Compose container, set `KAFKA_BOOTSTRAP_SERVERS=broker:19092`.

The application defaults to topic `user-behaviour.v1` and data root `data`.
Override them with `PULSEFLOW_TOPIC`, `KAFKA_BOOTSTRAP_SERVERS`, and
`PULSEFLOW_DATA_ROOT`.

The application writes Bronze Kafka records, deduplicated Silver events,
Quarantine records and Gold event counts using Parquet append sinks. Gold reads
from Silver's Parquet output, rather than raw Kafka or Bronze, so it aggregates
events only after the Silver event-ID deduplication stage. Silver and Gold use
30-second event-time watermarks; Gold groups into 10-second windows. Watermark
deduplication is bounded: duplicate IDs outside the retained watermark state
are not guaranteed to be removed.

Press Ctrl+C to stop cleanly. The application stops every active query before
stopping Spark. Restart with the same data root and unchanged checkpoint
directories to recover Kafka offsets and streaming state. Kafka's
`startingOffsets=earliest` applies when the Bronze checkpoint is first created;
on restart Spark resumes from the checkpoint. Do not remove or move
`data/checkpoints/` to reset or relocate the pipeline. The Parquet outputs are
append-only and are not cleared on shutdown.

The four checkpoints are:

- `data/checkpoints/bronze`
- `data/checkpoints/silver`
- `data/checkpoints/quarantine`
- `data/checkpoints/gold`

Corresponding output directories are under `data/bronze/user_behaviour`,
`data/silver/user_behaviour`, `data/quarantine/user_behaviour`, and
`data/gold/event_counts`.

## DuckDB Analytics

PulseFlow uses DuckDB to query Silver Parquet files directly on your machine; no
separate database server is needed. First generate synthetic events with the
event generator, run them through the Java ingestion API and Kafka, and let the
Structured Streaming pipeline write Silver data. Silver contains validated
events deduplicated by `event_id` within the configured event-time watermark.

From the repository root, with the Python package installed, run:

```bash
python -m pulseflow.analytics
```

The event summary reads `data/silver/user_behaviour/` and groups by event type
and device type. It reports event counts, distinct user IDs, and the earliest
and latest event timestamps. These are synthetic behavioural events and do not
represent real users. Watermark-based deduplication has a limited time horizon;
duplicate IDs arriving outside the retained state may be counted again.

Run the analytics tests without Kafka, Spark, Docker, or existing Silver data:

```bash
cd python
python -m pytest tests/test_analytics.py
```
