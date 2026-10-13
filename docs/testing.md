# PulseFlow testing guide

PulseFlow uses a testing pyramid: fast isolated tests run in the normal CI
workflow, while a separate, opt-in GitHub Actions job exercises the real
Kafka-to-Parquet pipeline. Java and Python/Spark runtime tests do not require a
live broker.

## Coverage layers

| Layer | What it verifies | Infrastructure |
| --- | --- | --- |
| Java unit/API tests | Controller validation and responses; publisher keys, acknowledgement, timeout, metrics, and errors | Spring test context; KafkaTemplate is mocked |
| Python unit tests | Generator behavior using HTTP mocks; canonical v1 schema checks; analytics query on temporary Parquet fixtures | No Kafka |
| Local Spark tests | Static DataFrame parsing/classification, quarantine reasons, static deduplication/Gold counts, and streaming query/sink construction | Spark local mode; no Kafka |
| Streaming integration | Real Kafka records flow through the running Structured Streaming application to Bronze, Silver, Gold, and Quarantine Parquet; verifies metadata, partitions, deduplication, late-event handling, and event-time window output | Docker Compose Kafka and Spark 4.2.0 Kafka connector |
| Compose smoke test | Java API accepts an event and Kafka exposes it with `user_id` as the record key | Compose Kafka and ingestion API |
| kind smoke test | Kubernetes Kafka and API deployment, health, HTTP acceptance, and Kafka key | Disposable kind cluster |

The fast suite currently has 25 Java tests and 60 Python tests. The separate
live streaming integration is one additional test and is not collected by the
fast `python/tests` command.

The Java `EventPublishingIntegrationTests` load the Spring application but
mock `KafkaTemplate`; they are not a live Kafka test. The local Spark tests
use static DataFrames or Spark's rate source; only the explicit streaming
integration test starts the actual Kafka source and Parquet pipeline.

The schema uses `minLength: 1`, so a single-space `user_id` passes JSON Schema
validation. The Java API's `@NotBlank` and Spark's trimmed blank check reject
it. This existing stricter runtime behavior is characterized by tests; neither
the v1 schema nor runtime rules are changed here. JSON Schema `format` checks
also depend on the validator implementation, and should not be treated as a
replacement for the API's typed timestamp binding.

## Fast tests

Run from the repository root:

```bash
mvn --batch-mode --file services/ingestion-api/pom.xml clean verify
source .venv/bin/activate
ruff check python/
python -m pytest -q python/tests
```

The Python environment needs Python 3.12, Java 21 for Spark, and the project
`dev` and `streaming` extras. Install them if needed:

```bash
python -m pip install -e "./python[streaming,dev]"
```

These are also the Java and Python jobs in `.github/workflows/ci.yml`. The CI
Docker job builds the ingestion image but does not publish it or deploy it.

## Real Kafka-to-Parquet integration

Prerequisites: Docker with Compose, Java 21, Python 3.12, the `dev` and
`streaming` extras, and access to Maven Central for the Spark 4.2.0 Kafka
connector. From the repository root, start only the existing Compose broker
and run the test:

```bash
docker compose -f infra/compose/docker-compose.yml up -d broker
source .venv/bin/activate
python -m pytest -q -s python/integration_tests/test_streaming_pipeline.py
```

The integration creates a uniquely named, one-partition Kafka topic and a
pytest-owned temporary data root. It publishes a valid event, its duplicate,
malformed JSON, event-time advancement records, and a late event. It verifies
raw Kafka fields in Bronze, matching event-date partitions and deduplicated
events in Silver, the quarantine reason, the actual watermark dropping the
late event, and closed Gold windows. It polls Kafka readiness and pipeline
progress with explicit timeouts. The test stops only its Spark queries,
removes only its uniquely named topic, and lets pytest remove its own
temporary directory. It does not stop the Compose broker, clear a Kafka
volume, or read/write repository `data/` or existing checkpoint directories.

The corresponding separate GitHub Actions workflow is
`.github/workflows/streaming-integration.yml`. It runs manually or on relevant
pull-request changes, starts a fresh runner-local Compose broker, runs the
same integration, prints broker logs on failure, and stops that broker
without removing its named volume. It has a bounded 45-minute job timeout and
read-only repository permissions. Its runner is disposable; it does not
deploy to a persistent environment.

## Expected outputs and troubleshooting

The fast Python suite reports passing test counts; the integration test
reports a pass only after all four Parquet layers and the event-time checks
have been observed. For an integration failure:

1. Confirm the broker is healthy with
   `docker compose -f infra/compose/docker-compose.yml ps`.
2. Inspect broker startup with
   `docker compose -f infra/compose/docker-compose.yml logs broker`.
3. Check that host Spark uses `localhost:9092`; inside Compose the broker
   address is `broker:19092`.
4. Confirm Java 21 and the PySpark version with
   `java -version` and `python -c 'import pyspark; print(pyspark.__version__)'`.
5. Check the error for Spark connector download failures; integration requires
   `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`.

The integration timeout and polling failures include Structured Streaming
query status/progress. Do not delete the Compose `kafka-data` volume or normal
Spark checkpoints as a troubleshooting shortcut.

## Delivery limitations

Kafka producer idempotence does not make the full platform exactly-once.
Kafka→Spark processing and Parquet sinks use checkpoints and stateful
watermark-bounded deduplication, not a guarantee against every duplicate or
replay across all failures. A duplicate `event_id` outside the retained
watermark horizon may be counted again. Events are synthetic portfolio data,
not real user activity.
