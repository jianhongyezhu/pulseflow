# PulseFlow

PulseFlow is a local-first data-platform portfolio project: a Spring Boot API
publishes synthetic user-behaviour events to Kafka, PySpark Structured
Streaming writes Bronze, Silver, Gold, and Quarantine Parquet datasets, and
DuckDB queries Silver data.

## Local services

Start the existing Kafka broker and ingestion API from the repository root:

```bash
docker compose -f infra/compose/docker-compose.yml up --build -d
```

The API is available at `http://localhost:8080`; Kafka's host listener is at
`localhost:9092`. Python, Spark, and analytics setup is documented in
[the ingestion service guide](services/ingestion-api/README.md) and
[the Python guide](python/README.md).

## Observability

Prometheus and Grafana are optional local Compose services. Follow
[the observability guide](docs/observability.md) to configure credentials,
start monitoring, run the WSL-hosted Spark exporter, and view the provisioned
six-panel dashboard.

## Testing

The fast test pyramid, real Kafka-to-Parquet integration command, prerequisites,
and known delivery limitations are documented in [docs/testing.md](docs/testing.md).

This demonstration uses synthetic events and local services. Publishing or
observing metrics does not deploy the platform to production or imply an
end-to-end exactly-once delivery guarantee.
