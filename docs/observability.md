# Local observability

PulseFlow's optional local monitoring stack is Prometheus and Grafana in
Docker Compose. The ingestion API is scraped over Compose DNS. Spark runs in
WSL Ubuntu, so its metrics listener is a host process and its address must be
reachable from the Prometheus container. Prometheus and Grafana host ports are
bound to loopback only; the Spark exporter has no Docker-published port.

## Configure and start

Prerequisites: Docker Desktop with WSL integration and Compose, Java 21,
Python 3.12, and the repository `.venv` with the `streaming` extra. Set a
random Grafana password in the terminal that will start Compose; do not use a
committed/default password:

```bash
export GRAFANA_ADMIN_PASSWORD="$(openssl rand -hex 24)"
docker compose -f infra/compose/docker-compose.yml \
  --profile observability up --build -d
```

Kafka data stays in the existing `kafka-data` volume. Prometheus time series
and Grafana state use separate `prometheus-data` and `grafana-data` named
volumes. Configuration and dashboards are bind-mounted read-only. No
`down -v` cleanup is needed or recommended.

Local URLs:

- Grafana: <http://localhost:3000> (username `admin`, password from
  `GRAFANA_ADMIN_PASSWORD`).
- Prometheus: <http://localhost:9090>.
- API health and Prometheus metrics:
  <http://localhost:8080/actuator/health> and
  <http://localhost:8080/actuator/prometheus>.

Grafana provisions the `PulseFlow Prometheus` datasource and `PulseFlow
Platform` dashboard automatically. To stop only monitoring:

```bash
docker compose -f infra/compose/docker-compose.yml \
  --profile observability stop grafana prometheus
```

Stop a Spark process with Ctrl+C. Stop the API and broker with the regular
Compose stop command if desired; this preserves named volumes:

```bash
docker compose -f infra/compose/docker-compose.yml stop
```

## WSL-hosted Spark metrics

The exporter is disabled by default. Install the optional streaming
dependencies once, then opt in from the WSL terminal. Its default bind address
is loopback:

```bash
source .venv/bin/activate
python -m pip install -e "./python[streaming,dev]"  # only if not already installed
export PULSEFLOW_METRICS_ENABLED=true
export PULSEFLOW_METRICS_PORT=9108
```

In the same WSL terminal, start the existing streaming application (the
connector coordinate must match Spark 4.2.0):

```bash
PYSPARK_DIR="$(python -c 'import pyspark; print(pyspark.__path__[0])')"
"$PYSPARK_DIR/bin/spark-submit" \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  python/pulseflow/streaming.py
```

The target is `host.docker.internal:9108`, with a `host-gateway` mapping.
This development environment was verified to route that address to a WSL
loopback listener; direct routing to WSL's `eth0` address timed out. WSL2 and
Docker Desktop routing varies by installation, so test the chosen route from
the Compose network. With Spark running, use a temporary curl container:

```bash
docker run --rm --network pulseflow_default curlimages/curl:8.12.1 \
  --fail http://host.docker.internal:9108/metrics
```

The exporter defaults to `127.0.0.1` and is disabled unless explicitly
enabled. Do not bind it to `0.0.0.0`, configure a public interface, add a
router/firewall forwarding rule, or expose port 9108 to the Internet. If
Docker cannot route to the loopback listener, keep the Spark target
unavailable rather than widening network access; API monitoring remains
usable.

## Generate activity and inspect health

Generate a small number of synthetic events through the existing API from a
separate WSL terminal:

```bash
source .venv/bin/activate
cd python
python -m pulseflow.generator \
  --url http://localhost:8080/v1/events \
  --events 10 --rps 2 --duplicate-rate 0
```

Check Prometheus target health and current alert state:

```bash
curl --fail http://localhost:9090/api/v1/targets
curl --fail http://localhost:9090/api/v1/rules
```

Open Grafana at <http://localhost:3000>, sign in with the generated local
password, and select **PulseFlow → PulseFlow Platform**. The six provisioned
panels are:

1. API request throughput, requests per second.
2. API p95 latency, milliseconds, calculated from the Spring request
   histogram.
3. HTTP 5xx responses as a percentage of observed HTTP responses; the result
   is undefined (no data) when no requests are recorded.
4. Silver input rows per second from its Structured Streaming progress.
5. Silver processed rows per second.
6. Silver microbatch trigger execution duration, milliseconds.

No synthetic zero samples are generated for missing Spark progress fields.
Low API traffic may leave request-rate and p95 series absent until enough
scrapes observe a request. Generate test traffic before judging these panels.

**Dashboard screenshot:** Add a screenshot here only after a human has run
this local setup and verified the displayed panels. No screenshot or
performance result is included yet.

## Metrics and alerts

Prometheus scrapes the API at `ingestion-api:8080/actuator/prometheus` and
Spark at the configured WSL host target. Spring's
`http_server_requests_seconds_*` histogram is enabled for real p95
calculations. Existing publishing counters are exported as
`pulseflow_events_published_total` and
`pulseflow_events_publish_failures_total`; no replacement counters are
registered.

The Spark exporter reports `pulseflow_stream_input_rows_per_second`,
`pulseflow_stream_processed_rows_per_second`, and
`pulseflow_stream_batch_duration_ms` from `pulseflow_silver`'s latest
Structured Streaming progress. Silver input rows/s measures the Silver query's
Bronze-Parquet source, not API request rate or Kafka broker throughput.

The provisioned rules are:

- `PulseFlowApiDown`: API scrape unavailable for 30 seconds.
- `PulseFlowStreamingStopped`: Spark metrics scrape unavailable for 60
  seconds. This intentionally fires when the optional local Spark application
  is stopped; it demonstrates an availability rule and is not evidence of a
  production incident.

Request rate, request latency, and event freshness are different measures.
Prometheus rate panels summarize observed metric changes; p95 is the
distribution of measured API request durations; neither shows how old an
event is by the time it reaches Silver/Gold. This dashboard does not claim an
end-to-end freshness SLI, Kafka throughput, or production performance.

## Troubleshooting

- **Grafana exits immediately:** ensure `GRAFANA_ADMIN_PASSWORD` is non-empty
  in the shell used by Compose. Check
  `docker compose --profile observability logs grafana`.
- **API target down:** check the API container and
  `curl http://localhost:8080/actuator/prometheus`; the API target uses Compose
  service DNS, not `localhost`.
- **Spark target down:** check the `spark-streaming` target in
  `infra/prometheus/prometheus.yml`,
  `curl http://127.0.0.1:9108/metrics` in WSL, then the container network
  curl command above. Confirm Spark started with
  `PULSEFLOW_METRICS_ENABLED=true` and is bound to loopback.
- **No Spark series:** send events, check the `pulseflow_silver` query logs
  and progress, and verify the configured target in Prometheus **Status →
  Targets**.
- **Datasource/dashboard missing:** inspect Grafana logs and confirm the
  provisioning YAML and dashboard JSON mounts are present and read-only.
- **Alert remains firing after shutdown:** expected for
  `PulseFlowStreamingStopped` when the optional exporter/application is not
  running.
