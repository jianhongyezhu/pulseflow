# Performance and recovery runbook

These experiments are local engineering checks, not production-scale claims.
Keep benchmark/recovery traffic on an isolated Compose project and use only
the dedicated `pulseflow-recovery` kind cluster for the Kubernetes experiment.
Do not stop the normal Compose project, remove Kafka volumes, or delete
developer Parquet data/checkpoints.

## Isolated Compose benchmark

Prerequisites: Docker Compose, Java 21, the project Python `streaming` extra,
and the pinned `grafana/k6:0.55.0` image. Start from the repository root in
one shell:

```bash
RUN_SUFFIX="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
export RUN_ID="perf-${RUN_SUFFIX}"
export COMPOSE_PROJECT_NAME="pulseflow-perf-${RUN_SUFFIX}"
export PULSEFLOW_TOPIC="pulseflow-perf-${RUN_SUFFIX}"
export PULSEFLOW_API_HOST_PORT=18080
export PULSEFLOW_KAFKA_HOST_PORT=19092
export PULSEFLOW_ARTIFACTS_DIR="$PWD/artifacts/performance/$RUN_ID"
mkdir -p "$PULSEFLOW_ARTIFACTS_DIR"

docker compose --project-name "$COMPOSE_PROJECT_NAME" \
  -f infra/compose/docker-compose.yml up --build -d broker ingestion-api
PULSEFLOW_TOPIC="$PULSEFLOW_TOPIC" scripts/create-topics.sh
```

The unique Compose project creates a separate network and `kafka-data` named
volume. Its API and Kafka host ports default to the normal values for ordinary
Compose users, but are overridden here to loopback ports 18080 and 19092.
Inside the k6 container, use `http://ingestion-api:8080`, not `localhost`.
Spark runs on the host and connects to Kafka using
`KAFKA_BOOTSTRAP_SERVERS=localhost:19092`; the broker advertises that host
listener on the configured port. Kafka topic creation uses `PULSEFLOW_TOPIC`
and defaults to `user-behaviour.v1` when unset.
Run one k6 profile per generated run ID/project so accepted-request counts and
Silver rows cannot include events from a previous profile.

Wait for the isolated broker and API before starting load:

```bash
deadline=$((SECONDS + 120))
until [[ "$(docker inspect --format '{{.State.Health.Status}}' \
  "$(docker compose --project-name "$COMPOSE_PROJECT_NAME" \
    -f infra/compose/docker-compose.yml ps -q broker)")" == healthy ]] \
  && curl --fail --silent http://127.0.0.1:18080/actuator/health >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "Isolated broker/API readiness timed out" >&2
    exit 1
  fi
  sleep 2
done
```

### Profiles and exact benchmark commands

Start the Spark command in the next section from a second terminal before
starting k6 when measuring end-to-end freshness. Copy `RUN_ID`,
`PULSEFLOW_TOPIC`, and `PULSEFLOW_ARTIFACTS_DIR` from the first terminal into
that Spark terminal; keep both terminals open for the benchmark and bounded
processing wait.

The handbook ramp uses **virtual users (VUs)**, not a fixed request rate. It
ramps 0→20 VUs in 30 seconds, 20→50 in 60 seconds, 50→100 in 60 seconds, then
100→0 in 30 seconds. Run it with:

```bash
K6_PROFILE=handbook RUN_ID="$RUN_ID" \
K6_BASE_URL=http://ingestion-api:8080 \
docker compose --project-name "$COMPOSE_PROJECT_NAME" \
  --profile loadtest -f infra/compose/docker-compose.yml run --rm k6
```

The handbook's **50 requests/second** latency target uses a constant-arrival
rate executor, independently of VU count:

```bash
K6_PROFILE=arrival K6_RATE=50 K6_PROFILE_DURATION=60s \
K6_PRE_ALLOCATED_VUS=50 K6_MAX_VUS=150 \
RUN_ID="$RUN_ID" K6_BASE_URL=http://ingestion-api:8080 \
docker compose --project-name "$COMPOSE_PROJECT_NAME" \
  --profile loadtest -f infra/compose/docker-compose.yml run --rm k6
```

Only the default `arrival` profile at exactly 50 requests/second enforces the
p95 `<150 ms` threshold. Other arrival rates, the handbook VU ramp, and the
opt-in stretch profile do not enforce that latency target. All profiles
enforce HTTP request failures `<1%`, a 202-response check success rate `>99%`,
and zero dropped iterations; the check threshold allows the same sub-1%
failure budget as the HTTP error threshold. The k6 process exits nonzero if a
configured threshold fails or iterations are dropped. Its JSON summary is
printed and written to
`$PULSEFLOW_ARTIFACTS_DIR/k6-summary.json`.

The 250 requests/second for 30 seconds stretch is opt-in and does not enforce
the 150 ms p95 target:

```bash
K6_PROFILE=stretch K6_RATE=250 K6_PROFILE_DURATION=30s \
K6_PRE_ALLOCATED_VUS=250 K6_MAX_VUS=500 \
RUN_ID="$RUN_ID" K6_BASE_URL=http://ingestion-api:8080 \
docker compose --project-name "$COMPOSE_PROJECT_NAME" \
  --profile loadtest -f infra/compose/docker-compose.yml run --rm k6
```

Rates are configurable up to 1000 requests/second and VU limits are capped at
500 to avoid unbounded local allocation. Do not increase them without
understanding host capacity. Each valid event gets a new UUID, a current UTC
timestamp, `attributes.source=k6-load-test`, and this run's `run_id`.

### Run isolated Spark and analyze event freshness

In the second terminal, activate the project environment, copy the run values
from the first terminal, select a unique data/checkpoint root outside the
repository, and use the dedicated broker's host listener. The exporter binds
to loopback and is optional; this example uses a separate port to avoid
colliding with other local Spark processes:

```bash
source .venv/bin/activate
export RUN_ID='perf-<copy-the-suffix>'
export PULSEFLOW_TOPIC='pulseflow-perf-<same-suffix>'
export PULSEFLOW_ARTIFACTS_DIR="$PWD/artifacts/performance/$RUN_ID"
export PULSEFLOW_DATA_ROOT="$(mktemp -d /tmp/pulseflow-perf-data.XXXXXX)"
export KAFKA_BOOTSTRAP_SERVERS=localhost:19092
export PULSEFLOW_TOPIC
export PULSEFLOW_METRICS_ENABLED=true
export PULSEFLOW_METRICS_HOST=127.0.0.1
export PULSEFLOW_METRICS_PORT=19108

PYSPARK_DIR="$(python -c 'import pyspark; print(pyspark.__path__[0])')"
"$PYSPARK_DIR/bin/spark-submit" --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  python/pulseflow/streaming.py
```

Keep Spark running for the selected k6 profile and the bounded processing
wait afterward. Copy the accepted 202 count from the k6 JSON summary and
analyze only Silver events carrying this run ID:

```bash
ACCEPTED_REQUESTS="$(python -c \
  'import json,os; print(json.load(open(os.path.join(os.environ["PULSEFLOW_ARTIFACTS_DIR"], "k6-summary.json")))["accepted_events"])')"
python -m pulseflow.benchmark_analysis \
  --silver-dir "$PULSEFLOW_DATA_ROOT/silver/user_behaviour" \
  --run-id "$RUN_ID" \
  --accepted-requests "$ACCEPTED_REQUESTS" \
  --wait-seconds 60 --poll-seconds 2 \
  | tee "$PULSEFLOW_ARTIFACTS_DIR/freshness.json"
```

The analysis reports Silver count, accepted-minus-observed discrepancy, count
of missing/invalid processing timestamps, and p50/p95
`processed_at - event_time` in seconds. Percentiles remain null when there
are no valid observations; no zero freshness is fabricated. `processed_at` is
derived Silver processing metadata and does not alter the v1 event schema.

For actual Spark progress, capture the exporter while Spark runs:

```bash
curl --fail http://127.0.0.1:19108/metrics \
  | grep '^pulseflow_stream_'
```

These Silver-query input/processed rows-per-second and microbatch duration
metrics are not API throughput, Kafka broker throughput, or consumer-group
lag. API request throughput is measured separately by k6.

### Kafka broker failure and recovery

Run this only for the isolated project from the same shell that exported
`COMPOSE_PROJECT_NAME`, `PULSEFLOW_API_HOST_PORT`, `PULSEFLOW_TOPIC`, and
`RUN_ID` above. The script refuses to proceed without explicit opt-in and
matching Compose project/service labels. It requires the isolated topic and
healthy API/broker, stops only that project's broker, expects HTTP 503, starts
the same broker, and retries with new event IDs for at most 60 seconds:

```bash
export PULSEFLOW_ALLOW_FAILURE_TESTS=YES
scripts/compose-broker-recovery.sh
```

The output and `compose-recovery-$RUN_ID.json` record the outage status,
recovery status/time, and result. On early failure, the script attempts to
restart the broker. It never removes a container, volume, topic, or dataset.
Do not run this against the normal Compose project or the default topic.
The outage request has a 70-second client bound because Kafka's default
`max.block.ms` is 60 seconds; `KafkaTemplate.send()` may wait for metadata
before the publisher's separate three-second acknowledgement wait begins.

Afterward, stop only the isolated project without removing its Kafka volume:

```bash
docker compose --project-name "$COMPOSE_PROJECT_NAME" \
  -f infra/compose/docker-compose.yml stop
```

Do not use `docker compose down -v`. The isolated Kafka volume is retained;
the temporary Spark output/checkpoints are under the printed `/tmp` path.

## Dedicated kind API pod recovery

This experiment is opt-in and uses only the disposable cluster named
`pulseflow-recovery`. It fails rather than reusing a cluster with that name.
Every kubectl command is explicitly scoped to `kind-pulseflow-recovery`.
Do not substitute the current context or a shared cluster.

First verify kind is installed and the dedicated cluster does not already
exist. Build/load the existing local development image, then deploy the
existing manifests:

```bash
if kind get clusters | grep -Fxq pulseflow-recovery; then
  echo "Refusing to reuse existing kind cluster pulseflow-recovery" >&2
  exit 1
fi
kind create cluster --name pulseflow-recovery \
  --config infra/kind/kind-config.yaml --wait 5m
kubectl --context kind-pulseflow-recovery get nodes --no-headers

docker build -f services/ingestion-api/Dockerfile \
  -t pulseflow-ingestion:dev services/ingestion-api
kind load docker-image pulseflow-ingestion:dev --name pulseflow-recovery
kubectl --context kind-pulseflow-recovery apply -f infra/kubernetes/namespace.yaml
kubectl --context kind-pulseflow-recovery apply -f infra/kubernetes/kafka.yaml
kubectl --context kind-pulseflow-recovery -n pulseflow \
  rollout status deployment/kafka --timeout=5m
kubectl --context kind-pulseflow-recovery -n pulseflow exec deployment/kafka -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic user-behaviour.v1 \
  --partitions 6 --replication-factor 1
kubectl --context kind-pulseflow-recovery apply \
  -f infra/kubernetes/ingestion-api.yaml
kubectl --context kind-pulseflow-recovery -n pulseflow \
  rollout status deployment/ingestion-api --timeout=5m
kubectl --context kind-pulseflow-recovery -n pulseflow \
  wait --for=condition=Available deployment/ingestion-api --timeout=60s
test "$(kubectl --context kind-pulseflow-recovery -n pulseflow \
  get deployment ingestion-api -o jsonpath='{.status.readyReplicas}')" = 2
```

In a separate terminal, port-forward only through the named cluster and
measure a normal baseline before deleting a pod:

```bash
kubectl --context kind-pulseflow-recovery -n pulseflow \
  port-forward service/ingestion-api 18081:8080
```

With the project Python environment active, send 20 baseline requests and
record the generator's successful/failed counts:

```bash
python -m pulseflow.generator \
  --url http://127.0.0.1:18081/v1/events \
  --events 20 --rps 2 --duplicate-rate 0
```

For recovery-period outcomes, start a bounded probe in another terminal. It
posts unique valid events once per second for 180 seconds and records each
HTTP status or network error in ignored local artifacts:

```bash
mkdir -p artifacts/performance/kind-recovery
python3 - artifacts/performance/kind-recovery/requests.jsonl <<'PY' &
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

output = sys.argv[1]
deadline = time.monotonic() + 180
with open(output, "w", encoding="utf-8") as report:
    while time.monotonic() < deadline:
        event_id = str(uuid.uuid4())
        event = {
            "event_id": event_id,
            "user_id": "kind-recovery-probe",
            "session_id": "kind-recovery-probe",
            "event_type": "PLAY",
            "content_id": "kind-recovery",
            "device_type": "desktop",
            "country_code": "GB",
            "event_ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "attributes": {"source": "kind-recovery-probe"},
        }
        request = urllib.request.Request(
            "http://127.0.0.1:18081/v1/events",
            data=json.dumps(event).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        started = time.time()
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                status = response.status
        except urllib.error.HTTPError as error:
            status = error.code
        except Exception as error:
            status = f"network_error:{type(error).__name__}"
        report.write(json.dumps({
            "timestamp_epoch": started,
            "event_id": event_id,
            "status": status,
        }) + "\n")
        report.flush()
        time.sleep(max(0, started + 1 - time.time()))
PY
PROBE_PID=$!
trap 'kill "$PROBE_PID" 2>/dev/null || true' EXIT
```

The service port-forward may select one backing API pod. If that pod is deleted,
the port-forward itself can exit; subsequent network errors then measure the
forwarding path, not necessarily Service availability. Preserve those outcomes
and do not claim uninterrupted availability. For a Service-level availability
measurement, run the probe from inside the dedicated cluster against
`http://ingestion-api.pulseflow.svc.cluster.local:8080/v1/events`.

Delete exactly one selected API pod at a time. After each deletion, measure
until two replicas are Ready again; wait for that before selecting the next
pod. Compare each measured interval to 60 seconds, and retain all probe
statuses—including failures:

```bash
set -euo pipefail
recovery_failed=0
for attempt in 1 2; do
  POD="$(kubectl --context kind-pulseflow-recovery -n pulseflow \
    get pods -l app=ingestion-api -o json | python3 -c '
import json
import sys

pods = json.load(sys.stdin)["items"]
ready = [
    pod["metadata"]["name"]
    for pod in pods
    if pod["status"].get("phase") == "Running"
    and any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod["status"].get("conditions", [])
    )
]
print(ready[0] if ready else "")
')"
  if [[ -z "$POD" ]]; then
    echo "No Ready API pod is available to delete safely." >&2
    exit 1
  fi
  echo "Deleting one API pod: $POD"
  started_ns="$(python3 -c 'import time; print(time.monotonic_ns())')"
  deadline_ns=$((started_ns + 60000000000))
  kubectl --context kind-pulseflow-recovery -n pulseflow \
    delete pod "$POD" --wait=false
  while :; do
    remaining="$(kubectl --context kind-pulseflow-recovery -n pulseflow \
      get pod "$POD" --ignore-not-found -o name)"
    ready_replicas="$(kubectl --context kind-pulseflow-recovery -n pulseflow \
      get deployment ingestion-api -o jsonpath='{.status.readyReplicas}')"
    now_ns="$(python3 -c 'import time; print(time.monotonic_ns())')"
    elapsed="$(python3 - "$started_ns" "$now_ns" <<'PY'
import sys
print(f"{(int(sys.argv[2]) - int(sys.argv[1])) / 1_000_000_000:.3f}")
PY
)"
    if (( now_ns >= deadline_ns )); then
      printf 'attempt=%s recovery_seconds=%s handbook_target_seconds=60 result=TIMEOUT; not deleting another pod.\n' \
        "$attempt" "$elapsed" >&2
      recovery_failed=1
      break 2
    fi
    if [[ -z "$remaining" && "$ready_replicas" == 2 ]]; then
      printf 'attempt=%s recovery_seconds=%s handbook_target_seconds=60 result=RECOVERED\n' \
        "$attempt" "$elapsed"
      break
    fi
    sleep 2
  done
done
kill "$PROBE_PID" 2>/dev/null || true
wait "$PROBE_PID" 2>/dev/null || true
trap - EXIT
python3 - artifacts/performance/kind-recovery/requests.jsonl <<'PY'
import collections
import json
import sys

statuses = collections.Counter(
    str(json.loads(line)["status"])
    for line in open(sys.argv[1], encoding="utf-8")
)
print("Recorded request outcomes:", dict(statuses))
PY
if [[ "$recovery_failed" -ne 0 ]]; then
  exit 1
fi
```

This measures pod replacement/readiness and client-observed outcomes. Report
any failed HTTP requests and network errors; do not describe availability as
uninterrupted unless the recorded outcomes support that statement.

Teardown is explicit and scoped to the dedicated cluster only:

```bash
kind delete cluster --name pulseflow-recovery
```

## Evidence and limitations

Use [the performance report template](performance-report.md) for machine
details, profiles, achieved request rates, latency/failure/dropped-iteration
counts, Spark progress, freshness distributions, and recovery outcomes.
Freshness uses the distribution of Silver `processed_at - event_time`, not the
age of the newest event. Kafka producer idempotence and
watermark-bounded Spark deduplication do not guarantee exactly-once delivery
end to end. The real Kafka-to-Parquet integration test remains the evidence
for duplicate, late-event, and quarantine behavior; malformed Kafka input is
not sent through the validating HTTP API.
