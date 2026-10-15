#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/infra/compose/docker-compose.yml"
OUTAGE_REQUEST_TIMEOUT_SECONDS=70
PROJECT="${COMPOSE_PROJECT_NAME:-}"
RUN_ID="${RUN_ID:-}"
API_PORT="${PULSEFLOW_API_HOST_PORT:-}"
TOPIC="${PULSEFLOW_TOPIC:-}"

if [[ "${PULSEFLOW_ALLOW_FAILURE_TESTS:-}" != "YES" ]]; then
  echo "Refusing failure experiment: set PULSEFLOW_ALLOW_FAILURE_TESTS=YES explicitly." >&2
  exit 2
fi
if [[ ! "$PROJECT" =~ ^pulseflow-perf-[a-z0-9][a-z0-9-]*$ ]]; then
  echo "Refusing failure experiment: COMPOSE_PROJECT_NAME must be a dedicated pulseflow-perf-* name." >&2
  exit 2
fi
if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "Refusing failure experiment: RUN_ID must contain lowercase letters, digits, or hyphens." >&2
  exit 2
fi
if [[ ! "$API_PORT" =~ ^[0-9]+$ ]] || (( API_PORT < 1024 || API_PORT > 65535 )); then
  echo "Refusing failure experiment: set PULSEFLOW_API_HOST_PORT to the dedicated API port." >&2
  exit 2
fi
if [[ ! "$TOPIC" =~ ^pulseflow-perf-[a-z0-9][a-z0-9-]*$ ]]; then
  echo "Refusing failure experiment: PULSEFLOW_TOPIC must be a dedicated pulseflow-perf-* topic." >&2
  exit 2
fi

compose=(docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE")
broker_id="$("${compose[@]}" ps --all --quiet broker)"
api_id="$("${compose[@]}" ps --all --quiet ingestion-api)"
if [[ -z "$broker_id" || "$broker_id" == *$'\n'* || -z "$api_id" || "$api_id" == *$'\n'* ]]; then
  echo "Refusing failure experiment: expected exactly one broker and one ingestion-api container." >&2
  exit 2
fi

verify_label() {
  local container_id="$1"
  local service="$2"
  local labels
  labels="$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}} {{index .Config.Labels "com.docker.compose.service"}}' "$container_id")"
  if [[ "$labels" != "$PROJECT $service" ]]; then
    echo "Refusing failure experiment: $service container labels do not match project $PROJECT." >&2
    exit 2
  fi
}
verify_label "$broker_id" broker
verify_label "$api_id" ingestion-api

broker_health="$(docker inspect --format '{{.State.Health.Status}}' "$broker_id")"
if [[ "$broker_health" != "healthy" ]]; then
  echo "Refusing failure experiment: isolated broker is not healthy ($broker_health)." >&2
  exit 2
fi
if ! curl --fail --silent --show-error --max-time 5 \
  "http://127.0.0.1:$API_PORT/actuator/health" >/dev/null; then
  echo "Refusing failure experiment: isolated API health endpoint is not healthy." >&2
  exit 2
fi
"${compose[@]}" exec -T broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 --describe --topic "$TOPIC" >/dev/null

temporary_dir="$(mktemp -d)"
broker_stopped=false
experiment_started=false
result_written=false
failure_status=""
recovery_status=""
failure_started=""
recovery_started=""
artifact_dir="${PULSEFLOW_ARTIFACTS_DIR:-$PROJECT_ROOT/artifacts/performance}"
report="$artifact_dir/compose-recovery-$RUN_ID.json"
recovery_started_ns=""

monotonic_ns() {
  python3 -c 'import time; print(time.monotonic_ns())'
}

recovery_elapsed_seconds() {
  if [[ -z "$recovery_started_ns" ]]; then
    printf ''
    return
  fi
  local finished_ns="${1:-$(monotonic_ns)}"
  python3 - "$recovery_started_ns" "$finished_ns" <<'PY'
import sys

elapsed_ns = max(0, int(sys.argv[2]) - int(sys.argv[1]))
print(f"{elapsed_ns / 1_000_000_000:.6f}")
PY
}

write_result() {
  local result="$1"
  local finished_at="$2"
  local recovery_elapsed="$3"
  mkdir -p "$artifact_dir"
  python3 - "$report" "$PROJECT" "$RUN_ID" "$TOPIC" "$failure_status" \
    "$recovery_status" "$failure_started" "$recovery_started" "$finished_at" \
    "$recovery_elapsed" \
    "$temporary_dir/failure-response.json" "$temporary_dir/recovery-response.json" \
    "$result" <<'PY'
import json
import sys
from pathlib import Path

(
    output,
    project,
    run_id,
    topic,
    failure_status,
    recovery_status,
    failure_started,
    recovery_started,
    finished_at,
    recovery_elapsed,
    failure_response_path,
    recovery_response_path,
    result,
) = sys.argv[1:]

def response_body(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

Path(output).write_text(
    json.dumps({
        "project": project,
        "run_id": run_id,
        "topic": topic,
        "failure_http_status": failure_status or None,
        "recovery_http_status": recovery_status or None,
        "recovery_seconds": float(recovery_elapsed) if recovery_elapsed else None,
        "failure_response": response_body(failure_response_path),
        "recovery_response": response_body(recovery_response_path),
        "failure_started_epoch_seconds": int(failure_started) if failure_started else None,
        "recovery_started_epoch_seconds": int(recovery_started) if recovery_started else None,
        "finished_epoch_seconds": int(finished_at) if finished_at else None,
        "result": result,
    }, indent=2) + "\n",
    encoding="utf-8",
)
print(f"Recovery result ({result}) saved to {output}")
PY
  result_written=true
}

restore_broker() {
  if [[ "$broker_stopped" == true ]]; then
    local current_broker_id
    local current_labels
    current_broker_id="$("${compose[@]}" ps --all --quiet broker 2>/dev/null || true)"
    if [[ -n "$current_broker_id" && "$current_broker_id" != *$'\n'* ]]; then
      current_labels="$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}} {{index .Config.Labels "com.docker.compose.service"}}' "$current_broker_id" 2>/dev/null || true)"
    else
      current_labels=""
    fi
    if [[ "$current_labels" == "$PROJECT broker" ]]; then
      echo "Attempting to restore the verified isolated broker after early exit." >&2
      "${compose[@]}" up -d broker >/dev/null || true
    else
      echo "Refusing cleanup restart: isolated broker project/service labels could not be verified." >&2
    fi
  fi
  if [[ "$experiment_started" == true && "$result_written" != true ]]; then
    write_result interrupted "$(date +%s)" "$(recovery_elapsed_seconds)" || true
  fi
  rm -rf -- "$temporary_dir"
}
trap restore_broker EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

create_event() {
  python3 - "recovery-$RUN_ID" <<'PY'
import json
import sys
import uuid
from datetime import UTC, datetime

run_id = sys.argv[1]
print(json.dumps({
    "event_id": str(uuid.uuid4()),
    "user_id": f"recovery-{run_id}",
    "session_id": f"recovery-{run_id}",
    "event_type": "PLAY",
    "content_id": "broker-recovery-test",
    "device_type": "desktop",
    "country_code": "GB",
    "event_ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    "attributes": {"source": "compose-recovery-test", "run_id": run_id},
}))
PY
}

post_event() {
  local output_file="$1"
  local max_time="${2:-10}"
  local payload
  payload="$(create_event)"
  curl --silent --show-error --max-time "$max_time" \
    --output "$output_file" --write-out '%{http_code}' \
    --header 'Content-Type: application/json' \
    --data "$payload" \
    "http://127.0.0.1:$API_PORT/v1/events" || true
}

experiment_started=true
failure_started="$(date +%s)"
broker_stopped=true
"${compose[@]}" stop broker
failure_status="$(post_event "$temporary_dir/failure-response.json" "$OUTAGE_REQUEST_TIMEOUT_SECONDS")"
if [[ "$failure_status" != "503" ]]; then
  echo "Expected HTTP 503 during the broker outage, observed HTTP $failure_status." >&2
  cat "$temporary_dir/failure-response.json" >&2 || true
  write_result "unexpected-outage-response" "$(date +%s)" ""
  exit 1
fi

recovery_started="$(date +%s)"
recovery_started_ns="$(monotonic_ns)"
recovery_deadline_ns=$((recovery_started_ns + 60000000000))
"${compose[@]}" up -d broker
restarted_broker_id="$("${compose[@]}" ps --quiet broker)"
if [[ -z "$restarted_broker_id" || "$restarted_broker_id" == *$'\n'* ]]; then
  echo "Isolated broker service did not return exactly one container." >&2
  exit 1
fi
verify_label "$restarted_broker_id" broker
broker_id="$restarted_broker_id"
recovery_status=""
recovery_finished_ns="$recovery_started_ns"
while :; do
  now_ns="$(monotonic_ns)"
  if (( now_ns >= recovery_deadline_ns )); then
    recovery_finished_ns="$now_ns"
    break
  fi
  broker_health="$(docker inspect --format '{{.State.Health.Status}}' "$broker_id" 2>/dev/null || true)"
  now_ns="$(monotonic_ns)"
  if (( now_ns >= recovery_deadline_ns )); then
    recovery_finished_ns="$now_ns"
    break
  fi
  if [[ "$broker_health" == "healthy" ]]; then
    remaining_ns=$((recovery_deadline_ns - now_ns))
    request_timeout="$(python3 - "$remaining_ns" <<'PY'
import sys

print(f"{int(sys.argv[1]) / 1_000_000_000:.9f}")
PY
)"
    recovery_status="$(post_event "$temporary_dir/recovery-response.json" "$request_timeout")"
    recovery_finished_ns="$(monotonic_ns)"
    if [[ "$recovery_status" == "202" ]] \
      && (( recovery_finished_ns < recovery_deadline_ns )); then
      break
    fi
    if (( recovery_finished_ns >= recovery_deadline_ns )); then
      break
    fi
  fi
  sleep 1
  recovery_finished_ns="$(monotonic_ns)"
done

if [[ "$recovery_status" != "202" ]] \
  || (( recovery_finished_ns >= recovery_deadline_ns )); then
  recovery_duration="$(recovery_elapsed_seconds "$recovery_finished_ns")"
  echo "Recovery did not produce HTTP 202 within 60 seconds (last HTTP $recovery_status, elapsed ${recovery_duration}s)." >&2
  write_result "recovery-timeout" "$(date +%s)" "$recovery_duration"
  exit 1
fi
broker_stopped=false
recovered_at="$(date +%s)"
recovery_duration="$(recovery_elapsed_seconds "$recovery_finished_ns")"
write_result recovered "$recovered_at" "$recovery_duration"
