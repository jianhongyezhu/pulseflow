#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/infra/compose/docker-compose.yml"
TOPIC="user-behaviour.v1"
API_BASE_URL="${PULSEFLOW_API_URL:-http://localhost:8080}"
API_ENDPOINT="$API_BASE_URL/v1/events"
TMP_DIR="$(mktemp -d)"
CONSUMER_PIDS=()

cleanup() {
  for pid in "${CONSUMER_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "Ensuring Kafka topic exists: $TOPIC"
"$PROJECT_ROOT/scripts/create-topics.sh"

echo "Waiting up to 60 seconds for API health..."
health_deadline=$((SECONDS + 60))
until curl --fail --silent --show-error \
  --connect-timeout 2 \
  --max-time 3 \
  "$API_BASE_URL/actuator/health" >/dev/null; do
  if ((SECONDS >= health_deadline)); then
    echo "API did not become healthy within 60 seconds: $API_BASE_URL/actuator/health" >&2
    exit 1
  fi
  sleep 2
done

echo "Capturing Kafka partition offsets..."
docker compose -f "$COMPOSE_FILE" exec -T broker \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server broker:19092 \
  --topic "$TOPIC" \
  --time -1 >"$TMP_DIR/offsets"

EVENT_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
USER_ID="smoke-$EVENT_ID"
SESSION_ID="session-$EVENT_ID"
EVENT_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PAYLOAD="$(
  python3 -c '
import json
import sys

event_id, user_id, session_id, event_ts = sys.argv[1:]
print(json.dumps({
    "event_id": event_id,
    "user_id": user_id,
    "session_id": session_id,
    "event_type": "PLAY",
    "content_id": "track-smoke-test",
    "device_type": "desktop",
    "country_code": "GB",
    "event_ts": event_ts,
    "attributes": {"source": "smoke-test", "subscription_tier": "free"},
}))
' "$EVENT_ID" "$USER_ID" "$SESSION_ID" "$EVENT_TS"
)"

echo "Starting consumers at pre-request offsets..."
while IFS=: read -r topic partition offset; do
  [[ "$topic" == "$TOPIC" ]] || continue
  docker compose -f "$COMPOSE_FILE" exec -T broker \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server broker:19092 \
    --topic "$TOPIC" \
    --partition "$partition" \
    --offset "$offset" \
    --max-messages 1 \
    --timeout-ms 15000 \
    --property print.key=true \
    --property key.separator=$'\t' \
    >"$TMP_DIR/partition-$partition.log" \
    2>"$TMP_DIR/partition-$partition.err" &
  CONSUMER_PIDS+=("$!")
done <"$TMP_DIR/offsets"

sleep 2
echo "Publishing synthetic event $EVENT_ID..."
HTTP_STATUS="$(
  curl --silent --show-error \
    --output "$TMP_DIR/response.json" \
    --write-out '%{http_code}' \
    --header 'Content-Type: application/json' \
    --data "$PAYLOAD" \
    "$API_ENDPOINT"
)"
if [[ "$HTTP_STATUS" != "202" ]]; then
  echo "Expected HTTP 202, received HTTP $HTTP_STATUS" >&2
  cat "$TMP_DIR/response.json" >&2
  exit 1
fi
python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as response_file:
    response = json.load(response_file)
expected_id = sys.argv[2]
if response.get("event_id") != expected_id or response.get("status") != "accepted":
    raise SystemExit(f"Unexpected API response: {response}")
' "$TMP_DIR/response.json" "$EVENT_ID"

for pid in "${CONSUMER_PIDS[@]}"; do
  wait "$pid" || true
done
CONSUMER_PIDS=()

if ! grep -F "$EVENT_ID" "$TMP_DIR"/partition-*.log \
  | grep -F "${USER_ID}"$'\t' >/dev/null; then
  echo "Kafka did not return event $EVENT_ID with key $USER_ID" >&2
  for error_file in "$TMP_DIR"/partition-*.err; do
    [[ -s "$error_file" ]] && cat "$error_file" >&2
  done
  exit 1
fi

echo "Smoke test passed: HTTP 202 and Kafka record verified with user_id key."
