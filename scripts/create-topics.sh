#!/usr/bin/env bash
set -euo pipefail

# Locate the PulseFlow repository root.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/infra/compose/docker-compose.yml"
TOPIC="${PULSEFLOW_TOPIC:-user-behaviour.v1}"

echo "Creating Kafka topic: $TOPIC"

docker compose -f "$COMPOSE_FILE" exec -T broker \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 6 \
  --replication-factor 1

echo "Verifying Kafka topic configuration..."

docker compose -f "$COMPOSE_FILE" exec -T broker \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --describe \
  --topic "$TOPIC"

echo "Kafka topic setup complete."
