#!/usr/bin/env bash
set -euo pipefail

# Locate the PulseFlow repository root.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/infra/compose/docker-compose.yml"

echo "Creating Kafka topic: user-behaviour.v1"

docker compose -f "$COMPOSE_FILE" exec -T broker \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create \
  --if-not-exists \
  --topic user-behaviour.v1 \
  --partitions 6 \
  --replication-factor 1

echo "Verifying Kafka topic configuration..."

docker compose -f "$COMPOSE_FILE" exec -T broker \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --describe \
  --topic user-behaviour.v1

echo "Kafka topic setup complete."
