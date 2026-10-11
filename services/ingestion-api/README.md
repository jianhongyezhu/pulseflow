# PulseFlow Ingestion API

The API validates user events against the v1 contract in [`schemas/user-behaviour-v1.schema.json`](../../schemas/user-behaviour-v1.schema.json), publishes them to Kafka using `user_id` as the record key, and returns HTTP 202 after Kafka acknowledges the send.

## Delivery and deduplication

The API does not persist request IDs or deduplicate HTTP submissions. Submitting the same event more than once, including a client retry after a timeout or lost response, can create multiple Kafka records with the same `event_id`.

Kafka producer idempotence protects against duplicate writes caused by producer retries within a producer session; it does not deduplicate separate HTTP requests or application resends. `acks=all` waits for acknowledgements from the broker's in-sync replicas, but does not confirm downstream processing.

Downstream PySpark jobs should deduplicate by `event_id`. That deduplication is only effective for as long as the job retains its deduplication state (for example, its configured watermark/checkpoint state), and only within the guarantees of the chosen sink. Events arriving after state expiry may be treated as new, and non-idempotent external side effects can still repeat. This is at-least-once delivery, not an end-to-end exactly-once guarantee.

If Kafka accepted a record but the API did not receive the acknowledgement before its three-second wait expired, the API returns HTTP 503. The result is ambiguous: the event may already be in Kafka. Retrying can therefore create a duplicate record.
