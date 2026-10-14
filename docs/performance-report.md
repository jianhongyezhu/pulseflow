# PulseFlow performance and recovery report

**Status: NOT RUN.** Fill this report only with measurements produced by the
commands in [the runbook](runbook.md). Do not treat handbook targets as
measured results or generalize local results to production-scale behavior.
Keep raw k6 output, reports, Spark data/checkpoints, and request traces in the
ignored `artifacts/performance/` directory or a temporary directory.

## Environment

| Field | Measurement |
| --- | --- |
| CPU / cores | NOT RUN |
| Memory | NOT RUN |
| OS, WSL version, Docker Desktop/runtime | NOT RUN |
| Java version | NOT RUN |
| Python version | NOT RUN |
| PySpark version | NOT RUN |
| Spark Kafka connector | NOT RUN |
| k6 version | NOT RUN |
| Docker Compose version | NOT RUN |

## API benchmark

| Field | Measurement |
| --- | --- |
| Profile (`handbook`, `arrival` baseline, or opt-in `stretch`) | NOT RUN |
| Run ID and isolated topic | NOT RUN |
| Duration / configured rate or VU stages | NOT RUN |
| Requested requests/second (arrival profile only) | NOT RUN |
| Achieved requests/second | NOT RUN |
| HTTP request count / HTTP 202 accepted count | NOT RUN |
| HTTP request failure rate | NOT RUN |
| HTTP request p95 latency (ms) | NOT RUN |
| HTTP 202 check passes / failures | NOT RUN |
| Dropped iterations | NOT RUN |
| Handbook target: failure rate <1% | NOT RUN |
| Handbook target: p95 <150 ms at 50 requests/second | NOT RUN |

VUs and requests/second are distinct measures. The handbook ramping-VU
profile does not establish a fixed arrival rate; the baseline arrival-rate
profile is required to assess the 50 requests/second target.

| Target check | Target | Measured | Result |
| --- | --- | --- | --- |
| Baseline offered load | 50 requests/second for 60 seconds | NOT RUN | NOT RUN |
| HTTP request failures | <1% | NOT RUN | NOT RUN |
| API p95 latency at baseline | <150 ms | NOT RUN | NOT RUN |
| Dropped k6 iterations | 0 | NOT RUN | NOT RUN |

## Spark and event freshness

| Field | Measurement |
| --- | --- |
| Isolated `PULSEFLOW_DATA_ROOT` | NOT RUN |
| Spark Silver input rows/second | NOT RUN |
| Spark Silver processed rows/second | NOT RUN |
| Spark microbatch execution duration (ms) | NOT RUN |
| k6 accepted events in this run | NOT RUN |
| Silver events with this run ID after bounded wait | NOT RUN |
| Accepted-minus-Silver discrepancy | NOT RUN |
| Missing/invalid processing timestamp count | NOT RUN |
| End-to-end freshness p50 (`processed_at - event_time`, seconds) | NOT RUN |
| End-to-end freshness p95 (`processed_at - event_time`, seconds) | NOT RUN |

Spark Silver input and processed rows/second describe Spark query progress,
not API request rate or Kafka broker throughput. Freshness percentiles are
null when no valid events are observed; no zero value should be substituted.

## Streaming quality evidence

The existing real Kafka-to-Parquet integration test verifies watermark-bounded
event-ID deduplication, late-event behavior, quarantine, and Gold output.
Record the actual command/result for this run; do not count malformed requests
sent through the API as quarantine evidence.

| Evidence | Result |
| --- | --- |
| Integration test command and result | NOT RUN |
| Duplicate event IDs in Bronze vs. Silver | NOT RUN |
| Invalid Kafka record and quarantine reason | NOT RUN |
| Watermark/late-event result | NOT RUN |

## Recovery experiments

| Field | Measurement |
| --- | --- |
| Isolated Compose project / topic | NOT RUN |
| API response during broker outage | NOT RUN |
| Broker/API final recovery result | NOT RUN |
| Compose recovery time (seconds) | NOT RUN |
| Dedicated kind cluster: `pulseflow-recovery` | NOT RUN |
| API pod recovery intervals (seconds) | NOT RUN |
| Handbook pod-recovery target: <=60 seconds | NOT RUN |
| Kind probe request outcomes (202 / other HTTP / network errors) | NOT RUN |

| Recovery target check | Target | Measured | Result |
| --- | --- | --- | --- |
| Compose broker-to-API recovery | Successful 202 within 60 seconds | NOT RUN | NOT RUN |
| kind API pod replacement/readiness | <=60 seconds per pod | NOT RUN | NOT RUN |

## Analysis and next steps

### Observed bottlenecks

NOT RUN.

### Limitations and environmental effects

NOT RUN.

### Next engineering steps

NOT RUN.

These local synthetic tests do not represent production-scale guarantees.
Kafka producer idempotence and Spark watermark-bounded deduplication do not
provide exactly-once delivery end to end.
