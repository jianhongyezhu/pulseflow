# PulseFlow performance and recovery report

**Status: MEASURED — local development environment.** These are reproducible
measurements from one WSL2/Docker Desktop workstation, not production-scale
claims. The exact commands and safety checks are in [the runbook](runbook.md).
Raw output, run summaries, and Spark data/checkpoints remain in the ignored
`artifacts/performance/` directory and temporary directories.

## Environment

| Field | Measurement |
| --- | --- |
| CPU / cores | AMD Ryzen 7 7735HS; 16 logical CPUs |
| Memory | 19 GiB reported by WSL |
| OS, WSL version, Docker Desktop/runtime | WSL2 Ubuntu; Linux kernel 6.18.54.1; Docker Desktop 4.84.0; Docker Engine 29.6.2 |
| Java version | OpenJDK 21.0.12.1 |
| Python version | 3.14.4 |
| PySpark version | 4.2.0 |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` |
| k6 version | `grafana/k6:0.55.0` (`k6 v0.55.0`, Go 1.23.3) |
| Docker Compose version | v5.3.1 |

## API benchmarks

The isolated Compose project was `pulseflow-perf-01c990568c`, with the unique
Kafka topic `pulseflow-perf-01c990568c`. All corrected runs used the internal
Compose API address. Each profile had a unique `run_id`; the first attempts
made before the `K6_PROFILE_DURATION` correction are excluded because native
k6 `K6_DURATION` overrode the script's dynamic scenarios.

| Profile | Run ID | Offered load / duration | Requests / HTTP 202 | Achieved requests/s | Request failures | p95 request duration | Checks passed / failed | Dropped iterations |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fixed arrival baseline | `pulseflow-perf-01c990568c-corrected-arrival-50` | 50 requests/s for 60 s | 3,001 / 3,001 | 50.0106 | 0% | 16.815 ms | 3,001 / 0 | 0 |
| Handbook VU ramp | `pulseflow-perf-01c990568c-corrected-handbook` | 30 s to 20 VUs; 60 s at 50 VUs; 60 s at 100 VUs; 30 s ramp down | 553,090 / 553,090 | 3,073.2374 observed over the run | 0% | 29.120 ms | 553,090 / 0 | 0 |
| Opt-in stretch | `pulseflow-perf-01c990568c-corrected-stretch-250` | 250 requests/s for 30 s | 7,501 / 7,501 | 249.9963 | 0% | 16.649 ms | 7,501 / 0 | 0 |

VUs and requests/second are distinct measures. The handbook profile is a
ramping-VU test, not a fixed arrival rate. Only the 50 requests/second fixed
arrival profile measures the handbook's 150 ms p95 target. The stretch p95 is
reported as an observation; the 150 ms threshold is not applied to that
profile.

| Target check | Target | Measured | Result |
| --- | --- | --- | --- |
| Baseline offered load | 50 requests/s for 60 seconds | 50.0106 requests/s for 60 seconds | PASS |
| HTTP request failures | <1% | 0% | PASS |
| API p95 latency at baseline | <150 ms | 16.815 ms | PASS |
| HTTP 202 checks at baseline | >99% | 100% (3,001 / 3,001) | PASS |
| Dropped k6 iterations | 0 | 0 in all three profiles | PASS |
| Opt-in 250 requests/s stretch | Exercise only; no handbook p95 claim | 249.9963 requests/s, 0% failures | COMPLETED |

## Spark and event freshness

The Spark data and checkpoints were isolated at
`/tmp/pulseflow-perf-01c990568c-spark.Lg1W8K`. Each run was analyzed after a
bounded 60-second processing wait. `processed_at - event_ts` is a distribution
over the run's Silver events, not the age of the latest event.

| Profile / run ID | Accepted requests | Silver events | Accepted minus Silver | Missing/invalid `processed_at` | Freshness p50 | Freshness p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fixed arrival / `pulseflow-perf-01c990568c-corrected-arrival-50` | 3,001 | 3,001 | 0 | 0 | 4.527 s | 6.062 s |
| Handbook / `pulseflow-perf-01c990568c-corrected-handbook` | 553,090 | 553,090 | 0 | 0 | 7.153 s | 10.643 s |
| Stretch / `pulseflow-perf-01c990568c-corrected-stretch-250` | 7,501 | 7,501 | 0 | 0 | 3.948 s | 5.521 s |

| Target check | Target | Measured | Result |
| --- | --- | --- | --- |
| Baseline event freshness (50 requests/s) | p95 <5 seconds | p95 6.062 s | FAIL |
| Spark processing throughput during each corrected profile | Processed rows/s >= input rows/s | Aggregate scrape includes invalid attempts and corrected profiles; per-profile comparison unavailable | NOT VERIFIED |

One-second exporter sampling recorded 370 samples. Input rows/s had p50 182.8,
p95 5,227.2, and maximum 6,769.5; processed rows/s had p50 226.55, p95
5,216.1, and maximum 6,013.6. Batch duration had p50 2,241 ms, p95 4,371 ms,
and maximum 5,503 ms. The scrape spans the initial invalid attempts as well as
the corrected profiles, so these aggregate Spark values cannot be attributed
to one profile. At the final idle sample, input and processed rates were 0;
the last batch-duration sample was 60 ms.

Spark Silver input and processed rates describe Spark query progress, not API
request rate or Kafka broker throughput. Freshness percentiles are null when
there are no valid events; no zero value is substituted.

## Streaming quality evidence

Command: `python -m pytest -q -s python/integration_tests/test_streaming_pipeline.py`
against the isolated Compose broker. **PASS — 1 test passed in 76.56 seconds.**
The test used its own temporary topic and pytest-owned temporary data root.

- Bronze contained six Kafka records, including the same valid payload twice.
- Silver retained one copy of the duplicated event ID; the test's final
  expected Silver set contained three unique non-late event IDs.
- One malformed Kafka JSON record was written to Quarantine with reason
  `malformed_json`.
- A late event older than the active Silver watermark was consumed but absent
  from Silver.
- Gold output contained the expected two closed event-time windows.

The malformed record was sent directly to Kafka for quarantine coverage; it
was not submitted through the validating HTTP API.

## Recovery experiments

### Isolated Compose broker recovery

| Field | Measurement |
| --- | --- |
| Compose project / topic | `pulseflow-perf-01c990568c` / `pulseflow-perf-01c990568c` |
| API response during broker outage | HTTP 503 Service Unavailable |
| Response after broker restart | HTTP 202 Accepted |
| Measured restart-to-accepted recovery | 20.989 seconds |
| Result against 60-second target | PASS |

### Dedicated kind API pod recovery

The experiment created the previously absent dedicated cluster
`pulseflow-recovery` and used only context `kind-pulseflow-recovery`. Two
replicas were Ready before the test. The warm baseline returned 20/20 HTTP
202s. An initial pre-warm baseline had one `ReadTimeout` and 19/20 HTTP 202s;
it is retained as an observed transient rather than hidden.

| Attempt | Deleted API pod | Replacement/readiness duration | Target | Result |
| --- | --- | ---: | ---: | --- |
| 1 | `ingestion-api-85d6b95f9-z5zvs` | 43.736 s | <=60 s | PASS |
| 2 | `ingestion-api-85d6b95f9-4s8vl` | 37.635 s | <=60 s | PASS |

The 180-second host-side request probe recorded 83 HTTP 202 responses, one
`RemoteDisconnected`, and 96 `URLError` outcomes. During the second pod
replacement, the `kubectl port-forward service/ingestion-api` process lost its
selected backing pod and exited. Therefore those network errors include a
broken local forwarding path and are **not** evidence that the Kubernetes
Service itself was unavailable. The readiness intervals are measured and
passed, but the probe does not establish uninterrupted client availability;
the availability result is inconclusive.

| Recovery target check | Target | Measured | Result |
| --- | --- | --- | --- |
| Compose broker-to-API recovery | HTTP 202 within 60 seconds | 20.989 s | PASS |
| kind API pod replacement/readiness | <=60 seconds per pod | 43.736 s and 37.635 s | PASS |
| kind client availability | Report observed outcomes; no unsupported continuity claim | 83 HTTP 202; forwarding-path errors noted above | INCONCLUSIVE |

## Analysis and next steps

### Observed bottlenecks

No CPU or memory utilization sampling, profiling, or bottleneck isolation was
performed. The reported API latency and Spark rates are observations from this
single local environment, not a diagnosis or capacity estimate.

### Limitations and environmental effects

- Docker Desktop/WSL2, a local Compose broker, and a one-node kind cluster
  differ materially from a production deployment.
- The Spark metrics summary combines initial invalid profile attempts and
  corrected runs; per-profile Spark throughput was not isolated.
- The host-side kind probe was coupled to a `kubectl port-forward` session
  whose backing pod was replaced. It cannot prove or disprove Service
  availability during the resulting network errors.
- Early k6 attempts that used the conflicting native `K6_DURATION` setting
  were excluded; all displayed k6 profile results are from corrected runs.

### Next engineering steps

For a more precise kind availability measurement, run the bounded probe from
inside the dedicated cluster against the ClusterIP Service, or use a stable
host networking path that is independent of an individual API pod. Repeat
Spark sampling separately for each profile if profile-correlated streaming
rates are required.

These local synthetic tests do not represent production-scale guarantees.
Kafka producer idempotence and Spark watermark-bounded deduplication do not
provide exactly-once delivery end to end.
