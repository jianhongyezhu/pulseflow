# PulseFlow Ingestion API

The API validates user events against the v1 contract in [`schemas/user-behaviour-v1.schema.json`](../../schemas/user-behaviour-v1.schema.json), publishes them to Kafka using `user_id` as the record key, and returns HTTP 202 after Kafka acknowledges the send.

## Run with Docker Compose

From the repository root, build the API image and start the broker and API:

```bash
docker compose -f infra/compose/docker-compose.yml up --build -d
```

Check container status and follow API logs with:

```bash
docker compose -f infra/compose/docker-compose.yml ps
docker compose -f infra/compose/docker-compose.yml logs -f ingestion-api
```

The API is published at `http://localhost:8080`. Check its health endpoint or
run the end-to-end smoke test:

```bash
curl --fail http://localhost:8080/actuator/health
scripts/smoke-test.sh
```

The smoke test creates the default topic if needed, sends one synthetic event,
checks for HTTP 202, then verifies the event was written to Kafka with `user_id`
as the record key.

Stop the services without removing Kafka data using:

```bash
docker compose -f infra/compose/docker-compose.yml stop
```

The API container connects to Kafka at `broker:19092` over the default Compose
network. Use `localhost:9092` for host tools such as the Python generator; that
listener is published on the host and is not the address containers should use.
Kafka data is stored in the named `kafka-data` volume and survives normal
container stops and removal. Do not use `docker compose down -v` unless you
intentionally want to delete that volume and its Kafka data.

## GitHub Actions CI

The `CI` workflow runs on pull requests and pushes to `main`. It verifies the
Java service with `mvn clean verify`, runs Ruff and the full Python test suite
(including PySpark transformation and DuckDB analytics tests), then builds the
ingestion API Docker image after both test jobs pass. The Docker job builds
locally on the runner and does not push an image or deploy an environment.

The `Kind smoke test` workflow runs on manual dispatch or pull requests that
change the Java service, Dockerfile, Kubernetes/kind resources, smoke-test
script, or the workflow itself. It creates an isolated `pulseflow-ci` kind
cluster, deploys Kafka and the API, publishes one event, and verifies its Kafka
key. It deletes only that workflow's disposable cluster at job completion.
Neither workflow deploys to a live or production environment.

Run the CI checks locally from the repository root:

```bash
mvn --batch-mode --file services/ingestion-api/pom.xml clean verify
source .venv/bin/activate
ruff check python/
python -m pytest -q python/tests
docker build --file services/ingestion-api/Dockerfile \
  --tag pulseflow-ingestion services/ingestion-api
```

The Python tests require Java 21 and the project's `streaming` and `dev`
extras; install them once with `python -m pip install -e "./python[streaming,dev]"`.
To run the kind smoke test on GitHub, open **Actions → Kind smoke test → Run
workflow**. For a local end-to-end run, follow the kind steps below with a
disposable cluster named `pulseflow-ci`. In those commands, use
`kind create cluster --name pulseflow-ci`, build/load the
`pulseflow-ingestion` image, apply the namespace and Kafka resources, wait for
Kafka, and create the six-partition topic. Apply a temporary copy of the API
manifest with its image changed from `pulseflow-ingestion:dev` to
`pulseflow-ingestion`; the workflow makes this temporary substitution and does
not change the tracked local `:dev` manifest. Then follow the port-forward,
health, event-publishing, and Kafka-verification steps below. Clean up only
your own disposable cluster with `kind delete cluster --name pulseflow-ci`.

For CI failures, open the failed workflow run and inspect the job logs. The
kind smoke job also captures Pod status, descriptions, events, Kafka/API logs,
and the API port-forward log on failure.

## Tagged GHCR releases

The `Publish ingestion API release` workflow runs only when a version tag
matching `v*.*.*` is pushed. Before publishing, it accepts only stable
`vMAJOR.MINOR.PATCH` tags (for example, `v0.1.0`) on a commit that is already
part of `origin/main`, and only in `jianhongyezhu/pulseflow`. It builds the
existing `services/ingestion-api/` Docker context and publishes
`ghcr.io/jianhongyezhu/pulseflow-ingestion:<version>` using the built-in
GitHub Actions token. The image includes the PulseFlow source-repository OCI
label. It does not create a floating `latest` tag.

Create and push a release tag only after the release PR has been merged into
`main`. This workflow does not create the tag for you:

```bash
git fetch origin main
git switch main
git pull --ff-only origin main
git tag -a v0.1.0 -m "PulseFlow v0.1.0"
git push origin v0.1.0
```

For example, pull the image published for `v0.1.0` with:

```bash
docker pull ghcr.io/jianhongyezhu/pulseflow-ingestion:0.1.0
```

Check the release workflow run's summary for its image name, version, and
digest. To inspect the registry manifest and confirm its source/version
metadata, use:

```bash
docker buildx imagetools inspect \
  ghcr.io/jianhongyezhu/pulseflow-ingestion:0.1.0
docker pull ghcr.io/jianhongyezhu/pulseflow-ingestion:0.1.0
docker image inspect ghcr.io/jianhongyezhu/pulseflow-ingestion:0.1.0 \
  --format '{{index .Config.Labels "org.opencontainers.image.source"}} {{index .Config.Labels "org.opencontainers.image.version"}} {{index .Config.Labels "org.opencontainers.image.revision"}}'
git rev-list -n 1 v0.1.0
```

The `org.opencontainers.image.revision` value should match the commit printed
by `git rev-list`; the workflow run verifies that commit is on `main`. Its
digest identifies the exact published image. The package's visibility and
repository association can be checked under the
repository owner's GitHub **Packages** page and the package's settings. Do not
assume the package is public: confirm its visibility there before distributing
pull instructions to users who are not authenticated.

CI tests code and builds an image without publishing it. A tagged release
delivers a versioned image to GHCR. Neither action deploys the platform:
the Kubernetes manifests continue to use their existing local
`pulseflow-ingestion:dev` image, and changing a Deployment to use a released
image requires a separate, deliberate deployment step. Publishing to GHCR
does not deploy the API, Kafka, or any other part of PulseFlow.

## Local Kubernetes with kind

This is a local-only demonstration. Install Docker, `kind`, and `kubectl`.
The single Kafka Pod uses `emptyDir` storage: its records are lost if the Pod
is recreated, and all demo Kafka data is lost when the kind cluster is deleted.
It is not a durable Kafka deployment.

From the repository root, create the single-control-plane cluster, build the
existing Java image, and load it into kind:

```bash
kind create cluster --name pulseflow --config infra/kind/kind-config.yaml --wait 5m
docker build -t pulseflow-ingestion:dev \
  -f services/ingestion-api/Dockerfile services/ingestion-api
kind load docker-image pulseflow-ingestion:dev --name pulseflow
```

Apply the namespace and Kafka resources, then wait for the broker:

```bash
kubectl apply -f infra/kubernetes/namespace.yaml
kubectl apply -f infra/kubernetes/kafka.yaml
kubectl rollout status deployment/kafka -n pulseflow --timeout=180s
kubectl get pods,services -n pulseflow
```

Create the topic with six partitions and verify it:

```bash
kubectl exec -n pulseflow deployment/kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic user-behaviour.v1 \
  --partitions 6 \
  --replication-factor 1
kubectl exec -n pulseflow deployment/kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe --topic user-behaviour.v1
```

Deploy two API replicas and wait for both to become ready:

```bash
kubectl apply -f infra/kubernetes/ingestion-api.yaml
kubectl rollout status deployment/ingestion-api -n pulseflow --timeout=180s
kubectl get pods -n pulseflow -l app=ingestion-api
```

In one terminal, forward the internal API Service to localhost:

```bash
kubectl port-forward -n pulseflow service/ingestion-api 8080:8080
```

In another terminal, use the repository-root virtual environment to send
synthetic events (no package reinstall is needed):

```bash
source .venv/bin/activate
cd python
python -m pulseflow.generator \
  --url http://localhost:8080/v1/events \
  --events 10 --rps 2 --duplicate-rate 0
```

For a subsequent API revision, build and load it with a new, unique tag, then
update the Deployment to that tag. Changing the Pod template this way triggers
a rollout; do not rebuild over an existing tag and rely on `IfNotPresent`,
which may keep using the image already cached in the kind node:

```bash
IMAGE_TAG="dev-$(date -u +%Y%m%dT%H%M%SZ)"
docker build -t "pulseflow-ingestion:${IMAGE_TAG}" \
  -f services/ingestion-api/Dockerfile services/ingestion-api
kind load docker-image "pulseflow-ingestion:${IMAGE_TAG}" --name pulseflow
kubectl set image deployment/ingestion-api -n pulseflow \
  "ingestion-api=pulseflow-ingestion:${IMAGE_TAG}"
kubectl rollout status deployment/ingestion-api -n pulseflow --timeout=180s
```

Inspect published Kafka records and their keys:

```bash
kubectl exec -n pulseflow deployment/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic user-behaviour.v1 \
  --from-beginning --max-messages 10 \
  --formatter-property print.key=true \
  --formatter-property key.separator=' | '
```

The API connects to `kafka:9092` through Kubernetes DNS. Local tools reach
the API through `kubectl port-forward`; they do not connect to Kafka at
`kafka:9092`. The Kafka and API Services are ClusterIP-only.
Kafka's single KRaft controller votes for itself at `localhost:29093`, while
the broker advertises `kafka:9092` to clients. The Kafka Service publishes its
endpoint before readiness so this single broker can resolve its advertised
address while starting; its Deployment uses `Recreate` to avoid overlapping
brokers with the same node ID.

Demonstrate API Deployment recovery by deleting one API Pod and waiting for its
replacement:

```bash
POD="$(kubectl get pods -n pulseflow -l app=ingestion-api \
  -o jsonpath='{.items[0].metadata.name}')"
kubectl delete pod -n pulseflow "$POD"
kubectl rollout status deployment/ingestion-api -n pulseflow --timeout=180s
kubectl get pods -n pulseflow -l app=ingestion-api
```

For troubleshooting, inspect Pod details/events and logs:

```bash
kubectl describe pods -n pulseflow
kubectl get events -n pulseflow --sort-by=.lastTimestamp
kubectl logs -n pulseflow deployment/kafka
kubectl logs -n pulseflow deployment/ingestion-api --all-pods=true --prefix=true
kubectl logs -n pulseflow deployment/ingestion-api --all-pods=true \
  --prefix=true --previous
```

Stop port-forwarding with Ctrl+C. Remove only the local demo namespace or
cluster when finished:

```bash
kubectl delete namespace pulseflow
# Or remove the whole local cluster:
kind delete cluster --name pulseflow
```

Deleting the namespace removes the Kafka Pod and its ephemeral `emptyDir`;
deleting the cluster removes all of its resources. Both operations discard
the local demo Kafka records. They do not affect the separate Compose
`kafka-data` volume or Spark Parquet/checkpoint data.

## Delivery and deduplication

The API does not persist request IDs or deduplicate HTTP submissions. Submitting the same event more than once, including a client retry after a timeout or lost response, can create multiple Kafka records with the same `event_id`.

Kafka producer idempotence protects against duplicate writes caused by producer retries within a producer session; it does not deduplicate separate HTTP requests or application resends. `acks=all` waits for acknowledgements from the broker's in-sync replicas, but does not confirm downstream processing.

Downstream PySpark jobs should deduplicate by `event_id`. That deduplication is only effective for as long as the job retains its deduplication state (for example, its configured watermark/checkpoint state), and only within the guarantees of the chosen sink. Events arriving after state expiry may be treated as new, and non-idempotent external side effects can still repeat. This is at-least-once delivery, not an end-to-end exactly-once guarantee.

If Kafka accepted a record but the API did not receive the acknowledgement before its three-second wait expired, the API returns HTTP 503. The result is ambiguous: the event may already be in Kafka. Retrying can therefore create a duplicate record.
