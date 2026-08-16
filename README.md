# Marquez

This is a deployment-focused Marquez repository. It contains the backend service, frontend, Java
client module required by the backend, k6 load tests, Docker builds, the OpenAPI contract, and a
Helm chart.

The upstream documentation site, examples, Python client, proposals, and project-governance or
release automation are intentionally omitted.

## Repository layout

- `api/` — Marquez API, database migrations, and tests
- [`api/benchmarks/performance-history.json`](api/benchmarks/performance-history.json) — versioned,
  machine-readable benchmark history
- `api/load-testing/` — k6 OpenLineage load test and fixture-generation guide
- `clients/java/` — Java models and client code required by the API build
- `web/` — Marquez frontend and its Docker image
- `docker/` and `docker-compose*.yml` — local backend/frontend stack and image helpers
- `spec/openapi.yml` — HTTP API contract
- `chart/` — Helm chart, render-contract checks, and chart test values

## Requirements

- Docker for image builds and integration tests
- Helm 3 and `kubectl` for deployment
- Java 17 for local Gradle builds and tests
- Node.js 22 and npm for local frontend builds and tests
- k6 for load testing

## Build and test

Build the application JAR:

```bash
./gradlew :api:shadowJar
```

Run the test suite:

```bash
./gradlew test
```

The retained k6 workload and Helm-oriented instructions are in
[api/load-testing/load-testing.md](api/load-testing/load-testing.md).

Benchmark results are append-only entries in the performance history and are validated by
[performance-history.schema.json](api/benchmarks/performance-history.schema.json). Keep the exact
revision, fixture, environment, units, and correctness gates with every new result.

Build and test the frontend:

```bash
npm ci --prefix web
npm test --prefix web -- --runInBand
npm run build --prefix web
```

## Run locally with Docker Compose

Build the backend and frontend from the current source, using PostgreSQL without OpenSearch:

```bash
./docker/up.sh --build --no-search
```

The API is available on `http://localhost:5000` and the frontend on
`http://localhost:3000`. Stop the stack with:

```bash
./docker/down.sh
```

Build and publish the backend and frontend images used by Helm:

```bash
docker build -t REGISTRY/REPOSITORY:TAG .
docker build -t REGISTRY/WEB_REPOSITORY:TAG web
docker push REGISTRY/REPOSITORY:TAG
docker push REGISTRY/WEB_REPOSITORY:TAG
```

The backend image supports both modes: Helm mounts `/usr/src/app/config.yml`, while Docker Compose
uses the bundled development configuration. Set JVM overrides with `JAVA_OPTS` when needed.

## Deploy with Helm

Build the chart dependencies, then install or upgrade using the image built from this source:

```bash
helm dependency build chart

helm upgrade --install marquez chart \
  --namespace marquez \
  --create-namespace \
  --set marquez.image.registry=REGISTRY \
  --set marquez.image.repository=REPOSITORY \
  --set-string marquez.image.tag=TAG \
  --set web.image.registry=REGISTRY \
  --set web.image.repository=WEB_REPOSITORY \
  --set-string web.image.tag=TAG \
  --set postgresql.enabled=true
```

The bundled PostgreSQL chart is convenient for evaluation. For an existing PostgreSQL service,
leave `postgresql.enabled=false` and configure `marquez.db` plus
`marquez.existingSecretName` in a values file.

This snapshot chart deliberately has no default backend image tag. The durable OpenLineage queue
and migrations require an image built from the same source revision as the chart.

See [chart/README.md](chart/README.md) and [chart/values.yaml](chart/values.yaml) for deployment and
configuration details.

## Verify

```bash
bash chart/tests/render-contract.sh
helm test marquez --namespace marquez
kubectl rollout status deployment/marquez --namespace marquez
```

The render-contract check requires the chart dependencies produced by `helm dependency build`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
