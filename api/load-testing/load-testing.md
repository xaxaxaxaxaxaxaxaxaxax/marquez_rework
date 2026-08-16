# Load testing Marquez with k6

This directory contains the retained k6 workload for `POST /api/v1/lineage`. It can target a
Helm-deployed Marquez service through `kubectl port-forward` or any other reachable Marquez API.

The workload reads generated OpenLineage events from `metadata.json`, distributes them across
virtual users, checks for HTTP `201`, and fails its thresholds if the request or response error rate
reaches one percent.

![Load-testing architecture](load-testing-architecture.png)

## Requirements

- k6
- A running Marquez API
- Java 17 for local event generation, or a Docker image built from this repository

## Target a Helm deployment

Forward the Marquez service from the cluster in a separate terminal:

```bash
kubectl port-forward --namespace marquez service/marquez 5000:80
```

If the release or namespace differs, adjust the command accordingly.

## Generate OpenLineage events

Build the application and generate a local fixture:

```bash
./gradlew :api:shadowJar

java -jar api/build/libs/marquez-api-*.jar metadata \
  --jobs 100 \
  --runs-per-job 10 \
  --bytes-per-event 16384 \
  --output api/load-testing/metadata.json
```

The same fixture can be generated with the Docker image that Helm deploys:

```bash
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --volume "$PWD/api/load-testing:/load-testing" \
  REGISTRY/REPOSITORY:TAG metadata \
  --jobs 100 \
  --runs-per-job 10 \
  --bytes-per-event 16384 \
  --output /load-testing/metadata.json
```

The generated fixture is ignored by Git.

## Run the load test

```bash
MARQUEZ_URL=http://localhost:5000 \
K6_VUS=25 \
K6_DURATION=30s \
k6 run api/load-testing/http.js
```

Optional settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MARQUEZ_URL` | `http://localhost:5000` | Marquez API base URL |
| `K6_VUS` | `25` | Concurrent virtual users |
| `K6_DURATION` | `30s` | Test duration |
| `K6_SLEEP_SECONDS` | `1` | Delay between requests per virtual user |

To retain detailed and summary output:

```bash
mkdir -p api/load-testing/results

k6 run \
  --out json=api/load-testing/results/full.json \
  --summary-export=api/load-testing/results/summary.json \
  api/load-testing/http.js
```

The generated results directory is ignored by Git.

----
SPDX-License-Identifier: Apache-2.0
Copyright 2018-2023 contributors to the Marquez project.
