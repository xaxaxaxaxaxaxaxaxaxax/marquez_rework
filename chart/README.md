# Marquez Helm chart

This chart deploys the Marquez API, the optional frontend retained in this repository, and
optionally a Bitnami PostgreSQL subchart.

The chart intentionally has no default `marquez.image.tag`. The released `0.51.1` backend image
does not contain this snapshot's durable OpenLineage queue and migrations, so installation fails
until a current-source image tag is supplied.

## Prepare

Run these commands from the repository root:

```bash
docker build -t REGISTRY/REPOSITORY:TAG .
docker build -t REGISTRY/WEB_REPOSITORY:TAG web
docker push REGISTRY/REPOSITORY:TAG
docker push REGISTRY/WEB_REPOSITORY:TAG
helm dependency build chart
```

## Install

For an evaluation deployment with chart-managed PostgreSQL:

```bash
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

For an existing PostgreSQL service, create a secret whose key is `marquez-db-password`:

```bash
kubectl create namespace marquez --dry-run=client -o yaml | kubectl apply -f -
kubectl -n marquez create secret generic marquez-db \
  --from-literal=marquez-db-password='DATABASE_PASSWORD'
```

Use a private values file such as:

```yaml
marquez:
  image:
    registry: REGISTRY
    repository: REPOSITORY
    tag: TAG
  existingSecretName: marquez-db
  db:
    host: postgres.example.internal
    port: 5432
    name: marquez
    user: marquez

web:
  image:
    registry: REGISTRY
    repository: WEB_REPOSITORY
    tag: TAG

postgresql:
  enabled: false
```

Then deploy it:

```bash
helm upgrade --install marquez chart \
  --namespace marquez \
  --create-namespace \
  --values values.private.yaml
```

Do not commit the private values file or database credentials.

## Important values

| Value | Default | Purpose |
| --- | --- | --- |
| `marquez.image.registry` | `docker.io` | Backend image registry |
| `marquez.image.repository` | `marquezproject/marquez` | Backend image repository |
| `marquez.image.tag` | `""` | Required current-source backend tag |
| `marquez.migrateOnStartup` | `true` | Apply Flyway migrations before serving |
| `marquez.db.autoCommentsEnabled` | `false` | Prefix SQL with DAO method comments |
| `marquez.openLineage.workerThreads` | `8` | Concurrent projection workers per replica |
| `marquez.openLineage.projectionBatchSize` | `8` | Events projected per transaction, from 1 to 64 |
| `marquez.openLineage.maxAttempts` | `10` | Committed failures before dead-lettering |
| `marquez.terminationGracePeriodSeconds` | `90` | Pod shutdown bound |
| `web.enabled` | `true` | Deploy the independently published Web UI image |
| `postgresql.enabled` | `false` | Deploy chart-managed PostgreSQL |

All supported settings and their comments are in [values.yaml](values.yaml).

## Upgrade behavior

The backend Deployment uses the `Recreate` strategy. Upgrades stop the old projector before the
new one starts, preventing two incompatible projector versions from overlapping. Brief API
unavailability during a rollout is expected.

Durable intake requires Flyway migrations V77 through V83. If `marquez.migrateOnStartup=false`,
run the current image's `db-migrate` command before starting the new application version.

`marquez.openLineage.projectionBatchSize` is independent of the HTTP admission limit. Larger
values reduce transaction count but may hold queue and metadata locks longer; use `1` for singleton
projection.

Set `marquez.terminationGracePeriodSeconds` higher than twice
`marquez.openLineage.shutdownGracePeriodMillis` after converting milliseconds to seconds. The
defaults allow two 30-second worker waits plus lifecycle overhead.

The readiness and liveness probes use Dropwizard's aggregate `/healthcheck` endpoint. An unhealthy
OpenLineage worker removes the Pod from service and eventually causes Kubernetes to restart it;
durable queued events remain in PostgreSQL.

## Validate

Render and check the deployment contract before installing:

```bash
helm dependency build chart
bash chart/tests/render-contract.sh
helm lint chart --set-string marquez.image.tag=validation
```

After installation:

```bash
kubectl rollout status deployment/marquez --namespace marquez
helm test marquez --namespace marquez
```

For a default `ClusterIP` service, access the API locally with:

```bash
kubectl port-forward --namespace marquez service/marquez 5000:80
```

The API is then available at `http://localhost:5000/api/v1/namespaces`. If `web.enabled=true`,
forward `service/marquez-web` in the same way, using local port `3000`.

## Uninstall

```bash
helm uninstall marquez --namespace marquez
```
