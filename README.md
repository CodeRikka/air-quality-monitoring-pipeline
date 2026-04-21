# Air Quality Monitoring Pipeline

This repository contains an end-to-end air quality data platform built around two EPA data sources:

- `AQS` as the authoritative historical and reconciliation source
- `AirNow` as the provisional near-real-time source for the most recent observation window

The project is designed to run in two environments without changing the overall architecture:

- a local single-node Ubuntu `MicroK8s` cluster for development
- a 3-node Raspberry Pi `MicroK8s` cluster that simulates production

The platform combines `PostgreSQL`, `MinIO`, `Apache Airflow`, `FastAPI`, and `Streamlit` into one deployable stack. Airflow ingests and normalizes data, PostgreSQL stores curated datasets and serving views, MinIO preserves raw inputs, FastAPI exposes query endpoints, and Streamlit provides a lightweight dashboard that only talks to the API.

## Project Overview

The main goal of this project is not simply to call an external API, but to build a repeatable data platform for ingesting, reconciling, storing, and serving air quality observations.

The core design decisions are:

- `AQS` is the system of record for historical and corrected data.
- `AirNow` fills the recent time range that has not yet appeared in AQS.
- When AQS later publishes data that overlaps with AirNow, AQS replaces the provisional AirNow version.
- Raw payloads are preserved in object storage before normalization and database loading.
- All user-facing reads go through `FastAPI`; the `Streamlit` app never queries PostgreSQL directly.

## What This Repository Deploys

The runtime is split across three Kubernetes namespaces:

- `storage`: `CloudNativePG` PostgreSQL and `MinIO`
- `airflow`: Apache Airflow, DAG sync, and task execution
- `serving`: `FastAPI` and `Streamlit`

At a high level, the platform looks like this:

1. Airflow extracts raw data from `AQS` and `AirNow`.
2. Raw files are written to `MinIO` and tracked in PostgreSQL manifest tables.
3. Transformation code normalizes source-specific payloads into a shared observation model.
4. Load steps write curated records into `raw`, `staging`, `core`, and `mart` database layers.
5. FastAPI queries the `mart` layer.
6. Streamlit renders dashboard views by calling FastAPI endpoints.

## Data Sources and Reconciliation Model

### AQS

`AQS` is the official EPA historical data source used for:

- metadata and code lists
- monitor metadata
- sample-level historical observations
- daily summary observations
- daily reconciliation using change-date windows

The AQS client and extract logic live under:

- `airflow/aq_pipeline/clients/aqs_client.py`
- `airflow/aq_pipeline/extract/aqs_extract.py`

### AirNow

`AirNow` is used as the provisional recent-data source. In this project it serves two roles:

- manual gap filling after the current AQS tail
- hourly hot-window refresh for recent observations that may be revised

The AirNow client and extract logic live under:

- `airflow/aq_pipeline/clients/airnow_client.py`
- `airflow/aq_pipeline/extract/airnow_extract.py`

### Source Priority

Source priority is fixed:

- `AQS` has higher priority
- `AirNow` is provisional

That rule is implemented in the normalization and merge path:

- `airflow/aq_pipeline/transform/normalize_aqs.py`
- `airflow/aq_pipeline/transform/normalize_airnow.py`
- `airflow/aq_pipeline/transform/merge_current.py`
- `airflow/aq_pipeline/load/load_postgres.py`

## Airflow DAGs

The repository defines five DAGs:

- `aqs_bootstrap_manual`
  - manual historical bootstrap from AQS
- `airnow_gap_bootstrap_manual`
  - manual AirNow backfill after the AQS bootstrap is complete
- `airnow_hourly_hot_sync`
  - hourly sync for the recent hot window
- `aqs_daily_reconcile`
  - daily AQS reconciliation that overwrites overlapping AirNow data
- `serving_rollups_daily`
  - daily refresh of the serving layer

A typical first-time workflow is:

1. deploy the platform
2. initialize the database
3. run `aqs_bootstrap_manual`
4. run `airnow_gap_bootstrap_manual`
5. let the scheduled DAGs maintain the current state

## Repository Layout

Key directories:

- `airflow/dags/`
  - Airflow DAG entry points
- `airflow/aq_pipeline/clients/`
  - source API clients
- `airflow/aq_pipeline/extract/`
  - raw extraction logic
- `airflow/aq_pipeline/transform/`
  - normalization and source-priority merge logic
- `airflow/aq_pipeline/load/`
  - MinIO landing, PostgreSQL upsert, and serving refresh logic
- `api/app/`
  - FastAPI application and routers
- `web/`
  - Streamlit dashboard
- `sql/`
  - schema, table, and view initialization scripts
- `configs/`
  - pollutant, region, and threshold configuration
- `infra/base/`
  - shared namespace manifests
- `infra/local/`
  - local MicroK8s manifests and helper scripts
- `infra/prod/`
  - production-simulation manifests and helper scripts
- `scripts/`
  - deploy, reset, build, and health-check helpers

## Architecture Details

### Storage and Data Layers

The database is organized into these logical layers:

- `raw`
  - manifest tables and raw-ingestion bookkeeping
- `staging`
  - source-specific intermediate tables
- `core`
  - normalized dimensions and fact tables
- `mart`
  - serving views used by the API
- `ops`
  - watermarks, pipeline runs, revisions, and data-quality audit tables

MinIO is used for raw object storage, including:

- AQS raw JSON payloads
- AirNow raw files
- pipeline artifacts
- PostgreSQL backup bucket placeholders

### Serving Layer

FastAPI exposes database-backed endpoints such as:

- `GET /healthz`
- `GET /latest/health`
- `GET /latest`
- `GET /map/latest`
- `GET /timeseries`
- `GET /quality/coverage`

The Streamlit dashboard calls only these HTTP endpoints and does not connect to PostgreSQL directly.

## Prerequisites

Before deploying, make sure you have:

- Ubuntu or another environment where `MicroK8s` is supported
- `microk8s` installed or permission to install it through `snap`
- Docker for local image builds
- network access to GitHub if you use Airflow `git-sync`
- valid credentials for:
  - `EPA_AQS_EMAIL`
  - `EPA_AQS_KEY`
  - `AIRNOW_API_KEY`

You also need a `.env` file. Start from:

```bash
cp .env.example .env
```

Then edit `.env` and provide real values for at least:

- `EPA_AQS_EMAIL`
- `EPA_AQS_KEY`
- `AIRNOW_API_KEY`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_SUPERUSER_PASSWORD`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`

Important notes:

- `.env.example` uses development-friendly defaults such as `admin` credentials. Replace them before using the stack outside a disposable environment.
- Airflow metadata DB credentials are set in `infra/local/06-airflow-values.yaml` and `infra/prod/07-airflow-values.yaml`. Keep them aligned with your PostgreSQL secrets if you change them.
- Airflow DAG sync currently pulls from `https://github.com/CodeRikka/air-quality-monitoring-pipeline.git`. If you deploy from a fork or private repository, update both the `.env` values and the Helm values files as needed.

## Local Deployment

The local environment targets a single-node MicroK8s cluster with:

- `microk8s-hostpath` storage
- local builds for `aq-api` and `aq-web`
- `imagePullPolicy: Never` for serving workloads

### 1. Bootstrap MicroK8s

Run:

```bash
bash infra/local/00-bootstrap-microk8s.sh
```

This enables:

- `dns`
- `ingress`
- `helm3`
- `metrics-server`
- `hostpath-storage`

### 2. Sync Secrets and Deploy the Base Stack

Run:

```bash
bash scripts/10-deploy-local.sh
```

This script performs the local infrastructure deployment in order:

1. create the `storage`, `airflow`, and `serving` namespaces
2. sync Kubernetes Secrets from `.env`
3. install the CloudNativePG operator
4. deploy the PostgreSQL cluster
5. deploy MinIO
6. create required MinIO buckets
7. install or upgrade Airflow through Helm
8. configure Airflow connections
9. deploy the FastAPI and Streamlit workloads

### 3. Initialize the Database

Run:

```bash
bash scripts/20-init-db.sh
```

This creates or refreshes the SQL objects from:

- `sql/001_schemas.sql`
- `sql/010_dim_tables.sql`
- `sql/015_staging_tables.sql`
- `sql/020_core_tables.sql`
- `sql/030_ops_tables.sql`
- `sql/040_mart_views.sql`

### 4. Build and Load Local Serving Images

Because local serving manifests use `imagePullPolicy: Never`, you must build and import the API and web images into MicroK8s:

```bash
bash scripts/30-build-and-load-local-images.sh
```

This builds:

- `ghcr.io/coderikka/aq-api:latest`
- `ghcr.io/coderikka/aq-web:latest`

and imports them into the local MicroK8s image store before restarting the `aq-api` and `aq-web` deployments.

### 5. Verify the Deployment

Check the core workloads:

```bash
microk8s kubectl get pods -n storage
microk8s kubectl get pods -n airflow
microk8s kubectl get pods -n serving
microk8s helm3 status airflow -n airflow
bash scripts/40-post-deploy-health-check.sh
```

If the serving pods show `ImagePullBackOff`, rerun:

```bash
bash scripts/30-build-and-load-local-images.sh
```

### 6. Access the Services

Airflow is exposed as a `NodePort` service. To inspect the assigned port:

```bash
microk8s kubectl get svc -n airflow
```

FastAPI and Streamlit are exposed internally as `ClusterIP` services. For local browser access, use port-forwarding:

```bash
microk8s kubectl port-forward -n serving svc/aq-api 8080:8080
microk8s kubectl port-forward -n serving svc/aq-web 8501:8501
```

Then open:

- FastAPI: `http://127.0.0.1:8080`
- Streamlit: `http://127.0.0.1:8501`

Current default Airflow credentials in the Helm values are:

- username: `admin`
- password: `admin`

Treat those as local-development defaults only.

### 7. Run the Initial Pipeline

After the stack is healthy:

1. open Airflow
2. trigger `aqs_bootstrap_manual`
3. wait for it to complete successfully
4. trigger `airnow_gap_bootstrap_manual`
5. confirm the scheduled DAGs are enabled:
   - `airnow_hourly_hot_sync`
   - `aqs_daily_reconcile`
   - `serving_rollups_daily`

### 8. Refresh or Reset Local State

If you need to clear pipeline state without redeploying the cluster, use:

```bash
bash scripts/21-reset-db.sh
```

By default this performs a full rebuild:

- drops and recreates application schemas
- recreates MinIO buckets
- resets Airflow metadata
- recreates Airflow connections

If you want to keep schema objects and only truncate data:

```bash
DB_RESET_MODE=truncate bash scripts/21-reset-db.sh
```

There is also a convenience helper for a one-command local refresh:

```bash
bash scripts/12-update-local-env.sh
```

This helper can:

- recreate `.env` from `.env.example`
- resync Kubernetes secrets
- reset the database
- rebuild local images
- restart Airflow pods
- run the post-deploy health check

Use it carefully: it is intentionally destructive by default.

## Production Simulation Deployment

The production-simulation environment targets a 3-node Raspberry Pi MicroK8s cluster with:

- all nodes joined as control-plane nodes
- `rook-ceph` storage
- smaller CPU and memory sizing than the local environment
- `IfNotPresent` image pull policy for serving workloads

### 1. Bootstrap Every Node

Run on each node:

```bash
bash infra/prod/00-bootstrap-microk8s.sh
```

This enables:

- `dns`
- `ingress`
- `helm3`
- `metrics-server`
- `rook-ceph`

### 2. Form the Cluster

On the primary node:

```bash
microk8s add-node
```

Run the generated join command on the other nodes. Do not add them as `--worker` nodes if you want to match the intended topology in this repository.

### 3. Prepare Secrets and Images

On the primary node:

1. copy `.env.example` to `.env`
2. set real secrets and credentials
3. ensure the serving images exist in your registry

To build and publish multi-arch images:

```bash
bash scripts/31-build-multiarch.sh
```

This publishes:

- `aq-api`
- `aq-web`
- `aq-airflow`

Current Helm values still point Airflow to the stock `apache/airflow:3.1.8` image, so the published `aq-airflow` image is not used unless you update the Helm values accordingly.

### 4. Run the Production Deploy Script

Run on the primary node:

```bash
bash scripts/11-deploy-prod.sh
```

You can also pass a custom env file path:

```bash
bash scripts/11-deploy-prod.sh /path/to/prod.env
```

The production deploy script performs:

1. namespace creation
2. secret sync from the env file
3. `rook-ceph` readiness setup
4. CloudNativePG operator install
5. PostgreSQL and MinIO deployment
6. readiness checks for PostgreSQL and MinIO endpoints
7. database initialization
8. MinIO bucket initialization
9. Airflow Helm deployment
10. Airflow connection setup
11. API and dashboard deployment
12. post-deploy health checks

## Environment Variables and Secrets

The main configuration surface is `.env`.

Frequently used variables include:

- `EPA_AQS_EMAIL`
- `EPA_AQS_KEY`
- `AIRNOW_API_KEY`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB_AIRFLOW`
- `POSTGRES_DB_AIRQUALITY`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_SUPERUSER_PASSWORD`
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_SECURE`
- `AIRFLOW_CONN_POSTGRES_ID`
- `AIRFLOW_CONN_MINIO_ID`
- `AIRFLOW_GIT_SYNC_REPO`
- `AIRFLOW_GIT_SYNC_BRANCH`

Important optional AQS controls include:

- `AQS_BOOTSTRAP_START_YEAR`
- `AQS_BOOTSTRAP_REGION_IDS`
- `AQS_BOOTSTRAP_PARAMETERS`
- `AQS_BOOTSTRAP_BDATE`
- `AQS_BOOTSTRAP_EDATE`
- `AQS_BOOTSTRAP_WINDOW_GRANULARITY`
- `AQS_RECONCILE_CBDATE`
- `AQS_RECONCILE_CEDATE`
- `AQS_REQUEST_TIMEOUT_SECONDS`
- `AQS_MIN_REQUEST_INTERVAL_SECONDS`
- `AQS_MAX_RETRIES`
- `AQS_RETRY_BACKOFF_FACTOR`

The secret sync scripts are:

- `infra/local/01-apply-secrets-from-env.sh`
- `infra/prod/01-apply-secrets-from-env.sh`

These create Kubernetes Secrets in:

- `storage`
- `airflow`
- `serving`

## Useful Operational Scripts

### Deployment

- `scripts/10-deploy-local.sh`
  - local full-stack deployment
- `scripts/11-deploy-prod.sh`
  - production-simulation deployment

### Database and State

- `scripts/20-init-db.sh`
  - apply SQL schema, table, and view definitions
- `scripts/21-reset-db.sh`
  - rebuild or truncate application state, reset buckets, and optionally reset Airflow metadata

### Images

- `scripts/30-build-and-load-local-images.sh`
  - build and import local serving images into MicroK8s
- `scripts/31-build-multiarch.sh`
  - publish multi-architecture images for registry-based deployments

### Maintenance

- `scripts/12-update-local-env.sh`
  - destructive local refresh helper
- `scripts/40-post-deploy-health-check.sh`
  - in-cluster API health validation
- `scripts/00-fix-microk8s-network-cert.sh`
  - repair MicroK8s certificate issues after network changes

## API and Dashboard Notes

The FastAPI service is implemented under `api/app/`, with routers for:

- latest observations
- latest map points
- time series
- regional coverage

The Streamlit dashboard is implemented in `web/app.py` and currently includes tabs for:

- latest map
- station time series
- regional trend
- data completeness

The API reads from PostgreSQL `mart` views, including:

- `mart.v_latest_station_observation`
- `mart.v_daily_trend`
- `mart.v_region_coverage`

## Troubleshooting

Useful commands:

```bash
microk8s kubectl get pods -n storage
microk8s kubectl get pods -n airflow
microk8s kubectl get pods -n serving
microk8s helm3 status airflow -n airflow
microk8s kubectl logs -n airflow deployment/airflow-api-server
microk8s kubectl logs -n airflow deployment/airflow-scheduler
microk8s kubectl logs -n serving deployment/aq-api
microk8s kubectl logs -n serving deployment/aq-web
microk8s kubectl exec -n airflow deployment/airflow-scheduler -- airflow connections get aq_postgres
microk8s kubectl exec -n airflow deployment/airflow-scheduler -- airflow connections get aq_minio
```

If you suspect cluster-level issues:

- verify namespaces exist: `microk8s kubectl get ns`
- verify PostgreSQL service endpoints: `microk8s kubectl get endpoints -n storage postgres-rw`
- verify MinIO service endpoints: `microk8s kubectl get endpoints -n storage minio`
- rerun the health check: `bash scripts/40-post-deploy-health-check.sh`

If MicroK8s stops working correctly after a host IP or network change:

```bash
bash scripts/00-fix-microk8s-network-cert.sh
```

## Known Caveats

- Airflow currently uses `git-sync`, so the cluster needs GitHub access unless you change the DAG delivery strategy.
- Local and production Helm values use development-style Airflow credentials by default. Harden them before any real shared deployment.
- There are no finished Ingress, domain, or TLS manifests in this repository yet. API and web access are typically done through `port-forward` or internal cluster networking.
- The production simulation assumes `rook-ceph-block` becomes available after enabling Rook Ceph. Storage setup on Raspberry Pi can require extra validation outside this repository.

## Tests

The repository includes test files under `tests/`, covering areas such as:

- DAG imports
- normalization logic
- API query behavior
- minimal end-to-end regression paths

Run your preferred Python test workflow from the repository root after installing the required dependencies for your environment.

## Key Files

If you are new to the repository, these files are the fastest way to understand the platform:

- `airflow/dags/aqs_bootstrap_manual.py`
- `airflow/dags/airnow_gap_bootstrap_manual.py`
- `airflow/dags/airnow_hourly_hot_sync.py`
- `airflow/dags/aqs_daily_reconcile.py`
- `airflow/dags/serving_rollups_daily.py`
- `airflow/aq_pipeline/extract/aqs_extract.py`
- `airflow/aq_pipeline/extract/airnow_extract.py`
- `airflow/aq_pipeline/transform/normalize_aqs.py`
- `airflow/aq_pipeline/transform/normalize_airnow.py`
- `airflow/aq_pipeline/transform/merge_current.py`
- `airflow/aq_pipeline/load/load_postgres.py`
- `api/app/main.py`
- `web/app.py`
- `scripts/10-deploy-local.sh`
- `scripts/11-deploy-prod.sh`
- `scripts/20-init-db.sh`
- `scripts/21-reset-db.sh`

## References

- [EPA AQS Data API](https://aqs.epa.gov/aqsweb/documents/data_api.html)
- [AirNow API Documentation](https://docs.airnowapi.org/)
- [AirNow FAQ](https://docs.airnowapi.org/faq)
