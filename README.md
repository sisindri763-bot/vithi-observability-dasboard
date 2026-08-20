# VITHI Data Observability API

Backend REST API for VITHI Data Observability Dashboard connected to AWS RDS MySQL (`webhooks_db`).

## Features & Endpoints

- **Overview APIs:**
  - `GET /api/health` — Health check
  - `GET /api/overview/kpis` — Total pipelines, success rate, failed runs, average duration, active incidents
  - `GET /api/overview/charts` — Pipeline runs over time, success rate over time, incidents over time
  - `GET /api/overview/health` — Data Observability pillars (Volume, Freshness, Schema)
  - `GET /api/overview/recent-incidents` — Incident log with severity breakdown
  - `GET /api/overview/pipeline-monitoring` — Per-pipeline health & telemetry metrics

- **Pipeline Deep-Dive:**
  - `GET /api/pipelines` — All unique pipelines
  - `GET /api/pipelines/{pipeline_id}/runs` — Historical execution runs for a pipeline

- **Observability Deep-Dive:**
  - `GET /api/observability/volume` — Source vs Target row count comparison & drop detection
  - `GET /api/observability/freshness` — Update delays & data staleness
  - `GET /api/observability/schema` — Source vs Target column differences & schema drift

- **Lineage & Logs:**
  - `GET /api/lineage` — Graph nodes & edges (Source Assets -> Pipeline Transform Jobs -> Target Assets)
  - `GET /api/logs` — Searchable & paginated pipeline run logs
  - `GET /api/runs/{run_id}` — Single run deep-dive with asset metadata

## Universal Filters

Every endpoint supports the following query parameters:
- `pipeline_name`, `pipeline_id`
- `status` (`success`, `failed`, `error`, `running`)
- `tool`
- `start_date`, `end_date` (YYYY-MM-DD)
- `start_time`, `end_time` (HH:MM:SS)
- `system_name`, `database_name`, `schema_name`, `object_name`

## Deployment on Vercel

Configured via `vercel.json` and `api/index.py`. Set the following environment variables in Vercel:
- `CENTRAL_DB_HOST`
- `CENTRAL_DB_PORT`
- `CENTRAL_DB_NAME`
- `CENTRAL_DB_USER`
- `CENTRAL_DB_PASSWORD`
