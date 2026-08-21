# VITHI Data Observability API

Production-grade Data Observability Backend REST API connected to AWS RDS MySQL (`metadata` DB).

## Observability Calculation Specifications

### 1. Incidents & Pipeline Failure States
- **Active Incidents**: Evaluates the latest execution run per pipeline. A pipeline has an **OPEN** incident if its latest run is in `failed` or `error` state. When a subsequent run succeeds, the incident transitions to **RESOLVED**.
- **Blast Radius**: Computed dynamically from downstream datasets registered in `obs_run_assets` with `asset_role = 'TARGET'`.

### 2. SLA-Based Freshness
- **Lag Metric**: `TIMESTAMPDIFF(MINUTE, last_updated_at, UTC_TIMESTAMP())`
- **SLA Classification**:
  - `Fresh`: $\text{lag} \le \text{SLA}$
  - `Delayed`: $\text{SLA} < \text{lag} \le 2 \times \text{SLA}$
  - `Stale`: $\text{lag} > 2 \times \text{SLA}$

### 3. Cartesian-Safe Volume Telemetry
- Source and target assets are aggregated into independent pre-aggregated CTEs per `run_id` to eliminate $M \times N$ Cartesian product skew.
- Flags volume drop anomalies when $(\text{src\_rows} - \text{tgt\_rows}) / \text{src\_rows} > 15\%$.

### 4. Temporal Schema Drift
- Tracks schema migrations across consecutive runs (Run $N$ vs Run $N-1$) for each dataset in `obs_run_columns`, isolating added columns, dropped columns, and data type changes.

### 5. Health Pillars & Zero Synthetic Assumptions
- **Freshness**: $\frac{\text{fresh\_assets}}{\text{total\_assets}} \times 100$
- **Volume**: $\frac{\text{normal\_runs}}{\text{total\_runs}} \times 100$
- **Data Quality**: Query compilation & execution success rate from `obs_run_query_history`.
- **Schema**: $\frac{\text{matching\_schema\_runs}}{\text{total\_runs}} \times 100$
- **Consistency & Uniqueness**: Explicitly marked as `N/A` (`available: false`) until custom assertion test rules are configured, ensuring the dashboard never reports artificial scores.

---

## API Endpoints

- **Root & Health:**
  - `GET /` — API welcome & documentation catalog
  - `GET /api/health` — Live database connectivity status

- **Overview APIs:**
  - `GET /api/overview/kpis` — Total pipelines, success rate, failed runs, avg latency, active incidents, period deltas
  - `GET /api/overview/charts` — Stacked runs over time, success curve, incident severity series
  - `GET /api/overview/health` — Observability pillar metrics & SLA scores
  - `GET /api/overview/recent-incidents` — Incident log with blast radius & resolution state
  - `GET /api/overview/pipeline-monitoring` — Pipeline health, runs, latency, and tools

- **Pipeline Deep-Dive:**
  - `GET /api/pipelines` — Registered pipelines
  - `GET /api/pipelines/{pipeline_id}/runs` — Historical runs with linked assets

- **Observability Deep-Dive:**
  - `GET /api/observability/volume` — Cartesian-safe source vs target row comparisons
  - `GET /api/observability/freshness` — SLA tier breakdown (Fresh / Delayed / Stale)
  - `GET /api/observability/schema` — Temporal schema drift events across runs
  - `GET /api/observability/data-quality` — Query-backed data quality check results
  - `GET /api/observability/metrics` — Calculated run metrics for dashboard consumers

- **Lineage & Diagnostics:**
  - `GET /api/lineage` — Deterministic data lineage graph
  - `GET /api/logs` — Searchable execution logs & query traces
  - `GET /api/runs/{run_id}` — Single run deep-dive with assets, columns, and query history
