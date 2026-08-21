# Data Observability Calculation Fix Plan

## Goal

Make the API calculations truthful, filter-aware, and consistent with the dashboard screens. No endpoint should return synthetic scores, duplicate incidents, or misleading empty-state values.

## Priority 1: Shared Calculation Rules

Create a shared metrics layer for formulas used by Overview and detail endpoints.

Document and reuse helpers for:

- Period boundaries and previous-period comparisons
- Incident derivation
- Freshness classification
- Volume aggregation
- Null and empty-result handling

Use UTC consistently for timestamps.

## Priority 2: Incident Modeling

Use `etl_incidents` if the table exists. Otherwise derive incidents from `obs_pipeline_runs`:

1. Group runs by `pipeline_id` and sort chronologically.
2. Open one incident when a pipeline enters `failed` or `error`.
3. Keep the incident open across consecutive failures.
4. Resolve it at the first later successful run.
5. Use the first failed run as the incident opener.
6. Calculate affected datasets from target assets linked to the incident run.

Acceptance criteria:

- Four failures from one pipeline produce one incident.
- A later success marks that incident as resolved.
- Incident filters apply before aggregation.
- Open, resolved, severity, and blast-radius counts are consistent across KPI and incident endpoints.

## Priority 3: SLA-Based Freshness

Replace the sampling-time comparison with:

```text
lag_minutes = current_utc_time - last_successful_update
```

Use a dataset or pipeline SLA field where available. Do not silently treat every dataset as having the same SLA.

Classification:

```text
Fresh   = lag_minutes <= sla_minutes
Delayed = sla_minutes < lag_minutes <= 2 * sla_minutes
Stale   = lag_minutes > 2 * sla_minutes
```

Return:

- `lag_minutes`
- `sla_minutes`
- `sla_status`
- `is_breached`
- `last_successful_update`

Missing timestamps or SLA values must return `null` status and must not count as Fresh. Empty results must return a `null` score or `N/A`, not `100%`.

## Priority 4: Volume Calculations

For each run:

1. Aggregate all SOURCE assets by `run_id`.
2. Aggregate all TARGET assets by `run_id`.
3. Join the two aggregated datasets once.
4. Apply pipeline, date, tool, and asset filters in the query.
5. Prefer object-level mappings when the metadata supports them.
6. Label run-level totals explicitly when object mappings are unavailable.

Formula:

```text
drop_percentage = ((source_rows - target_rows) / source_rows) * 100
```

Use one documented threshold consistently:

- Good: drop is at most 15%
- Warning: drop is greater than 15%
- Critical: drop is greater than 30%

Add page-level totals and current-versus-previous-period changes.

## Priority 5: Health Endpoint

Health pillars must use real checks and the same filters as their detail pages:

- Freshness: percentage of assets meeting SLA
- Volume: percentage of volume checks within threshold
- Data Quality: configured data-quality checks only
- Schema: percentage of comparable runs without schema drift
- Consistency: `N/A` until checks exist
- Uniqueness: `N/A` until checks exist

For every pillar:

```text
no records => score: null, status: N/A
```

Do not use pipeline success rate as a Data Quality score unless the product explicitly defines it that way.

## Priority 6: KPI Period Deltas

Define a current period and an equal-length previous period:

- With date filters: use the requested interval and the interval immediately before it.
- Without date filters: use a documented default, such as the last 24 hours versus the preceding 24 hours.

Calculate deltas for:

- Success rate
- Failed runs
- Average duration
- Active incidents
- Volume totals
- Freshness compliance

Return structured values where possible:

```json
{
  "value": 94.2,
  "previous_value": 91.8,
  "delta": 2.4,
  "delta_percent": 2.6
}
```

Avoid hardcoded text such as `All pipelines healthy` when the underlying metric is unavailable.

## Priority 7: Schema Drift

Add the standard pipeline, tool, and date filters to the schema endpoint.

Compare consecutive comparable successful runs for the same dataset identity. Detect:

- Added columns
- Dropped columns
- Data type changes
- Ordinal changes when relevant

Do not compare unrelated SOURCE and TARGET table names without an explicit mapping.

## Priority 8: Deterministic Lineage

Build lineage from, in order of preference:

1. `etl_pipeline_io`
2. Validated pipeline configuration mappings
3. Assets linked to pipeline runs

Apply `pipeline_id` and `pipeline_name` filters. Deduplicate nodes and edges. Do not infer relationships from names or unused configuration fields.

## Priority 9: Dedicated APIs

Add:

```text
GET /api/observability/data-quality
GET /api/observability/metrics
```

The Data Quality endpoint should expose check totals, pass/fail counts, check type, dataset, run, and failure details. The Metrics endpoint should expose the shared calculated values used by Overview and detail pages.

## Validation Plan

Add focused tests for:

- Multiple failures from one pipeline
- Failure followed by success
- Missing freshness timestamps
- Different SLA values
- Empty datasets
- Multiple SOURCE and TARGET assets
- Volume filters
- Schema additions, removals, and type changes
- Previous-period delta calculations
- Duplicate lineage edges

Run the basic syntax check:

```powershell
python -m py_compile main.py api\index.py
```

Then run the focused API test suite with `pytest` and FastAPI's test client.

## Recommended Delivery Order

1. Shared metrics and period helpers
2. Incident deduplication
3. Freshness SLA logic
4. Volume filters and totals
5. Health endpoint
6. KPI deltas
7. Schema filters
8. Lineage filters and deduplication
9. Data Quality and Metrics endpoints
10. Tests and README formula updates
