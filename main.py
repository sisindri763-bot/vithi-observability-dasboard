import os
import json
import pymysql
from decimal import Decimal
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

HOST     = os.getenv("CENTRAL_DB_HOST", "")
PORT     = int(os.getenv("CENTRAL_DB_PORT", "3306"))
USER     = os.getenv("CENTRAL_DB_USER", "")
PASSWORD = os.getenv("CENTRAL_DB_PASSWORD", "")
DB_NAME  = os.getenv("CENTRAL_DB_NAME", "")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="VITHI Data Observability API",
    description=(
        "Backend REST API for VITHI Data Observability Dashboard.\n\n"
        "Connected to AWS RDS MySQL `webhooks_db`.\n\n"
        "**Tables used:** `pipeline_runs`, `source_asset_metadata`, `target_asset_metadata`\n\n"
        "**Universal filters supported on every endpoint:**\n"
        "`pipeline_name`, `pipeline_id`, `status`, `tool`, "
        "`start_date`, `end_date`, `start_time`, `end_time`, "
        "`system_name`, `database_name`, `schema_name`, `object_name`"
    ),
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# JSON encoder — handles datetime, Decimal, bytes
# ---------------------------------------------------------------------------
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj) if "." in str(obj) else int(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)

def jsonify(data):
    return json.loads(json.dumps(data, cls=CustomEncoder))

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
    try:
        return pymysql.connect(
            host=HOST, port=PORT, user=USER, password=PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {e}")


def query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cleaned = []
            for row in rows:
                cleaned.append({
                    k: (float(v) if "." in str(v) else int(v)) if isinstance(v, Decimal) else v
                    for k, v in row.items()
                })
            return cleaned
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Universal filter builder
# ---------------------------------------------------------------------------
def build_run_filters(
    pipeline_name: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    status: Optional[str] = None,
    tool: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
):
    """
    Builds WHERE clause fragments + params for pipeline_runs table.
    All params optional. Multiple status values supported comma-separated.
    Date + time are combined for exact datetime filtering.
    """
    clauses, params = [], []

    if pipeline_name:
        names = [n.strip() for n in pipeline_name.split(",")]
        placeholders = ",".join(["%s"] * len(names))
        clauses.append(f"pipeline_name IN ({placeholders})")
        params.extend(names)

    if pipeline_id:
        ids = [i.strip() for i in pipeline_id.split(",")]
        placeholders = ",".join(["%s"] * len(ids))
        clauses.append(f"pipeline_id IN ({placeholders})")
        params.extend(ids)

    if status:
        statuses = [s.strip().lower() for s in status.split(",")]
        placeholders = ",".join(["%s"] * len(statuses))
        clauses.append(f"LOWER(status) IN ({placeholders})")
        params.extend(statuses)

    if tool:
        tools = [t.strip().lower() for t in tool.split(",")]
        placeholders = ",".join(["%s"] * len(tools))
        clauses.append(f"LOWER(tool_name) IN ({placeholders})")
        params.extend(tools)

    # Combine date + time into full datetime string
    start_dt = None
    end_dt = None
    if start_date:
        start_dt = f"{start_date} {start_time}" if start_time else f"{start_date} 00:00:00"
    if end_date:
        end_dt = f"{end_date} {end_time}" if end_time else f"{end_date} 23:59:59"

    if start_dt:
        clauses.append("COALESCE(start_time, saved_at) >= %s")
        params.append(start_dt)
    if end_dt:
        clauses.append("COALESCE(start_time, saved_at) <= %s")
        params.append(end_dt)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, tuple(params)


def build_meta_filters(
    system_name: Optional[str] = None,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    object_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    run_ids: Optional[List[str]] = None,
    alias: str = "m",
):
    """
    Builds WHERE clause fragments for source/target_asset_metadata tables.
    """
    clauses, params = [], []

    if system_name:
        names = [n.strip() for n in system_name.split(",")]
        placeholders = ",".join(["%s"] * len(names))
        clauses.append(f"{alias}.system_name IN ({placeholders})")
        params.extend(names)

    if database_name:
        dbs = [d.strip() for d in database_name.split(",")]
        placeholders = ",".join(["%s"] * len(dbs))
        clauses.append(f"{alias}.database_name IN ({placeholders})")
        params.extend(dbs)

    if schema_name:
        schemas = [s.strip() for s in schema_name.split(",")]
        placeholders = ",".join(["%s"] * len(schemas))
        clauses.append(f"{alias}.schema_name IN ({placeholders})")
        params.extend(schemas)

    if object_name:
        objects = [o.strip() for o in object_name.split(",")]
        placeholders = ",".join(["%s"] * len(objects))
        clauses.append(f"{alias}.object_name IN ({placeholders})")
        params.extend(objects)

    start_dt = None
    end_dt = None
    if start_date:
        start_dt = f"{start_date} {start_time}" if start_time else f"{start_date} 00:00:00"
    if end_date:
        end_dt = f"{end_date} {end_time}" if end_time else f"{end_date} 23:59:59"

    if start_dt:
        clauses.append(f"{alias}.observed_at >= %s")
        params.append(start_dt)
    if end_dt:
        clauses.append(f"{alias}.observed_at <= %s")
        params.append(end_dt)

    if run_ids:
        placeholders = ",".join(["%s"] * len(run_ids))
        clauses.append(f"{alias}.run_id IN ({placeholders})")
        params.extend(run_ids)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, tuple(params)


# Helper: get matching run_ids from pipeline_runs given run filters
def get_matching_run_ids(
    pipeline_name=None, pipeline_id=None, status=None, tool=None,
    start_date=None, end_date=None, start_time=None, end_time=None,
):
    where, params = build_run_filters(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )
    rows = query(f"SELECT id FROM pipeline_runs {where}", params)
    return [r["id"] for r in rows]


# =============================================================================
# COMMON QUERY PARAM DEPENDENCY DOCSTRINGS (used across all endpoints)
# =============================================================================
_PIPELINE_NAME_DESC = "Filter by pipeline name(s). Comma-separated for multiple values."
_PIPELINE_ID_DESC   = "Filter by pipeline ID(s). Comma-separated for multiple values."
_STATUS_DESC        = "Filter by status. Options: success, failed, error, running. Comma-separated."
_TOOL_DESC          = "Filter by tool name (e.g. dbt). Comma-separated."
_START_DATE_DESC    = "Start date filter (YYYY-MM-DD)."
_END_DATE_DESC      = "End date filter (YYYY-MM-DD)."
_START_TIME_DESC    = "Start time filter (HH:MM:SS). Combined with start_date."
_END_TIME_DESC      = "End time filter (HH:MM:SS). Combined with end_date."
_SYSTEM_NAME_DESC   = "Filter by system name (e.g. Snowflake, MySQL). Comma-separated."
_DB_NAME_DESC       = "Filter by database name. Comma-separated."
_SCHEMA_NAME_DESC   = "Filter by schema name. Comma-separated."
_OBJECT_NAME_DESC   = "Filter by object/table name. Comma-separated."

# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/api/health", tags=["Health"], summary="API & DB health check")
def api_health():
    """Check API is alive and database is reachable."""
    query("SELECT 1")
    return {
        "status": "ok",
        "database": "connected",
        "timestamp": datetime.now().isoformat(),
    }


# =============================================================================
# OVERVIEW — KPIs
# =============================================================================

@app.get("/api/overview/kpis", tags=["Overview"], summary="KPI summary cards")
def get_overview_kpis(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns the 5 KPI summary cards for the dashboard Overview page:
    - Total distinct pipelines
    - Success rate %
    - Failed runs count
    - Average pipeline duration
    - Active incidents count
    
    Also returns a sparkline path string from recent run durations.
    All universal filters apply.
    """
    where, params = build_run_filters(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    # Run stats
    stats = query(f"""
        SELECT
            COUNT(DISTINCT pipeline_id)                                                    AS total_pipelines,
            COUNT(*)                                                                       AS total_runs,
            SUM(CASE WHEN LOWER(status) = 'success' THEN 1 ELSE 0 END)                    AS success_runs,
            SUM(CASE WHEN LOWER(status) IN ('failed','error') THEN 1 ELSE 0 END)          AS failed_runs,
            SUM(CASE WHEN LOWER(status) = 'running' THEN 1 ELSE 0 END)                    AS running_runs,
            AVG(COALESCE(duration, 0))                                                     AS avg_duration
        FROM pipeline_runs {where}
    """, params)

    s = stats[0] if stats else {}
    total_pipelines = int(s.get("total_pipelines") or 0)
    total_runs      = int(s.get("total_runs") or 0)
    success_runs    = int(s.get("success_runs") or 0)
    failed_runs     = int(s.get("failed_runs") or 0)
    running_runs    = int(s.get("running_runs") or 0)
    avg_dur         = float(s.get("avg_duration") or 0)

    success_rate = round(success_runs / total_runs * 100, 1) if total_runs > 0 else 0.0
    active_incidents = failed_runs + running_runs

    # Duration string
    avg_dur_int = int(round(avg_dur))
    if avg_dur_int >= 60:
        duration_str = f"{avg_dur_int // 60}m {avg_dur_int % 60}s"
    else:
        duration_str = f"{avg_dur_int}s"

    # Sparkline from last 10 runs
    sparkline_rows = query(f"""
        SELECT COALESCE(duration, 0) AS duration
        FROM pipeline_runs {where}
        ORDER BY COALESCE(start_time, saved_at) DESC
        LIMIT 10
    """, params)
    durations = [int(r["duration"] or 0) for r in reversed(sparkline_rows)] or [0]
    max_d = max(durations) if max(durations) > 0 else 1
    step  = 100 / max(1, len(durations) - 1) if len(durations) > 1 else 100
    points = [f"{int(i * step)},{int(30 - (d / max_d * 20))}" for i, d in enumerate(durations)]
    sparkline = "M" + " L".join(points)

    return jsonify({
        "filters_applied": {
            "pipeline_name": pipeline_name, "pipeline_id": pipeline_id,
            "status": status, "tool": tool,
            "start_date": start_date, "end_date": end_date,
            "start_time": start_time, "end_time": end_time,
        },
        "kpis": {
            "total_pipelines":  {"value": total_pipelines, "label": "Total Pipelines"},
            "success_rate":     {"value": f"{success_rate}%", "label": "Successful Runs", "raw": success_rate},
            "failed_runs":      {"value": failed_runs, "label": "Failed Runs"},
            "avg_duration":     {"value": duration_str, "label": "Avg Pipeline Duration", "raw_seconds": avg_dur_int},
            "active_incidents": {"value": active_incidents, "label": "Active Incidents"},
        },
        "summary": {
            "total_runs": total_runs,
            "success_runs": success_runs,
            "failed_runs": failed_runs,
            "running_runs": running_runs,
        },
        "sparkline": sparkline,
    })


# =============================================================================
# OVERVIEW — CHARTS
# =============================================================================

@app.get("/api/overview/charts", tags=["Overview"], summary="Time-series chart data")
def get_overview_charts(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns time-series chart data:
    - Pipeline Runs Over Time (success / failed / running / cancelled stacked bars)
    - Pipeline Success Rate Over Time (line chart %)
    - Incidents Over Time (high / medium / low severity bars)
    
    All universal filters apply.
    """
    where, params = build_run_filters(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    rows = query(f"""
        SELECT
            DATE_FORMAT(COALESCE(start_time, saved_at), '%%b %%d %%H:00')                 AS time_label,
            MIN(COALESCE(start_time, saved_at))                                            AS sort_ts,
            SUM(CASE WHEN LOWER(status) = 'success' THEN 1 ELSE 0 END)                    AS success_cnt,
            SUM(CASE WHEN LOWER(status) IN ('failed','error') THEN 1 ELSE 0 END)          AS failed_cnt,
            SUM(CASE WHEN LOWER(status) = 'running' THEN 1 ELSE 0 END)                    AS running_cnt,
            SUM(CASE WHEN LOWER(status) NOT IN ('success','failed','error','running') THEN 1 ELSE 0 END) AS cancelled_cnt,
            COUNT(*)                                                                        AS total_cnt
        FROM pipeline_runs {where}
        GROUP BY time_label
        ORDER BY sort_ts ASC
    """, params)

    if not rows:
        return jsonify({
            "labels": [], "runs_over_time": {}, "success_rate_over_time": [], "incidents_over_time": {}
        })

    labels          = [r["time_label"] or "Run" for r in rows]
    success_arr     = [int(r["success_cnt"] or 0) for r in rows]
    failed_arr      = [int(r["failed_cnt"] or 0) for r in rows]
    running_arr     = [int(r["running_cnt"] or 0) for r in rows]
    cancelled_arr   = [int(r["cancelled_cnt"] or 0) for r in rows]
    total_arr       = [int(r["total_cnt"] or 1) for r in rows]
    success_rate_arr = [round(s / t * 100, 1) for s, t in zip(success_arr, total_arr)]

    # Incident severity: high=failed/error, medium=runs with partial failure, low=running issues
    high_arr   = failed_arr
    medium_arr = [1 if f > 0 else 0 for f in failed_arr]
    low_arr    = [1 if r > 0 else 0 for r in running_arr]

    return jsonify({
        "filters_applied": {
            "pipeline_name": pipeline_name, "start_date": start_date, "end_date": end_date,
        },
        "labels": labels,
        "runs_over_time": {
            "success": success_arr, "failed": failed_arr,
            "running": running_arr, "cancelled": cancelled_arr,
        },
        "success_rate_over_time": success_rate_arr,
        "incidents_over_time": {
            "high": high_arr, "medium": medium_arr, "low": low_arr,
        },
    })


# =============================================================================
# OVERVIEW — DATA OBSERVABILITY HEALTH
# =============================================================================

@app.get("/api/overview/health", tags=["Overview"], summary="Observability health scores")
def get_overview_health(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns 3 computable observability health pillars derived from actual DB data:
    
    - **Volume**: Source row count vs target row count comparison (drop/growth %).
    - **Freshness**: How recently the data was last updated (hours since last_updated_at).
    - **Schema**: Column count match between source and target per run.
    
    All universal filters apply.
    """
    run_ids = get_matching_run_ids(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    meta_where_s, meta_params_s = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
        run_ids=run_ids if run_ids else None,
        alias="s",
    )
    meta_where_t, meta_params_t = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
        run_ids=run_ids if run_ids else None,
        alias="t",
    )

    # --- Volume ---
    vol_src = query(f"""
        SELECT AVG(row_count) AS avg_rows, SUM(row_count) AS total_rows
        FROM source_asset_metadata s {meta_where_s}
    """, meta_params_s)
    vol_tgt = query(f"""
        SELECT AVG(row_count) AS avg_rows, SUM(row_count) AS total_rows
        FROM target_asset_metadata t {meta_where_t}
    """, meta_params_t)

    src_rows = float(vol_src[0]["total_rows"] or 0) if vol_src else 0
    tgt_rows = float(vol_tgt[0]["total_rows"] or 0) if vol_tgt else 0
    volume_drop_pct = round((1 - tgt_rows / src_rows) * 100, 1) if src_rows > 0 else 0
    volume_score = max(0, round(100 - abs(volume_drop_pct), 1))

    # --- Freshness ---
    freshness_rows = query(f"""
        SELECT
            MAX(last_updated_at) AS latest_update,
            AVG(TIMESTAMPDIFF(HOUR, last_updated_at, observed_at)) AS avg_delay_hours
        FROM source_asset_metadata s {meta_where_s}
    """, meta_params_s)
    fr = freshness_rows[0] if freshness_rows else {}
    avg_delay_hours = float(fr.get("avg_delay_hours") or 0)
    latest_update   = fr.get("latest_update")
    freshness_score = max(0, round(100 - min(avg_delay_hours * 5, 100), 1))

    # --- Schema ---
    schema_rows = query(f"""
        SELECT
            s.run_id,
            s.column_count AS src_cols,
            t.column_count AS tgt_cols,
            s.column_names AS src_col_names,
            t.column_names AS tgt_col_names
        FROM source_asset_metadata s
        JOIN target_asset_metadata t ON s.run_id = t.run_id
        {('WHERE ' + ' AND '.join([
            c.replace('s.', 's.').replace('t.', 't.')
            for c in (meta_where_s.replace('WHERE ','').split(' AND ') if meta_where_s else [])
        ])) if meta_where_s else ''}
    """, meta_params_s)

    total_schema_checks  = len(schema_rows)
    schema_match_count   = sum(1 for r in schema_rows if r["src_cols"] == r["tgt_cols"])
    schema_score = round(schema_match_count / total_schema_checks * 100, 1) if total_schema_checks > 0 else 100.0

    def score_label(score):
        if score >= 90: return "Good"
        if score >= 70: return "Warning"
        return "Critical"

    return jsonify({
        "filters_applied": {
            "pipeline_name": pipeline_name, "database_name": database_name,
            "start_date": start_date, "end_date": end_date,
        },
        "health_pillars": {
            "volume": {
                "score": volume_score,
                "label": score_label(volume_score),
                "details": {
                    "source_total_rows": int(src_rows),
                    "target_total_rows": int(tgt_rows),
                    "volume_drop_pct": volume_drop_pct,
                }
            },
            "freshness": {
                "score": freshness_score,
                "label": score_label(freshness_score),
                "details": {
                    "avg_delay_hours": round(avg_delay_hours, 2),
                    "latest_update": latest_update,
                }
            },
            "schema": {
                "score": schema_score,
                "label": score_label(schema_score),
                "details": {
                    "total_checks": total_schema_checks,
                    "matched": schema_match_count,
                    "mismatched": total_schema_checks - schema_match_count,
                    "runs": jsonify(schema_rows),
                }
            },
        },
    })


# =============================================================================
# OVERVIEW — RECENT INCIDENTS
# =============================================================================

@app.get("/api/overview/recent-incidents", tags=["Overview"], summary="Recent incident feed")
def get_recent_incidents(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
    limit:         int           = Query(20, description="Max number of incidents to return (default 20)"),
):
    """
    Returns recent pipeline incidents (failed / error runs) with:
    - Severity (high = error/failed with message, medium = failed without detail, low = running stuck)
    - Error message
    - Pipeline name, tool, start/end time
    
    All universal filters apply. If status not specified, defaults to failed + error runs only.
    """
    # Default to failed/error if no status filter
    effective_status = status if status else "failed,error"
    where, params = build_run_filters(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=effective_status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    rows = query(f"""
        SELECT
            id, pipeline_id, pipeline_name, status, tool_name,
            start_time, end_time, duration, error_message,
            triggered_by, execution_mode, saved_at
        FROM pipeline_runs {where}
        ORDER BY COALESCE(start_time, saved_at) DESC
        LIMIT %s
    """, params + (limit,))

    incidents = []
    for r in rows:
        has_message = bool(r.get("error_message"))
        if r["status"].lower() == "error" and has_message:
            severity = "high"
        elif r["status"].lower() in ("failed", "error"):
            severity = "medium"
        else:
            severity = "low"

        incidents.append({
            "id": r["id"],
            "pipeline_id": r["pipeline_id"],
            "pipeline_name": r["pipeline_name"],
            "status": r["status"],
            "tool": r["tool_name"],
            "severity": severity,
            "error_message": r["error_message"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "duration_seconds": r["duration"],
            "triggered_by": r["triggered_by"],
            "execution_mode": r["execution_mode"],
            "saved_at": r["saved_at"],
        })

    return jsonify({
        "filters_applied": {
            "pipeline_name": pipeline_name, "status": effective_status,
            "start_date": start_date, "end_date": end_date,
        },
        "total": len(incidents),
        "incidents": incidents,
    })


# =============================================================================
# OVERVIEW — PIPELINE MONITORING TABLE
# =============================================================================

@app.get("/api/overview/pipeline-monitoring", tags=["Overview"], summary="Pipeline monitoring table")
def get_pipeline_monitoring(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns pipeline monitoring table rows showing per-pipeline:
    - Pipeline name, latest status, total runs, success rate %, average duration
    
    All universal filters apply.
    """
    where, params = build_run_filters(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    rows = query(f"""
        SELECT
            pipeline_id,
            pipeline_name,
            tool_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN LOWER(status) = 'success' THEN 1 ELSE 0 END) AS success_runs,
            SUM(CASE WHEN LOWER(status) IN ('failed','error') THEN 1 ELSE 0 END) AS failed_runs,
            AVG(COALESCE(duration, 0)) AS avg_duration,
            MAX(COALESCE(start_time, saved_at)) AS last_run_at,
            SUBSTRING_INDEX(GROUP_CONCAT(status ORDER BY COALESCE(start_time, saved_at) DESC), ',', 1) AS latest_status
        FROM pipeline_runs {where}
        GROUP BY pipeline_id, pipeline_name, tool_name
        ORDER BY last_run_at DESC
    """, params)

    pipelines = []
    for r in rows:
        total  = int(r["total_runs"] or 0)
        succ   = int(r["success_runs"] or 0)
        sr     = round(succ / total * 100, 1) if total > 0 else 0.0
        avg_d  = int(round(float(r["avg_duration"] or 0)))
        dur_str = f"{avg_d // 60}m {avg_d % 60}s" if avg_d >= 60 else f"{avg_d}s"

        pipelines.append({
            "pipeline_id":    r["pipeline_id"],
            "pipeline_name":  r["pipeline_name"],
            "tool":           r["tool_name"],
            "latest_status":  r["latest_status"],
            "total_runs":     total,
            "success_runs":   succ,
            "failed_runs":    int(r["failed_runs"] or 0),
            "success_rate":   sr,
            "avg_duration":   dur_str,
            "avg_duration_seconds": avg_d,
            "last_run_at":    r["last_run_at"],
        })

    return jsonify({
        "filters_applied": {
            "pipeline_name": pipeline_name, "status": status,
            "start_date": start_date, "end_date": end_date,
        },
        "total": len(pipelines),
        "pipelines": pipelines,
    })


# =============================================================================
# PIPELINES — LIST & PER-PIPELINE RUNS
# =============================================================================

@app.get("/api/pipelines", tags=["Pipelines"], summary="All pipelines aggregated")
def get_pipelines(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns all unique pipelines with aggregated statistics and most recent run metadata.
    All universal filters apply.
    """
    # Reuse pipeline monitoring endpoint logic
    return get_pipeline_monitoring(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
    )


@app.get("/api/pipelines/{pid}/runs", tags=["Pipelines"], summary="All runs for a specific pipeline")
def get_pipeline_runs(
    pid: str,
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
    limit:         int           = Query(50, description="Max runs to return"),
):
    """
    Returns all execution runs for a given pipeline ID (`pid`), with source and target
    asset metadata joined per run. All universal filters apply.
    """
    where, params = build_run_filters(
        pipeline_id=pid,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    runs = query(f"""
        SELECT
            id, pipeline_id, pipeline_name, status, tool_name,
            start_time, end_time, duration,
            rows_read, rows_written,
            error_message, execution_mode, triggered_by, saved_at
        FROM pipeline_runs {where}
        ORDER BY COALESCE(start_time, saved_at) DESC
        LIMIT %s
    """, params + (limit,))

    if not runs:
        return jsonify({"pipeline_id": pid, "total": 0, "runs": []})

    run_ids = [r["id"] for r in runs]
    meta_where_s, meta_params_s = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=run_ids, alias="s",
    )
    meta_where_t, meta_params_t = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=run_ids, alias="t",
    )

    src_meta = query(f"SELECT * FROM source_asset_metadata s {meta_where_s}", meta_params_s)
    tgt_meta = query(f"SELECT * FROM target_asset_metadata t {meta_where_t}", meta_params_t)

    src_by_run = {r["run_id"]: r for r in src_meta}
    tgt_by_run = {r["run_id"]: r for r in tgt_meta}

    result = []
    for r in runs:
        result.append({
            **r,
            "source_asset": src_by_run.get(r["id"]),
            "target_asset": tgt_by_run.get(r["id"]),
        })

    return jsonify({
        "pipeline_id": pid,
        "pipeline_name": runs[0]["pipeline_name"] if runs else None,
        "total": len(result),
        "runs": result,
    })


# =============================================================================
# OBSERVABILITY — VOLUME
# =============================================================================

@app.get("/api/observability/volume", tags=["Data Observability"], summary="Volume health — source vs target row counts")
def get_volume(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns per-run volume comparison between source and target:
    - Source row count, target row count, row drop/growth count and %
    - Volume health score per run
    
    All universal filters apply.
    """
    run_ids = get_matching_run_ids(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    meta_where, meta_params = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=run_ids if run_ids else None, alias="s",
    )

    rows = query(f"""
        SELECT
            s.run_id,
            s.database_name AS src_database, s.schema_name AS src_schema, s.object_name AS src_object,
            s.row_count AS src_rows, s.size_bytes AS src_bytes,
            t.database_name AS tgt_database, t.schema_name AS tgt_schema, t.object_name AS tgt_object,
            t.row_count AS tgt_rows, t.size_bytes AS tgt_bytes,
            s.observed_at
        FROM source_asset_metadata s
        LEFT JOIN target_asset_metadata t ON s.run_id = t.run_id
        {meta_where}
        ORDER BY s.observed_at DESC
    """, meta_params)

    result = []
    for r in rows:
        src = float(r["src_rows"] or 0)
        tgt = float(r["tgt_rows"] or 0)
        diff = tgt - src
        drop_pct = round((1 - tgt / src) * 100, 1) if src > 0 else 0
        score = max(0, round(100 - abs(drop_pct), 1))
        result.append({
            **r,
            "row_diff": int(diff),
            "volume_drop_pct": drop_pct,
            "volume_score": score,
            "volume_status": "Good" if score >= 90 else "Warning" if score >= 70 else "Critical",
        })

    return jsonify({
        "filters_applied": {"pipeline_name": pipeline_name, "database_name": database_name,
                             "start_date": start_date, "end_date": end_date},
        "total": len(result),
        "volume_checks": result,
    })


# =============================================================================
# OBSERVABILITY — FRESHNESS
# =============================================================================

@app.get("/api/observability/freshness", tags=["Data Observability"], summary="Freshness health — data staleness")
def get_freshness(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns freshness health per asset:
    - Last updated timestamp
    - Hours since last update (delay)
    - Freshness score (100 = fresh, lower = stale)
    
    All universal filters apply.
    """
    run_ids = get_matching_run_ids(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    meta_where, meta_params = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=run_ids if run_ids else None, alias="m",
    )

    rows = query(f"""
        SELECT
            m.run_id, m.system_name, m.database_name, m.schema_name, m.object_name,
            m.last_updated_at, m.observed_at,
            TIMESTAMPDIFF(HOUR, m.last_updated_at, m.observed_at) AS delay_hours,
            TIMESTAMPDIFF(HOUR, m.last_updated_at, NOW()) AS hours_since_update
        FROM source_asset_metadata m {meta_where}
        ORDER BY m.observed_at DESC
    """, meta_params)

    result = []
    for r in rows:
        delay = float(r["delay_hours"] or 0)
        score = max(0, round(100 - min(delay * 5, 100), 1))
        result.append({
            **r,
            "freshness_score": score,
            "freshness_status": "Good" if score >= 90 else "Warning" if score >= 70 else "Critical",
        })

    return jsonify({
        "filters_applied": {"pipeline_name": pipeline_name, "database_name": database_name,
                             "start_date": start_date, "end_date": end_date},
        "total": len(result),
        "freshness_checks": result,
    })


# =============================================================================
# OBSERVABILITY — SCHEMA
# =============================================================================

@app.get("/api/observability/schema", tags=["Data Observability"], summary="Schema health — column diff source vs target")
def get_schema(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns schema diff per run:
    - Source columns vs target columns
    - Added/removed columns detected by name comparison
    - Schema match score per run
    
    All universal filters apply.
    """
    run_ids = get_matching_run_ids(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    meta_where, meta_params = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=run_ids if run_ids else None, alias="s",
    )

    rows = query(f"""
        SELECT
            s.run_id,
            s.database_name AS src_database, s.schema_name AS src_schema, s.object_name AS src_object,
            s.column_count AS src_col_count, s.column_names AS src_col_names,
            t.database_name AS tgt_database, t.schema_name AS tgt_schema, t.object_name AS tgt_object,
            t.column_count AS tgt_col_count, t.column_names AS tgt_col_names,
            s.observed_at
        FROM source_asset_metadata s
        LEFT JOIN target_asset_metadata t ON s.run_id = t.run_id
        {meta_where}
        ORDER BY s.observed_at DESC
    """, meta_params)

    result = []
    for r in rows:
        src_cols = set(c.strip() for c in (r["src_col_names"] or "").split(",") if c.strip())
        tgt_cols = set(c.strip() for c in (r["tgt_col_names"] or "").split(",") if c.strip())

        added    = sorted(tgt_cols - src_cols)
        removed  = sorted(src_cols - tgt_cols)
        matched  = sorted(src_cols & tgt_cols)
        col_match = len(src_cols) == len(tgt_cols) and not added and not removed

        score = 100.0 if col_match else max(0, round(len(matched) / max(len(src_cols), 1) * 100, 1))

        result.append({
            "run_id":        r["run_id"],
            "src_database":  r["src_database"],
            "src_schema":    r["src_schema"],
            "src_object":    r["src_object"],
            "tgt_database":  r["tgt_database"],
            "tgt_schema":    r["tgt_schema"],
            "tgt_object":    r["tgt_object"],
            "src_col_count": r["src_col_count"],
            "tgt_col_count": r["tgt_col_count"],
            "columns_added":   added,
            "columns_removed": removed,
            "columns_matched": matched,
            "schema_match":    col_match,
            "schema_score":    score,
            "schema_status":   "Good" if score >= 90 else "Warning" if score >= 70 else "Critical",
            "observed_at":   r["observed_at"],
        })

    return jsonify({
        "filters_applied": {"pipeline_name": pipeline_name, "database_name": database_name,
                             "start_date": start_date, "end_date": end_date},
        "total": len(result),
        "schema_checks": result,
    })


# =============================================================================
# LINEAGE
# =============================================================================

@app.get("/api/lineage", tags=["Lineage"], summary="End-to-end data lineage graph")
def get_lineage(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns lineage graph nodes and edges:
    - Nodes: source assets, pipeline runs (transformation jobs), target assets
    - Edges: source → run, run → target
    
    All universal filters apply.
    """
    where, params = build_run_filters(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    runs = query(f"""
        SELECT id, pipeline_id, pipeline_name, status, tool_name, start_time, saved_at
        FROM pipeline_runs {where}
    """, params)

    if not runs:
        return jsonify({"nodes": [], "edges": [], "total_runs": 0})

    run_ids = [r["id"] for r in runs]
    meta_where_s, meta_params_s = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=run_ids, alias="s",
    )
    meta_where_t, meta_params_t = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=run_ids, alias="t",
    )

    src_meta = query(f"SELECT * FROM source_asset_metadata s {meta_where_s}", meta_params_s)
    tgt_meta = query(f"SELECT * FROM target_asset_metadata t {meta_where_t}", meta_params_t)

    nodes, edges, seen_nodes = [], [], set()

    def add_node(nid, ntype, label, meta=None):
        if nid not in seen_nodes:
            seen_nodes.add(nid)
            nodes.append({"id": nid, "type": ntype, "label": label, "meta": meta or {}})

    for r in runs:
        run_node_id = f"run_{r['id']}"
        add_node(run_node_id, "pipeline_run", r["pipeline_name"], {
            "status": r["status"], "tool": r["tool_name"],
            "pipeline_id": r["pipeline_id"],
            "run_time": str(r["start_time"] or r["saved_at"]),
        })

    for s in src_meta:
        src_id = f"src_{s['database_name']}_{s['schema_name']}_{s['object_name']}"
        add_node(src_id, "source_asset",
                 f"{s['database_name']}.{s['schema_name']}.{s['object_name']}",
                 {"system": s["system_name"], "rows": s["row_count"], "cols": s["column_count"]})
        edges.append({"from": src_id, "to": f"run_{s['run_id']}", "label": "feeds"})

    for t in tgt_meta:
        tgt_id = f"tgt_{t['database_name']}_{t['schema_name']}_{t['object_name']}"
        add_node(tgt_id, "target_asset",
                 f"{t['database_name']}.{t['schema_name']}.{t['object_name']}",
                 {"system": t["system_name"], "rows": t["row_count"], "cols": t["column_count"]})
        edges.append({"from": f"run_{t['run_id']}", "to": tgt_id, "label": "produces"})

    return jsonify({
        "filters_applied": {"pipeline_name": pipeline_name, "status": status,
                             "start_date": start_date, "end_date": end_date},
        "total_runs": len(runs),
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "nodes": nodes,
        "edges": edges,
    })


# =============================================================================
# LOGS
# =============================================================================

@app.get("/api/logs", tags=["Logs"], summary="Pipeline execution logs")
def get_logs(
    pipeline_name: Optional[str] = Query(None, description=_PIPELINE_NAME_DESC),
    pipeline_id:   Optional[str] = Query(None, description=_PIPELINE_ID_DESC),
    status:        Optional[str] = Query(None, description=_STATUS_DESC),
    tool:          Optional[str] = Query(None, description=_TOOL_DESC),
    start_date:    Optional[str] = Query(None, description=_START_DATE_DESC),
    end_date:      Optional[str] = Query(None, description=_END_DATE_DESC),
    start_time:    Optional[str] = Query(None, description=_START_TIME_DESC),
    end_time:      Optional[str] = Query(None, description=_END_TIME_DESC),
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
    has_error:     Optional[bool] = Query(None, description="If true, returns only runs with an error message."),
    limit:         int            = Query(50, description="Max number of log entries to return (default 50)"),
    offset:        int            = Query(0,  description="Pagination offset (default 0)"),
):
    """
    Returns filterable pipeline execution logs with:
    - Run ID, pipeline name, status, tool, timing, rows read/written, error message
    - Pagination via limit + offset
    
    All universal filters apply.
    """
    where, params = build_run_filters(
        pipeline_name=pipeline_name, pipeline_id=pipeline_id,
        status=status, tool=tool,
        start_date=start_date, end_date=end_date,
        start_time=start_time, end_time=end_time,
    )

    extra_clauses = []
    if has_error is True:
        extra_clauses.append("error_message IS NOT NULL AND error_message != ''")
    elif has_error is False:
        extra_clauses.append("(error_message IS NULL OR error_message = '')")

    if extra_clauses:
        if where:
            where += " AND " + " AND ".join(extra_clauses)
        else:
            where = "WHERE " + " AND ".join(extra_clauses)

    count_row = query(f"SELECT COUNT(*) AS total FROM pipeline_runs {where}", params)
    total = int(count_row[0]["total"]) if count_row else 0

    rows = query(f"""
        SELECT
            id AS run_id, pipeline_id, pipeline_name, status, tool_name,
            start_time, end_time, duration,
            rows_read, rows_written,
            error_message, execution_mode, triggered_by, saved_at
        FROM pipeline_runs {where}
        ORDER BY COALESCE(start_time, saved_at) DESC
        LIMIT %s OFFSET %s
    """, params + (limit, offset))

    return jsonify({
        "filters_applied": {
            "pipeline_name": pipeline_name, "status": status,
            "tool": tool, "has_error": has_error,
            "start_date": start_date, "end_date": end_date,
        },
        "pagination": {"total": total, "limit": limit, "offset": offset, "returned": len(rows)},
        "logs": rows,
    })


# =============================================================================
# SINGLE RUN DETAIL
# =============================================================================

@app.get("/api/runs/{run_id}", tags=["Logs"], summary="Detailed view of a single pipeline run")
def get_run_detail(
    run_id: str,
    system_name:   Optional[str] = Query(None, description=_SYSTEM_NAME_DESC),
    database_name: Optional[str] = Query(None, description=_DB_NAME_DESC),
    schema_name:   Optional[str] = Query(None, description=_SCHEMA_NAME_DESC),
    object_name:   Optional[str] = Query(None, description=_OBJECT_NAME_DESC),
):
    """
    Returns complete detail for a single pipeline run:
    - Full run metadata
    - Source asset metadata
    - Target asset metadata
    - Raw execution log (JSON)
    """
    runs = query("""
        SELECT * FROM pipeline_runs WHERE id = %s
    """, (run_id,))

    if not runs:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")

    run = runs[0]

    meta_where_s, meta_params_s = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=[run_id], alias="s",
    )
    meta_where_t, meta_params_t = build_meta_filters(
        system_name=system_name, database_name=database_name,
        schema_name=schema_name, object_name=object_name,
        run_ids=[run_id], alias="t",
    )

    src_meta = query(f"SELECT * FROM source_asset_metadata s {meta_where_s}", meta_params_s)
    tgt_meta = query(f"SELECT * FROM target_asset_metadata t {meta_where_t}", meta_params_t)

    # Parse raw_log if it's a string
    raw_log = run.get("raw_log")
    if isinstance(raw_log, str):
        try:
            raw_log = json.loads(raw_log)
        except Exception:
            pass

    return jsonify({
        "run": {**run, "raw_log": raw_log},
        "source_asset": src_meta[0] if src_meta else None,
        "target_asset": tgt_meta[0] if tgt_meta else None,
    })
