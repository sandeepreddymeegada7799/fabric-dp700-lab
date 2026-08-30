# Fabric DP-700 Lab — Troubleshooting Runbook
Built from real errors encountered across the 4-week sprint.

| Symptom | Where to look | Root cause / Fix |
|---|---|---|
| Pipeline Copy activity fails with 404 | Monitoring Hub → click run → error detail | Bad relative URL or missing trailing slash on base URL. Error message names the exact table/path — read it literally (1C) |
| PATH_NOT_FOUND in a notebook | Red cell traceback | Wrong table path OR notebook's default lakehouse isn't the one you're targeting — use three-part name (`lakehouse.schema.table`) instead of relative path (2B, 4C) |
| Notebook variable "not defined" | Cell error, session shows "Not connected" | Spark session restarted (idle timeout) — all variables lost. Re-run the full cell chain from the top; pipeline-run notebooks must be fully self-contained (2B, 2D) |
| DELTA_MULTIPLE_SOURCE_ROW error on MERGE | Error names "multiple source rows matched" | Merge key isn't unique enough — add columns until the key uniquely identifies one target row (2B) |
| DELTA_MERGE_UNRESOLVED_EXPRESSION on MERGE | Error lists columns present in target but not source | whenMatchedUpdateAll/InsertAll requires source schema ⊇ target schema — add missing columns (e.g. timestamp) to source before merging (2B) |
| Update policy shows 0 rows for minutes | `.show table policy update`, then `.show ingestion failures` | Policies fire per sealed ingestion batch (up to ~5 min latency) before assuming it's broken. If truly stuck, check column names are exact-case correct in the transform function (3B) |
| Materialized view CREATE fails, names `$IngestionTime` | Error: invalid column, doesn't comply with naming rules | MVs cannot aggregate system columns like `ingestion_time()` — use a real column, or design the source to carry its own timestamp (3B) |
| Eventstream shows "Fatal"/"Warning" before publish | Authoring errors tab in Eventstream editor | Node is unwired — drag a connection from output port to input port; Fatal blocks publish, Warning doesn't (3A) |
| Query fails with Msg 230 (permission denied) | Column-level: error names the specific column | CLS DENY on that column — either grant it or query only allowed columns; `SELECT *` will always fail if any column is denied (3D) |
| Query succeeds but returns 0/fewer rows silently | No error at all — this IS the symptom | RLS filter predicate active — check `Security.<policy_name>` and the predicate function's WHERE logic (3D) |
| T-SQL "Invalid column name" on a system view | `SELECT TOP 1 * FROM <view>` to see real schema | Don't guess column names from memory/other DMVs — every Fabric system view has its own exact schema (4C) |
| Deployment pipeline stage shows no items after assignment | Refresh, or re-check assignment | Workspace assignment requires an explicit CONFIRM (green checkmark) — selecting in the dropdown alone doesn't save it (4A) |
| Delta table file count doesn't drop after OPTIMIZE | List files before/after | OPTIMIZE only logically removes old files (marks them dead in the transaction log) — physical deletion requires VACUUM (4C) |
| VACUUM doesn't reduce file count | Check RETAIN hours value | Default 168-hour (7-day) retention won't delete recent files — this is a safety feature, not a bug. Never use RETAIN 0 outside a lab (4C) |
| Slow Warehouse query, unclear why | `queryinsights.exec_requests_history` ordered by `total_elapsed_time_ms` | Same view also logs failed/denied queries (`error_code`, `error_severity`) — check both performance AND permission issues here (4C) |
| Slow Spark job on small capacity | Notebook execution time, Spark UI | Default `spark.sql.shuffle.partitions` (200) oversized for F2 — reduce to match available cores; `.cache()` a DataFrame reused 2+ times (4C — measured 7.06s → 0.34s, ~20x) |
| Capacity seems slow / operations queuing | Capacity Metrics app → Health/Compute pages | Check for throttling stages: overage tracking → interactive delay → interactive rejection → background rejection (4B) |
| Fabric trial won't activate on new tenant | Admin portal → Tenant settings | New-tenant eligibility restriction, not a config error — provision a pay-as-you-go F2 capacity instead (pre-flight) |

## General debugging discipline learned this sprint
1. **Read the error message completely** — Fabric/Delta/T-SQL errors are almost always self-diagnosing (they name the exact column, path, or constraint violated)
2. **Check `SELECT TOP 1 *` before guessing column names** on any unfamiliar system view or DMV
3. **Silence is a symptom too** — RLS, deactivated streams, and unwired Eventstream nodes fail without throwing errors; "it just doesn't work" needs a different investigation than "it threw an error"
4. **Session state doesn't survive restarts** — always make pipeline-run notebooks self-contained
5. **OPTIMIZE ≠ cleanup** — compaction and physical deletion are two separate operations (OPTIMIZE vs VACUUM)