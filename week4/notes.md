## Session 4A — Git integration + deployment pipelines

### Part 1 — Git integration
**Theory:** Fabric workspaces sync to a Git repo (GitHub or Azure DevOps).
Every item serializes into a folder of definition files (.platform +
item-specific content) that gets committed — version control for the WHOLE
workspace, not just code. — EXAM TOPIC (configure version control)

Connected: dp700-dev → GitHub repo dp700-fabric-workspace, branch main.
Note: GitHub Git integration required an existing tenant setting to be
enabled (Admin portal → Tenant settings → Git integration section — 4
related toggles), and authentication used a GitHub Personal Access Token
(classic, repo scope), not an OAuth popup.

### Part 2 — Commit workflow
```sql
CREATE VIEW dbo.vw_open_violations AS
SELECT COUNT(*) AS open_count
FROM dbo.fact_violations
WHERE compliance_status_code NOT IN ('K', 'R');
```
Change in Fabric UI → Source control panel shows modified item → commit
message → Commit → pushed to Git, visible as a diff in GitHub.

### Part 3 — Deployment pipelines — EXAM TOPIC
**Theory:** A deployment pipeline promotes items across stages (Dev → Test →
Prod), each stage bound to its own workspace. Deployment copies item
DEFINITIONS; data/connections follow the source unless a deployment rule
overrides them.

Built: dp700-cicd2 — Development (dp700-dev) → Test (dp700-test) →
Production (dp700-prod), all sharing the ascivofabric F2 capacity (capacity
assignment and pipeline stage are independent settings).
**Deployed Development → Test successfully** — all items (notebooks,
pipelines, warehouse, dashboard, activator) copied and confirmed present
in dp700-test with "Same as source" status.

**UI note (real-world lesson):** workspace assignment per stage requires an
explicit CONFIRM click (a green checkmark next to the dropdown) — selecting
the workspace in the dropdown alone does not save the assignment. Skipping
this caused the stage to appear assigned but show no items until confirmed.
Worth remembering as a "did you actually save it" habit for any Fabric
multi-step config panel.

### Part 4 — Deployment rules — EXAM TOPIC (heavily tested, conceptual this session)
**Theory:** A deployment rule overrides a specific value (parameter,
connection) for one stage, applied AUTOMATICALLY on every future
deployment — configured once, self-maintaining forever after. Without a
rule, Test/Prod point at the same source as Dev, which is usually wrong
(e.g., Test hitting a live rate-limited API, or emailing real users).

Two rule types: **parameter rules** (override a parameter's default value
per stage — e.g., pl_bronze_epa_http's state_code = NY in Test vs PA in
Dev) and **connection rules** (swap which data source connection an item
uses per stage, without touching the item's configuration).

Rules are scoped to ONE stage transition — a Test-stage rule doesn't
automatically apply to Production; each stage needing an override needs
its own rule.

**Note:** located the item-level rules UI in this preview version, but it
opened Activator-style automation rules (Teams/email alerts on pipeline
success/failure) rather than parameter override rules — a UI location
difference to be aware of, not a concept gap.

### Part 5 — Database projects — EXAM TOPIC
**Theory:** Treats Warehouse schema as source-controlled .sql files instead
of ad-hoc UI changes — CREATE TABLE/VIEW/PROC definitions reviewable in pull
requests. Built via "Download SQL database project" from a Warehouse item;
deployed back via schema comparison (DACPAC-like in traditional SQL Server
tooling). Choose over ad-hoc CTAS when the team wants schema changes
code-reviewed before hitting production.

Downloaded: wh_gold SQL database project artifact.



## Session 4B — Monitoring Hub + Capacity Metrics

### Part 1 — Monitoring Hub
**Theory:** Unified run history across every item type (pipelines,
notebooks, dataflows, Spark jobs) in every workspace the user can access.
Persists historical runs, not just live sessions. Columns: Status, Start
time, Duration, Submitter, Location. — EXAM TOPIC (monitor ingestion,
transformation, semantic model refresh)

Reviewed: pl_bronze_epa_http history (including the 1C 404 failure and
badpath drill), pl_master_nightly history (including the 2D Failed/Skipped
pair), Spark notebook session history from 2A/2B/3A.

### Part 2 — Capacity Metrics app
**Theory:** A Power BI app installed over a specific Fabric capacity,
showing CU utilization over time. Installed from Admin portal → Capacity
settings (or via app search). — EXAM TOPIC (monitor via Capacity Metrics)

Installed and viewed against ascivofabric — usage spikes visible for every
session, flat zero during paused periods across the full Aug 9–30 sprint.

### Part 3 — CU behavior — EXAM TOPIC (heavily tested, memorize the list)
**Theory:**
- **Interactive operations**: user actively waiting (notebook cell run,
  live query) — lower tolerance for delay
- **Background operations**: no one watching (scheduled pipeline,
  dataflow refresh, streaming ingestion) — can be smoothed/throttled more

- **Smoothing**: CU consumption spread across a rolling window instead of
  charged as an instant spike, so one heavy job doesn't blow the capacity
  instantly.

- **Throttling escalation (four stages, in order)**:
  1. Overage tracking — usage exceeds capacity, still runs, logged
  2. Interactive delay — interactive operations slow down
  3. Interactive rejection — interactive operations refused
  4. Background rejection — background operations refused entirely

### Part 4 — Audit logs vs Monitoring Hub — EXAM TOPIC
**Theory:** Monitoring Hub = operational run history (did it succeed, how
long did it take). Audit logs = security/compliance trail (who viewed,
changed, or deleted an item; permission changes). Different questions,
different tools — a scenario asking "who changed this permission" points
to Audit logs, not Monitoring Hub.

### Capacity Metrics app — connected and verified
Installed Microsoft Fabric Capacity Metrics app, connected to ascivofabric
via OAuth2 (UTC offset -4 for Eastern Time). Pages available: Health,
Compute, Storage, Autoscale compute for Spark, Item History (Preview).
Health page shows utilization trend across the sprint — visible spikes
during active sessions, flat during paused periods.

## Session 4C — Delta/Spark optimization + troubleshooting runbook

### Part 1 — OPTIMIZE, VACUUM, V-Order — EXAM TOPIC (proven hands-on)
**Theory:** Every write to a Delta table — especially MERGE — can create new
small Parquet files without removing old ones. Small-file buildup degrades
read performance over time (seen back in 2B's Delta history output: 8 files
added/removed per merge operation, repeated across multiple merges).

- **OPTIMIZE**: compacts many small files into fewer large ones. Critically,
  it only marks old files as LOGICALLY removed in the Delta transaction log
  — the physical files remain on disk until VACUUM runs.
- **OPTIMIZE ... VORDER**: Fabric-specific write-time optimization. Sorts
  and encodes data as it writes, so downstream reads by Fabric engines (SQL
  analytics endpoint, Warehouse, Power BI Direct Lake) are dramatically
  faster — like pre-sorting a phone book for instant lookups. Opt-in,
  additive, doesn't change underlying Parquet validity. Trade-off: heavier
  write cost for faster read performance — ideal for write-once-read-many
  tables like Silver/Gold layers.
- **VACUUM ... RETAIN n HOURS**: physically deletes old file versions past
  the retention window (default 7 days / 168 hours). This default exists
  to protect time-travel to recent versions. `RETAIN 0 HOURS` requires
  disabling a safety check (`retentionDurationCheck.enabled = false`) and
  must NEVER be used outside a lab — it destroys recovery ability.

```python
# Check file count before optimizing (via full ABFS path — relative paths
# resolve against the notebook's default lakehouse, not always the target)
files_before = mssparkutils.fs.ls(
    "abfss://dp700-dev@onelake.dfs.fabric.microsoft.com/lh_silver.Lakehouse/Tables/dbo/violations_pa_silver"
)
print("Files before:", len([f for f in files_before if f.name.endswith(".parquet")]))
```

```sql
%%sql
OPTIMIZE lh_silver.dbo.violations_pa_silver
```

```sql
%%sql
OPTIMIZE lh_silver.dbo.violations_pa_silver VORDER
```

```sql
%%sql
-- Lab-only override to force immediate cleanup for demonstration.
-- NEVER use RETAIN 0 in production — it removes time-travel recovery.
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM lh_silver.dbo.violations_pa_silver RETAIN 0 HOURS
```

**PROVEN — the full two-step lifecycle, with real numbers:**
- **96 files** before optimizing (fragmented by repeated 2B MERGE operations)
- **97 files** after `OPTIMIZE` — one new compacted file added, but the 96
  originals were only logically removed, not physically deleted yet
- **9 files** after `VACUUM` with retention override — physical cleanup
  actually happened
- **10 files** after a second `OPTIMIZE ... VORDER` run — same pattern
  repeats: compaction adds a file, old ones stay until the next VACUUM

**EXAM LESSON:** OPTIMIZE and VACUUM are two separate operations with two
separate jobs — OPTIMIZE compacts (logically), VACUUM cleans up
(physically). A file count that doesn't drop right after OPTIMIZE is
expected behavior, not a failure.

### Part 2 — Query Insights on the Warehouse — EXAM TOPIC (proven hands-on)
**Theory:** `queryinsights.exec_requests_history` is a system view logging
every query run against a Fabric Warehouse — comparable to SQL Server's
Query Store. Used to diagnose both performance issues (slow queries) and
security issues (permission failures) in Domain 3 scenarios.

```sql
-- Recent query history
SELECT TOP 10
    distributed_statement_id, start_time, end_time,
    total_elapsed_time_ms, command, status
FROM queryinsights.exec_requests_history
ORDER BY start_time DESC
```

```sql
-- Slowest queries — where you'd start for a "dashboard is slow" scenario
SELECT TOP 5 start_time, total_elapsed_time_ms, command
FROM queryinsights.exec_requests_history
ORDER BY total_elapsed_time_ms DESC
```

```sql
-- Failed/errored queries — corrected after hitting Invalid column name
-- (the view has no "error" column; real columns are error_code/
-- error_severity/error_state — discovered via SELECT TOP 1 *)
SELECT TOP 10
    start_time, status, error_code, error_severity, command
FROM queryinsights.exec_requests_history
WHERE status != 'Succeeded'
ORDER BY start_time DESC
```

**Full column list of this view** (for reference — discovered via
`SELECT TOP 1 * FROM queryinsights.exec_requests_history`):
`distributed_statement_id, database_name, submit_time, start_time,
end_time, is_distributed, is_accelerated, statement_type,
total_elapsed_time_ms, login_name, row_count, status, session_id,
connection_id, program_name, batch_id, root_batch_id, query_hash, label,
result_cache_hit, sql_pool_name, error_code, error_severity, error_state,
allocated_cpu_time_ms, data_scanned_remote_storage_mb,
data_scanned_memory_mb, data_scanned_disk_mb, is_using_external_api,
command`

**PROVEN:** the failed-queries query surfaced the 3D column-level-security
DENY error from 10 days earlier — `error_code 229` (SQL Server standard
permission-denied code), `error_severity 25`, with `command` showing the
exact blocked query (`SELECT TOP 5 * FROM dbo.dim_system_contact`). This
confirms Query Insights retains history across sessions/days and captures
security-permission failures alongside syntax/logic errors — one tool for
both performance AND security troubleshooting.

**EXAM LESSON:** when a system view or DMV throws "Invalid column name,"
run `SELECT TOP 1 *` to discover the real schema rather than guessing
column names from memory or from a different SQL Server DMV.

### Part 3 — Spark performance tuning — EXAM TOPIC (proven with measurements)
**Theory:** `spark.sql.shuffle.partitions` defaults to 200 — oversized for
small clusters (like an F2 capacity) and modest datasets, causing
scheduling/task overhead that outweighs any parallelism benefit.
`.cache()` persists a DataFrame in memory after it's first materialized —
this has an upfront cost (the first pass still has to compute and store
the data) but pays off on every subsequent reuse. Caching a DataFrame
touched only once is pure overhead with no benefit.

```python
import time

# Baseline — default 200 shuffle partitions, no caching
start = time.time()
df = spark.read.format("delta").load("Tables/dbo/violations_pa_bronze")
result = df.groupBy("epa_region").count().collect()
elapsed_unoptimized = time.time() - start
print("Unoptimized time:", elapsed_unoptimized, "seconds")

# Tuned — reduced partitions + cache
spark.conf.set("spark.sql.shuffle.partitions", "4")
start = time.time()
df_cached = df.cache()
df_cached.count()  # force materialization
result2 = df_cached.groupBy("epa_region").count().collect()
elapsed_optimized = time.time() - start
print("Optimized time:", elapsed_optimized, "seconds")
```

**PROVEN — real measurements on 172,851-row violations_pa_bronze,
groupBy(epa_region).count():**
- Unoptimized (200 partitions, no cache): **7.06 seconds**
- First optimized run (4 partitions, cache building): **2.50 seconds**
- Second run against already-cached data: **0.34 seconds** — roughly
  **20x faster** than the original baseline

**EXAM LESSON:** the true payoff of caching shows on the SECOND reuse, not
the first — the first cached run still pays the cost of materializing the
cache. This distinguishes "caching helped a little" (comparing runs 1 and
2) from "caching helped enormously" (comparing runs 1 and 3) — both are
real, but they're measuring different things.

### General optimization decision tree — EXAM SUMMARY
- Lakehouse table has many small files → **OPTIMIZE**, then **VACUUM** for
  physical cleanup
- Table is read heavily by Warehouse/Power BI/Direct Lake → add **VORDER**
  to the OPTIMIZE
- Warehouse query is slow → check **queryinsights.exec_requests_history**
  ordered by `total_elapsed_time_ms`
- Spark job is slow on a small capacity → reduce **shuffle.partitions** to
  match available cores; **cache** any DataFrame reused multiple times
- Need to investigate a security/permission failure in the Warehouse → same
  **queryinsights.exec_requests_history** view, filter on `status != 'Succeeded'`