## Session 3A — Eventstream → Eventhouse

### Part 1 — Eventhouse hierarchy
**Theory:** Eventhouse is the CONTAINER for real-time analytics; KQL
databases live inside it (like a SQL Server instance holding databases).
Optimized for append-heavy, timestamped, high-frequency data queried by
time windows. Third data store type: Lakehouse (files + Spark), Warehouse
(T-SQL + dimensional), Eventhouse (streaming + KQL). Delta/Lakehouse is
wrong for high-frequency micro-writes (small-file problem) — that's WHY
Eventhouse exists. — EXAM TOPIC (choose appropriate data store)

Created: eh_realtime with auto-created KQL database of the same name.

### Part 2 — Eventstream (the always-on pipe)
**Theory:** Eventstream = managed streaming pipe: sources → optional
in-flight transforms → destinations. Nothing is "run" — it catches events
continuously. Sources: Event Hubs, IoT Hub, Kafka, CDC, sample data.
Batch loading = pipeline wakes on schedule; streaming loading = eventstream
never sleeps. — EXAM TOPIC (design loading pattern for streaming data)

Built: es_bike_stream with Bicycles sample source; live preview confirmed.

### Part 3 — In-flight transforms
**Theory:** Transforms process events mid-flight, BEFORE landing:
Manage fields, Filter, Aggregate, Group by, Join, Union, Expand.
Only applied when destination mode = "Event processing before ingestion";
"Direct ingestion" bypasses transforms entirely. — EXAM TRAP

Applied: ManageFields — removed Latitude/Longitude, renamed
BikepointID → station_id. Verified at destination with:
```kusto
bike_events | getschema
```

### Part 4 — Destination + the wiring lesson
**Theory:** Destination node config: Eventhouse → KQL database → new table
bike_events, Json input, activate ingestion. Nothing is live until Publish
(Edit mode). Nodes must be WIRED: drag from output port → input port.
— EXAM TOPIC (Eventstream error triage, Domain 3)

Error hit for real: Authoring errors tab showed
- ManageFields / Warning: "missing a destination to work"
- Eventhouse / Fatal: "missing an input to work"
= unwired node. Fatal blocks publishing; Warning doesn't. Fixed by
dragging the connection, errors cleared, published.

### Part 5 — Live ingestion proven with KQL
```kusto
bike_events | take 10
bike_events | count      // 17,430
bike_events | count      // 17,700 one minute later — nobody ran anything
```
270 events/minute arriving continuously. KQL reads top-to-bottom via pipes.
Full KQL session = 3B (queryset kql_3a_explore reused there).

### Choose the streaming engine — EXAM TOPIC
- Eventstream: managed, no/low-code, built-in sources + transforms. DEFAULT.
- Spark structured streaming: code-first (readStream/writeStream), custom
  logic/ML in-stream, exotic sinks. Escape hatch.

### Native tables vs OneLake shortcuts in RTI — EXAM TOPIC
- Native KQL table (bike_events): ingested into eventhouse, hot cache,
  fastest time-window queries.
- OneLake shortcut inside eventhouse: points at existing Delta (e.g.
  lh_silver) — query batch + streaming together, no copy, slower.

### Cost note (real-world + capacity behavior)
A published Eventstream consumes CUs CONTINUOUSLY. Shutdown ritual for
streaming sessions: deactivate stream FIRST, then pause capacity.


## Session 3B — KQL fundamentals on a live stream

### Part 1 — Core operators (the SQL → KQL bridge)
**Theory:** KQL reads top-to-bottom through pipes; each line transforms the
previous result. Direct mapping from T-SQL: `where` = WHERE, `project` =
SELECT (columns), `summarize ... by` = GROUP BY + aggregates, `order by` /
`top` = ORDER BY / TOP, `take` = quick sample. All queries below ran against
bike_events while it was actively ingesting (~270 events/min).
— EXAM TOPIC (transform data using KQL)

```kusto
// WHERE — filter
bike_events
| where No_Bikes > 20
| take 10

// SELECT columns — project
bike_events
| where Neighbourhood == "Chelsea"
| project StationID, Street, No_Bikes, No_Empty_Docks
| take 10

// GROUP BY — summarize
bike_events
| summarize avg_bikes = avg(No_Bikes), events = count() by Neighbourhood
| order by events desc

// TOP N
bike_events
| summarize max_bikes = max(No_Bikes) by Street
| top 10 by max_bikes desc
```

### Part 2 — Windowing with bin() — THE exam pattern
**Theory:** bin(timestamp, 5m) rounds every timestamp down into its 5-minute
bucket, so summarize groups by time window. "Aggregate per time window" is
the single most-tested KQL pattern on DP-700. AquaProof translation: avg
chlorine reading per facility per 5-min window.
**LESSON EARNED:** every expression in `by` must be aliased, or KQL
auto-names it Column1 (same habit as aliasing computed columns in T-SQL).
`ingestion_time()` = built-in per-row arrival timestamp (hidden system
column $IngestionTime) — works in queries, NOT in materialized views.
— EXAM TOPIC (create windowing functions)

```kusto
// Events per 5-minute window (alias the bin!)
bike_events
| summarize events = count() by time_bucket = bin(ingestion_time(), 5m)
| order by time_bucket asc

// Two-dimensional window: per neighbourhood per 10 min
bike_events
| summarize avg_bikes = avg(No_Bikes) by Neighbourhood, time_bucket = bin(ingestion_time(), 10m)
| order by Neighbourhood asc, time_bucket asc

// Instant visualization — feeds Real-Time Dashboards (3C)
bike_events
| summarize events = count() by time_bucket = bin(ingestion_time(), 5m)
| render timechart
```

### Part 3 — let, datatable, join
**Theory:** `let` = variable/CTE. `datatable` = inline literal table.
`join kind=inner` = INNER JOIN (also leftouter etc.). Same relational
algebra, KQL syntax.

```kusto
let zone_lookup = datatable(Neighbourhood:string, zone:string) [
    "Chelsea", "West",
    "Fitzrovia", "Central",
    "Mile End", "East",
    "Wandsworth Road", "South"
];
bike_events
| join kind=inner zone_lookup on Neighbourhood
| summarize avg_bikes = avg(No_Bikes) by zone
```

### Part 4 — Update policy (streaming ETL inside the database)
**Theory:** An update policy auto-runs a function whenever new rows land in
a source table, writing derived rows to a target table — server-side
transform-on-ingest, no pipeline, no notebook. Dot-prefixed commands
(.create, .alter, .show) are MANAGEMENT commands (schema/policy), distinct
from queries — exam distinguishes them.
— EXAM TOPIC (optimize Eventhouses)

```kusto
// 1. Target table
.create table bike_events_summary (station_id: string, Street: string, No_Bikes: long, capacity: long)

// 2. Transform function
.create-or-alter function TransformBikeEvents() {
    bike_events
    | project station_id = StationID, Street, No_Bikes, capacity = No_Bikes + No_Empty_Docks
}

// 3. Attach policy
.alter table bike_events_summary policy update
@'[{"IsEnabled": true, "Source": "bike_events", "Query": "TransformBikeEvents()", "IsTransactional": false}]'

// Verify
bike_events_summary | count
```

**LESSONS EARNED (count stayed 0, then jumped to 750):**
- Policies fire per SEALED INGESTION BATCH — eventstream batches can take
  up to ~5 min to close. Zero rows for a few minutes ≠ broken.
- Update policies are FORWARD-ONLY: they transform only events arriving
  after attachment, never backfill history (750 summary vs 18k+ raw).
- With IsTransactional:false, a broken policy fails SILENTLY — source keeps
  ingesting, derived table just stays empty. — EXAM TRAP
- Triage sequence (Domain 3, Eventhouse errors):
```kusto
.show table bike_events_summary policy update   // is it attached?
TransformBikeEvents() | take 10                 // does the function run?
bike_events | getschema                          // exact column names (CASE-SENSITIVE)
.show ingestion failures | where Table == "bike_events_summary"
```

### Part 5 — Materialized view
**Theory:** A materialized view is a continuously-maintained aggregation —
pre-computed as data arrives, so querying it skips re-scanning millions of
raw rows. arg_max(ts, ...) = latest-row-per-group (needs a real timestamp).
— EXAM TOPIC (optimize Eventhouses)

**ERROR HIT (exam-grade):** `.create materialized-view` with
`arg_max(ingestion_time(), ...)` failed:
`'$IngestionTime' is not valid ... does not comply with naming rules`.
**LESSON:** MVs cannot materialize system columns — real columns only.
Latest-per-entity semantics require the source to CARRY its own timestamp
column (design it into the eventstream). getschema confirmed our table has
no datetime column (StationID, Street, Neighbourhood, No_Bikes,
No_Empty_Docks — note: rename produced StationID, exact case matters).

Pivot — peak per station over real columns:
```kusto
.create materialized-view mv_peak_per_station on table bike_events
{
    bike_events
    | summarize peak_bikes = max(No_Bikes) by StationID, Street
}

mv_peak_per_station | count   // 120 at creation, climbing toward ~790
```

**LESSON EARNED:** MVs are also forward-only BY DEFAULT — start from
creation time. `with (backfill=true)` includes history (MV-only option;
update policies can never backfill):
```kusto
.create materialized-view with (backfill=true) mv_peak_per_station on table bike_events
{ bike_events | summarize peak_bikes = max(No_Bikes) by StationID, Street }
```

### The decision triangle — EXAM TOPIC (memorize)
- **Plain query:** computed on demand, every time. Ad-hoc analysis.
- **Update policy:** transform/route rows AS THEY ARRIVE into a new table.
  "Reshape on ingest."
- **Materialized view:** continuously-maintained AGGREGATION over one table.
  "Latest/max per entity, fast, always current."

### The three-table story (what the counts proved)
- bike_events (raw log): grows forever — 18k+ and climbing
- bike_events_summary (update policy): grows, but only from policy
  attachment forward — 750+
- mv_peak_per_station (MV): plateaus at the entity count (~790 stations)
  regardless of how long the stream runs

### Part 6 — Query acceleration vs standard shortcuts — EXAM TOPIC
- Standard OneLake shortcut in an eventhouse: reads Delta files cold each
  query. No extra cost, slower; batch + streaming queryable together.
- Query acceleration ON the shortcut: caches/indexes the shortcut data
  inside the eventhouse → near-native KQL speed, at extra CU + storage cost.
- Trade-off question shape: "team needs fast KQL over existing Delta
  without ingesting a copy" → shortcut + query acceleration.

### Cost/behavior note
Stream stayed live all session (needed for 3C Activator). Shutdown order
for streaming work: deactivate eventstream FIRST, then pause capacity.


## Session 3C — Activator alerts + Real-Time Dashboard

### Part 1 — Activator (detection → action on streaming data)
**Theory:** Activator watches a stream (or a KQL query / dashboard tile) and
fires an ACTION — email, Teams, or a Fabric item — when a CONDITION is met.
No polling, no dashboard-watching human, no scheduled job. It's the "act"
corner of Real-Time Intelligence. Rules are created from an eventstream's
**Set alert** button and live inside an Activator item, which shows the rule,
a live chart of the monitored property, and an ACTIVATION HISTORY of every
firing. — EXAM TOPIC (configure alerts)

Built: rule `bikes_above_30` on es_bike_stream
- Monitor source: es_bike_stream-stream
- Grouping field: StationID  → condition evaluated PER STATION, not across
  the whole firehose
- When: No_Bikes
- Condition: Numeric state → **Becomes greater than 30**
- Action: Email → subject "Activator alert"

**EXAM NUANCE — the condition categories (seen in the real UI):**
| Category | Fires when | Use for |
|---|---|---|
| Numeric/Text/Logical **change** | the value CHANGES (any change, or by an amount) | drift detection |
| Numeric/Text/Logical **state** | value's STATE vs a threshold (Becomes greater than, Becomes less than, Is greater than) | threshold alerts ← used here |
| Common change → **Changes** | ANY change at all | too noisy, alert spam |
| **Heartbeat** | data STOPS arriving | dead sensor / broken feed detection |

Key distinction: **"Becomes greater than"** fires once on the TRANSITION
across the threshold; **"Is greater than"** fires on EVERY event above it
(spam). Transition semantics + a grouping field = one useful alert per
entity per crossing.

AquaProof mapping: No_Bikes becomes > 30 per StationID
≡ chlorine_ppm becomes > regulatory limit per sensor_id → page the
compliance officer in real time. Heartbeat ≡ "sensor went dark."

### Part 2 — Real-Time Dashboard
**Theory:** RTI's serving layer. Each tile ("visual") is a KQL query with an
auto-refresh interval, sourced from a KQL database. Best practice: point
tiles at MATERIALIZED VIEWS / summary tables rather than raw event scans —
pre-computed aggregates are faster and cheaper on every refresh.

Built: `rtd_bike_ops`, data source eh_realtime, 3 tiles, auto-refresh 30s.

```kusto
// Tile 1 (Time series) — live ingestion rate
bike_events
| summarize events = count() by time_bucket = bin(ingestion_time(), 5m)
| render timechart
```

```kusto
// Tile 2 (Column chart) — reads the MATERIALIZED VIEW, not raw
mv_peak_per_station
| top 10 by peak_bikes
| render columnchart
```

```kusto
// Tile 3 (Bar chart) — availability by neighbourhood
bike_events
| summarize avg_bikes = avg(No_Bikes) by Neighbourhood
| render barchart
```

Dashboard also offers **Add alert** directly on a tile — a second entry
point to Activator (same engine, different starting surface).

### The complete Real-Time Intelligence picture — EXAM SUMMARY
```
Eventstream   → catch + transform events in flight   (3A)
Eventhouse    → store + query with KQL               (3A, 3B)
  ├ update policy      → transform on ingest
  └ materialized view  → maintained aggregation
Activator     → ACT on conditions (email/Teams)      (3C)
RT Dashboard  → SEE it live, auto-refreshing         (3C)
```
Batch lane (pipeline → lakehouse → warehouse) never touches this path.
Scenario tells: "continuous arrival + time-window queries + act within
seconds" → RTI. "Files on a schedule + dimensional model" → batch.

### Cost/behavior note
A published eventstream + a live Activator rule + an auto-refreshing
dashboard all consume CUs CONTINUOUSLY. Shutdown order for streaming
sessions: deactivate eventstream FIRST → then pause the capacity.

## Session 3D — Security & Governance

### Part 0 — Source data with PII
**Theory:** The EPA water systems data carries real-world sensitive fields —
admin_name, email_addr, phone_number — making it a natural target for the
security controls in this session. The 1C pipeline created a nested folder
structure (URL path segments became folders) so the CSV had to be read with
Spark using the explicit path, not the lakehouse UI "Load to Tables."
— EXAM TOPIC (identify and resolve pipeline/Copy activity quirks)

```python
# Read the nested CSV that the Copy activity created
df = spark.read.option("header", "true").csv(
    "Files/raw/epa_pw_systems_PA.csv/WATER_SYSTEM/PRIMACY_AGENCY_CODE/PA/"
)
df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
    .saveAsTable("lh_bronze.dbo.pw_systems_contact")
```

```sql
-- Expose PII columns in the Gold Warehouse
CREATE TABLE dbo.dim_system_contact
AS
SELECT DISTINCT pwsid, pws_name, admin_name, email_addr,
    phone_number, city_name, state_code, epa_region
FROM lh_bronze.dbo.pw_systems_contact;
```

### Part 1 — Second user for security testing
**Theory:** Security you can't test is security you don't understand.
Created analyst@...onmicrosoft.com in Entra (no admin role) and added to
dp700-dev as Viewer. Verified baseline: analyst can query wh_gold, sees
8,052 systems in region 3 — no restrictions yet applied.

### Part 2 — Workspace roles & item permissions — EXAM TOPIC
**Theory:** Four roles, least → most powerful:
- **Viewer**: read items and data only
- **Contributor**: create, edit, delete items; run pipelines
- **Member**: Contributor + share items + add others (up to Member level)
- **Admin**: Member + change workspace settings, delete workspace

Item-level permission = sharing one item without workspace access.
In wh_gold → ⋯ → Manage permissions → you can grant Read/ReadData/ReadAll/Build.
These are FINER-grained than workspace roles.
— EXAM TOPIC: workspace roles vs item permissions vs OneLake data access roles
  are three SEPARATE security layers; a scenario question names the layer.

### Part 3 — Row-Level Security (RLS) — EXAM TOPIC
**Theory:** RLS silently filters rows using a predicate function called once
per row. Three required parts: schema, function (WITH SCHEMABINDING, returns
TABLE), security policy with FILTER PREDICATE.
Admin bypass: USER_NAME() check inside the WHERE ensures admins see all rows.
FILTER = hides rows from SELECT; BLOCK = prevents writes violating the rule.

```sql
CREATE SCHEMA Security;
```

```sql
-- Predicate: returns 1 row (allow) or 0 rows (hide) per row evaluated
CREATE FUNCTION Security.fn_region_filter(@epa_region VARCHAR(10))
RETURNS TABLE
WITH SCHEMABINDING        -- required for RLS predicate functions
AS
RETURN
    SELECT 1 AS fn_result
    WHERE @epa_region = '9'    -- allow region 9 (none in our data = analyst sees nothing)
       OR USER_NAME() = 'fabric@sandeepr568gmail.onmicrosoft.com';  -- admin bypass
```

```sql
-- Policy attaches the function to the table; STATE = ON activates it
CREATE SECURITY POLICY Security.RegionFilter
ADD FILTER PREDICATE Security.fn_region_filter(epa_region)
ON dbo.dim_water_system
WITH (STATE = ON);
```

**PROVEN:** fabric@ sees 8,052 rows; analyst sees 0 rows with NO error.
RLS behavior: **silent filtering** — query succeeds, just returns fewer rows.
To disable: `ALTER SECURITY POLICY Security.RegionFilter WITH (STATE = OFF);`

### Part 4 — Column-Level Security (CLS) — EXAM TOPIC
**Theory:** DENY on specific columns blocks access loudly with Msg 230.
DENY overrides any GRANT — strongest permission wins.

```sql
DENY SELECT ON dbo.dim_system_contact(email_addr, phone_number, admin_name)
TO [analyst@sandeepr568gmail.onmicrosoft.com];
```

**PROVEN:**
- `SELECT * FROM dbo.dim_system_contact` → Msg 230 fired 3 times (once per
  denied column). Query failed completely.
- `SELECT pwsid, pws_name, city_name, state_code ...` → succeeded, returned data.

**THE KEY CONTRAST (exam's favorite pair):**
| | RLS | CLS |
|---|---|---|
| Mechanism | predicate filter | DENY permission |
| User experience | silent, no error | loud, Msg 230 |
| Query outcome | succeeds, fewer rows | fails entirely |
| Implication | SELECT * still works | SELECT * breaks |

### Part 5 — Dynamic Data Masking (DDM) — EXAM TOPIC
**Theory:** Masks column values at display time — data on disk is unchanged.
Query succeeds, columns are accessible, but values are obfuscated.
Users with UNMASK permission see real values. DDM is obfuscation, NOT
encryption. NEVER a substitute for CLS/encryption for true security.

```sql
-- First: REVOKE the CLS DENY so DDM can show (DENY would win otherwise)
REVOKE SELECT ON dbo.dim_system_contact(email_addr, phone_number, admin_name)
TO [analyst@sandeepr568gmail.onmicrosoft.com];
```

```sql
-- email() mask: first letter + XXXX@XXXX.com
ALTER TABLE dbo.dim_system_contact
ALTER COLUMN email_addr VARCHAR(8000) MASKED WITH (FUNCTION = 'email()');

-- partial(prefix, padding, suffix): show last 4 digits of phone
ALTER TABLE dbo.dim_system_contact
ALTER COLUMN phone_number VARCHAR(8000) MASKED WITH (FUNCTION = 'partial(0,"XXX-XXX-",4)');

-- default(): XXXX for strings, 0 for numbers, 1900-01-01 for dates
ALTER TABLE dbo.dim_system_contact
ALTER COLUMN admin_name VARCHAR(8000) MASKED WITH (FUNCTION = 'default()');
```

**Mask function reference:**
- `default()` → XXXX / 0 / 1900-01-01
- `email()` → aXXX@XXXX.com
- `partial(p,"pad",s)` → p chars from start + literal pad + s chars from end
- `random(1,100)` → random number in range (numeric columns)

**PROVEN:** analyst query succeeded; admin_name showed XXXX, email showed
masked form, phone showed XXX-XXX-NNNN. fabric@ saw real values.

### Part 6 — OneLake security — EXAM TOPIC
**Theory:** OneLake data access roles restrict at the STORAGE LAYER — below
workspace roles. DefaultReader is auto-created for every Lakehouse; it grants
Read permission to all Tables and Files by default to workspace members.
You can EDIT DefaultReader or create custom roles to restrict access to
specific schemas, tables, or file folders — e.g. block raw/ from analysts
while still letting them query Tables via the SQL endpoint.

Hierarchy:
```
Workspace role (Admin/Member/Contributor/Viewer)
    ↓
OneLake data access role (DefaultReader, custom roles)
    ↓
Warehouse security (RLS / CLS / DDM)
```
Each layer is independent; a tighter inner layer always wins.

Seen in UI: DefaultReader covers Tables/dbo (Schema), Tables/ref_codes_shortcut
(Schema), Files/raw (Folder) — all with Read/Grant.

### Part 7 — Sensitivity labels (theory only — needs Purview licensing)
Labels (Confidential, Public, General, etc.) classify items and TRAVEL
DOWNSTREAM into any export or derived report. Assigned in item Settings.
Managed centrally in Microsoft Purview. Not available on personal tenants.
— EXAM TOPIC: know that labels travel and are Purview-managed.

### Part 8 — Endorsement — EXAM TOPIC
**Theory:** Endorsement = a trust badge on an item. Two levels:
- **Promoted**: any workspace member can set — "I vouch for this"
- **Certified**: set only by a tenant-admin-designated certifier — "IT approved"

Done: wh_gold set to Promoted. Badge visible in workspace list Endorsement column.
Purpose: analysts use the badged version, not a stale copy someone made last month.

### Part 9 — Domains — EXAM TOPIC (configure domain workspace settings)
**Theory:** Domains group workspaces for governance: apply default sensitivity
labels per domain, set data residency policies, delegate admin rights.
Done: created "Water Utilities" domain in Admin portal → Domains;
assigned dp700-dev to it.