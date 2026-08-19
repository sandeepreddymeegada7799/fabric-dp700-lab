## Session 3A — Eventstream → Eventhouse

### Part 1 — Eventhouse hierarchy
**Theory:** Eventhouse is the CONTAINER for real-time analytics; KQL
databases live inside it (like a SQL instance holding databases). Optimized
for time-series, append-heavy, high-frequency data. Third data store type:
Lakehouse (files+Spark), Warehouse (T-SQL), Eventhouse (streaming+KQL).
— EXAM TOPIC (choose appropriate data store)

Created: eh_realtime (Eventhouse) with auto-created KQL database.

### Part 2 — Eventstream
**Theory:** Eventstream = managed, no-code streaming pipe: sources →
optional in-flight transforms → destinations. Data flows continuously;
nothing is "run." Sources include Event Hubs, IoT Hub, Kafka, CDC feeds,
sample data. — EXAM TOPIC (process data using Eventstreams)

Built: es_bike_stream using Bicycles sample source; live preview confirmed.

### Part 3 — In-flight transforms
**Theory:** Eventstream transforms process events mid-flight: Manage fields
(rename/remove), Filter, Aggregate, Group by, Join, Union, Expand.
Applied BEFORE landing when destination uses "Event processing before
ingestion" mode; "Direct ingestion" bypasses transforms. — EXAM TRAP

Applied: Manage fields — removed 2 columns, renamed BikepointID → station_id.

### Part 4 — Destination + loading pattern
**Theory:** Streaming loading pattern = source → eventstream → eventhouse
table, continuously. Contrast batch: source → pipeline (scheduled) →
lakehouse. — EXAM TOPIC (design loading pattern for streaming data)

Routed to eh_realtime → new table bike_events; row count grows on refresh.

### Part 5 — KQL first taste
```kusto
bike_events | take 10
bike_events | count
```
Pipes, top-to-bottom. Full KQL session = 3B.

### Choose the streaming engine — EXAM TOPIC
- **Eventstream**: managed, no-code/low-code, built-in sources + transforms,
  routes to Eventhouse/Lakehouse. DEFAULT choice.
- **Spark structured streaming**: code-first (readStream/writeStream in a
  notebook), custom logic, ML in-stream, exotic sinks. Escape hatch when
  Eventstream transforms aren't enough.

### Native tables vs OneLake shortcuts in RTI — EXAM TOPIC
- Native KQL table (bike_events): data ingested INTO the eventhouse —
  fastest queries, hot cache.
- OneLake shortcut in eventhouse: points at existing Delta (e.g. lh_silver
  tables) — query batch + streaming together, no copy, slower than native.

### Eventstream/Eventhouse error triage — EXAM TOPIC (Domain 3)
- Per-node status on the Eventstream canvas + Data insights = where errors
  surface (schema mismatch, broken destination).
- Deactivated stream = ingestion stops silently — count stops growing.
- COST: a published Eventstream consumes CUs continuously.