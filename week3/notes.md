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