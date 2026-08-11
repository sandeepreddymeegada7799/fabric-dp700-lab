## Session 1B — OneLake + Lakehouse + Delta

### OneLake architecture
OneLake (tenant storage)
  └── dp700-dev (workspace)
        └── lh_bronze (Lakehouse)
              ├── Files/   → raw, unmanaged, any format, no SQL access
              └── Tables/  → managed Delta tables (Parquet + _delta_log)
                    └── dbo/sdwa_ref_code_values/

### Three query methods — EXAM TOPIC
| Method          | Read | Write | Compute needed |
|-----------------|------|-------|----------------|
| Spark notebook  | ✓    | ✓     | Yes (CUs)      |
| SQL endpoint    | ✓    | ✗     | No             |
| Visual explorer | ✓    | ✗     | No             |

### SQL endpoint is READ-ONLY — EXAM TRAP
Error: "DML statements are not supported for this table type"
Only SELECT works. INSERT/UPDATE/DELETE all fail.
Write operations must go through Spark (notebooks/pipelines).

### Data store choice — EXAM TOPIC
- Lakehouse  → Spark + SQL + raw files (Bronze/Silver layers)
- Warehouse  → pure SQL + dimensional model (Gold layer)  
- Eventhouse → real-time streaming + KQL (Week 3)

### Delta format internals
Delta = Parquet (data) + _delta_log (transaction log JSON files)
_delta_log enables: ACID transactions, time travel, schema enforcement


## Session 1C — Pipeline ingestion

### Pipeline anatomy
Source (HTTP/EPA Envirofacts API) → Copy activity → Sink (lh_bronze/Files)
Base URL: https://data.epa.gov/efservice/
Relative URL: WATER_SYSTEM/PRIMACY_AGENCY_CODE/PA/CSV

### Schedule trigger — EXAM TOPIC
Daily at midnight Eastern — configured via Schedule button on pipeline
Failure notification email configured on schedule panel

### Pipeline error triage — EXAM TOPIC
Where to look: Output tab → failed activity → error details
404 error = wrong URL path (bad table name or relative URL)
Read the error message carefully — it tells you exactly what's wrong

### Monitor ingestion — EXAM TOPIC  
All pipeline runs visible in: left rail → Monitor → Monitoring Hub
Shows: run ID, status (succeeded/failed), duration, start time