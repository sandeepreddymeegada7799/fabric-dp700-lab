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



## Session 1D — Dataflow Gen2 + Tool Choice

### When to use Dataflow Gen2 — EXAM TOPIC
USE when:
- Business analyst or non-developer needs to transform data (no code)
- Source is already supported by Power Query connectors
- Light transforms: rename, filter, change types, merge queries
- Output goes directly to a Lakehouse table or Warehouse

DON'T USE when:
- Data volume is very large (Dataflow Gen2 has limits vs Spark)
- Complex logic needed (loops, custom functions, ML)
- You need full code control and unit testing

### When to use Pipeline — EXAM TOPIC  
USE when:
- Moving data from external source into Fabric (Copy activity)
- Orchestrating multiple steps in sequence with dependencies
- Scheduling and triggering workflows
- Looping over multiple files/tables (ForEach)
- Need retry logic, failure notifications, branching

DON'T USE when:
- You need to transform data with business logic → use Notebook
- Low-code transforms → use Dataflow Gen2

### When to use Notebook (PySpark) — EXAM TOPIC
USE when:
- Complex transformations (MERGE, dedup, watermark, custom logic)
- Large data volumes (3.8GB violations file)
- Need full Python ecosystem (pandas, ML libraries)
- Building Silver/Gold layers with incremental load patterns

DON'T USE when:
- Simple column-level transforms → use Dataflow Gen2
- Just moving data between systems → use Pipeline

### When to use T-SQL — EXAM TOPIC
USE when:
- Working in the Warehouse (Gold layer)
- Creating views, aggregations, analytical queries
- CTAS (Create Table As Select) for dimensional model
- Team is SQL-heavy, not Python-heavy

DON'T USE when:
- Working with raw files in Lakehouse Files zone (no SQL access)
- Need Spark-scale processing

### When to use KQL — EXAM TOPIC
USE when:
- Querying streaming/time-series data in Eventhouse
- Real-time dashboards and alerts
- Time-window aggregations (bin(), summarize)

DON'T USE when:
- Batch data in Lakehouse/Warehouse → use SQL or PySpark

### One-line decision rule — EXAM SHORTCUT
- External data → Fabric: Pipeline (Copy activity)
- Low-code transforms: Dataflow Gen2
- Complex/large-scale transforms: Notebook (PySpark)
- Dimensional model queries: T-SQL (Warehouse)
- Streaming/real-time queries: KQL (Eventhouse)


## Session 1E — Shortcuts + Mirroring


## Mirroring capability matrix — EXAM TOPIC

### What mirroring does
Continuously replicates data from external source into Fabric OneLake
Near real-time, no pipeline needed, change data capture (CDC) based
Data lands as Delta Parquet in OneLake — queryable by Spark and SQL endpoint

### Supported sources (memorize these)
- Azure SQL Database ✓
- Azure SQL Managed Instance ✓  
- Azure Cosmos DB ✓
- Snowflake ✓
- Azure Databricks ✓
- Open mirroring (any source via API) ✓

### Mirroring vs Shortcut vs Pipeline — EXAM TOPIC
- Mirroring: external DB → Fabric, continuous CDC replication, near real-time
- Shortcut: Fabric/cloud storage → Fabric, zero copy, no replication (pointer only)
- Pipeline: any source → Fabric, scheduled/triggered, batch copy

### When exam says "use mirroring":
- Source is Azure SQL DB / Snowflake / Cosmos
- Need near real-time data in Fabric without writing pipelines
- Want data as Delta format automatically

### When exam says "use shortcut":
- Data already in OneLake, ADLS Gen2, S3, or GCS
- Don't want to copy data — just need to access it from multiple workspaces
- Zero ETL, zero storage duplication


## Shortcut supported targets — EXAM TOPIC
Shortcuts can point to:
- Another OneLake location (different workspace/lakehouse) ← what you built today
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Amazon S3
- Google Cloud Storage (GCS)
- Dataverse
- S3-compatible storage

Shortcuts CANNOT point to:
- Azure SQL Database (use mirroring instead)
- On-premises data sources (use pipeline with gateway)