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