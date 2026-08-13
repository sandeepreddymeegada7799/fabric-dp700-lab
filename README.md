# Fabric DP-700 Lab — Water Utility Compliance Analytics

# Fabric DP-700 Lab — Water Utility Compliance Analytics
End-to-end Microsoft Fabric solution over EPA SDWA drinking-water compliance data.
Built on a pay-as-you-go F2 capacity (West US 3) for the DP-700 certification sprint.

## Architecture

### Week 1 — Bronze Layer ✅
Three ingestion paths into lh_bronze:
1. Pipeline (pl_bronze_epa_http) — HTTP → Copy activity → Files/raw/ (scheduled daily)
2. Dataflow Gen2 (df_bronze_epa_violations) — EPA API → Power Query → Tables/dbo/violations_pa_bronze
3. Shortcut (ref_codes_shortcut) — zero-copy pointer to lh_reference/sdwa_ref_code_values

### Week 2 — Silver + Gold (coming)
### Week 3 — Real-Time + Security (coming)
### Week 4 — CI/CD + Optimization (coming)

## Workspace Items (dp700-dev)
| Item | Type | Purpose |
|------|------|---------|
| lh_bronze | Lakehouse | Bronze raw layer |
| lh_reference | Lakehouse | Reference data source |
| pl_bronze_epa_http | Pipeline | HTTP ingestion, daily schedule |
| df_bronze_epa_violations | Dataflow Gen2 | Violations ingestion + transforms |
| nb_1b_lakehouse | Notebook | PySpark exploration |

## Environment
- Capacity: ascivofabric (F2, West US 3) — pause after every session
- Login: fabric@sandeepr568gmail.onmicrosoft.com
- Dataset: EPA SDWA quarterly download (ECHO) + Envirofacts API

## Exam Topics Covered — Week 1
**Domain 1:** OneLake settings, workspace settings, orchestration basics
**Domain 2:** Pipeline ingestion, Dataflow Gen2, shortcuts, mirroring matrix, tool-choice comparison
**Domain 3:** Pipeline error triage, Dataflow errors, shortcut errors, Monitoring Hub


### Week 2 — Silver + Gold ✅
Medallion complete, one scheduled chain:

Bronze (12:00 AM) ─ pl_bronze_epa_http → lh_bronze
        ↓
Silver (12:30 AM) ─ pl_master_nightly → Notebook nb_silver_merge
                    dedup + MERGE upsert (pwsid, violation_id) + watermark incremental
                    → lh_silver.dbo.violations_pa_silver (172,851 rows)
        ↓
Gold ──────────────  Stored procedure sp_rebuild_fact_violations (DROP + CTAS)
                    → wh_gold: dim_water_system, dim_contaminant, fact_violations

Orchestration: on-success dependency wiring, per-activity retry (1x/120s),
failure notification, mid-chain failure isolation proven (Failed → Skipped).

| New items (Week 2) | Type | Purpose |
|---|---|---|
| lh_silver | Lakehouse | Silver cleaned layer |
| wh_gold | Warehouse | Gold star schema |
| nb_2a_spark, nb_2b_silver | Notebooks | PySpark dev |
| nb_silver_merge | Notebook | Pipeline-ready Silver merge |
| pl_master_nightly | Pipeline | Silver+Gold orchestration, daily 12:30 AM |