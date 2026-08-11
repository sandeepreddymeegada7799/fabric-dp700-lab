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