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