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