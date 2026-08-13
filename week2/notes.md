## Session 2A — PySpark basics

### Core PySpark pattern — EXAM TOPIC
df = spark.read.format("delta").load("Tables/dbo/table_name")  # read
df.filter() / .select() / .withColumn() / .withColumnRenamed()  # transform
df.write.format("delta").mode("overwrite").saveAsTable("lh.dbo.table") # write

### Example Code
df = spark.read.format("delta").load("Tables/dbo/sdwa_ref_code_values")

df.printSchema()

Output:
root
 |-- VALUE_TYPE: string (nullable = true)
 |-- VALUE_CODE: string (nullable = true)
 |-- VALUE_DESCRIPTION: string (nullable = true)

--------------------------------------------------------------------------

### Key functions used
- filter() → WHERE equivalent
- select() → SELECT columns
- withColumn() → add/modify column
- withColumnRenamed() → rename column
- upper(), current_timestamp() → built-in functions
- createOrReplaceTempView() → use SQL on a DataFrame

### Example Code
# Filter: only contaminant codes
df_contaminants = df.filter(df.VALUE_TYPE == "CONTAMINANT_CODE")

# Select specific columns
df_selected = df_contaminants.select("VALUE_CODE", "VALUE_DESCRIPTION")

# Rename a column
df_renamed = df_selected.withColumnRenamed("VALUE_CODE", "contaminant_code") \
                        .withColumnRenamed("VALUE_DESCRIPTION", "contaminant_name")

# Show result
df_renamed.show(10)
print("Total contaminant codes:", df_renamed.count())

Output:
+----------------+--------------------+
|contaminant_code|    contaminant_name|
+----------------+--------------------+
|            2072|       Endosulfan II|
|            2073|            Phosdrin|
|            2074|  Endosulfan sulfate|
|            4336|     74-TUNGSTEN-181|
|            0200|Surface Water Tre...|
|            0300|Interim Enhanced ...|
|            0400|Stage 1 Disinfect...|
|            0500|Filter Backwash Rule|
|            0600|Stage 2 Disinfect...|
|            0700|    Groundwater Rule|
+----------------+--------------------+
only showing top 10 rows

Total contaminant codes: 871

----------------------------------------------------------------------------

### %%sql magic — EXAM TOPIC
Write SQL directly in a notebook cell
Must register DataFrame as temp view first with createOrReplaceTempView()

### Example Code

df_renamed.createOrReplaceTempView("contaminant_codes")

%%sql
SELECT contaminant_code, contaminant_name
FROM contaminant_codes
WHERE contaminant_name LIKE '%Lead%'
ORDER BY contaminant_code

Output:

contaminant_code contaminant_name
1030             Lead
5000             Lead and Copper Rule1030

----------------------------------------------------------------------

### Write back to Delta:
# Write the transformed DataFrame as a new Delta table in lh_bronze
df_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lh_bronze.dbo.ref_contaminants_silver_prep")

print("Table written successfully")

Output:
Table written successfully(Verified in lh_bronze the table ref_contaminants_silver_prep
exists )

-----------------------------------------------------------------------

### Spark session config — EXAM TOPIC
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.sparkContext.defaultParallelism → number of cores available

# Example Code
# Check current Spark configuration
print("App name:", spark.sparkContext.appName)
print("Default parallelism:", spark.sparkContext.defaultParallelism)

# Set a config value
spark.conf.set("spark.sql.shuffle.partitions", "8")
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

Output:
App name: nb_2a_spark_a9f03b48-521e-4502-94f9-b9a0cdc4269d
Default parallelism: 8
Shuffle partitions: 8

--------------------------------------------------------------------------------

### Notebook error triage — EXAM TOPIC
PATH_NOT_FOUND → wrong table path
AnalysisException → column not found, wrong data type
Read the error message — it tells you exactly what's wrong

# Example Code
# Deliberately read a table that doesn't exist
df_bad = spark.read.format("delta").load("Tables/dbo/this_table_does_not_exist")
df_bad.show()

Output:
AnalysisException
[PATH_NOT_FOUND] Path does not exist: Tables/dbo/this_table_does_not_exist.


## Session 2B — Silver layer: MERGE + incremental watermark

### Full load vs incremental load — EXAM TOPIC
- **Full load**: read ALL source data, overwrite target every run
  - Simple but slow — reprocesses 3.8GB every night
  - Use for: small tables, first-time loads, reference data
- **Incremental load**: read only NEW/CHANGED records since last run
  - Faster, cheaper — only processes delta
  - Requires a watermark column (timestamp, ID, or date)

### Handle duplicates, nulls, late-arriving data — EXAM TOPIC
- **Duplicates**: df.dropDuplicates(["key_col1", "key_col2"])
- **Nulls**: df.fillna({"col": default_value}) or df.dropna()
- **Late-arriving**: watermark filter catches records that arrive after expected window

### MERGE / upsert pattern — EXAM TOPIC (write from memory)
```python
from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, "lh_silver.dbo.table_name")

silver_table.alias("silver") \
    .merge(df_new.alias("new"), "silver.key = new.key") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```
- whenMatchedUpdateAll() → UPDATE existing rows
- whenNotMatchedInsertAll() → INSERT new rows
- Result: upsert — no duplicates, always current

### Watermark pattern — EXAM TOPIC (write from memory)
```python
# 1. Get watermark from Silver (last processed timestamp)
watermark = silver_df.select(max("processed_at")).collect()[0][0]

# 2. Read only new records from Bronze
df_incremental = bronze_df.filter(col("load_date") > watermark)

# 3. MERGE incremental records into Silver
silver_table.merge(df_incremental, "silver.key = new.key") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

### Delta time travel
DeltaTable.forName(spark, "table").history() → see every operation
Every MERGE, overwrite, append is logged with version + timestamp

## Session 2C — Gold star schema + Warehouse

### Part 1 — Warehouse creation

**Theory:** A Fabric Warehouse is the third data store type (after Lakehouse and
Eventhouse). Full T-SQL read AND write — unlike the Lakehouse SQL endpoint which
is read-only. Under the hood it still stores data as Delta in OneLake, so Spark
can read Warehouse tables too. Choose Warehouse when: pure SQL team, dimensional
models, Gold serving layer. — EXAM TOPIC (choose appropriate data store)

**Warehouse T-SQL gaps vs SQL Server — EXAM TRAP:**
- No enforced PRIMARY KEY / FOREIGN KEY (metadata only, NOT ENFORCED)
- No triggers, no IDENTITY columns
- Limited data types (no money, no datetimeoffset, etc.)

### Part 2 — Cross-database queries

**Theory:** Every Lakehouse automatically gets a SQL analytics endpoint.
Any Warehouse in the workspace can read any Lakehouse via three-part naming —
like a linked server in SQL Server. Data never moves; the Warehouse reaches
across to Silver's endpoint. Writes fail (endpoint is read-only). — EXAM TOPIC

```sql
SELECT TOP 5 pwsid, violation_id, contaminant_code
FROM lh_silver.dbo.violations_pa_silver;
```

### Part 3 — Dimension tables via CTAS

**Theory:** CTAS (CREATE TABLE AS SELECT) is THE Warehouse loading pattern —
creates and populates in one statement. Dims hold descriptive attributes,
one row per entity, built with SELECT DISTINCT. — EXAM TOPIC (prepare data
for dimensional model)

```sql
-- One row per water system
CREATE TABLE dbo.dim_water_system
AS
SELECT DISTINCT
    pwsid, pws_type_code, population_served_count,
    primary_source_code, epa_region
FROM lh_silver.dbo.violations_pa_silver;

-- Code → name lookup; TRY_CAST filters non-numeric codes like '1***'
CREATE TABLE dbo.dim_contaminant
AS
SELECT DISTINCT
    CAST(VALUE_CODE AS BIGINT) AS contaminant_code,
    VALUE_DESCRIPTION AS contaminant_name
FROM lh_bronze.dbo.sdwa_ref_code_values
WHERE VALUE_TYPE = 'CONTAMINANT_CODE'
  AND TRY_CAST(VALUE_CODE AS BIGINT) IS NOT NULL;
```

### Part 4 — Fact table

**Theory:** The fact table holds events at a declared grain — here, one row =
one violation. Contains foreign keys to dims + measures/flags + dates.
Fact row count must match the source (172,851 = Silver count) — always verify.
Splitting one wide Silver table into dims + fact = the "denormalize data"
exam bullet (normalization direction), and joining them back at query time
is denormalization for analytics.

```sql
CREATE TABLE dbo.fact_violations
AS
SELECT
    pwsid, violation_id, contaminant_code, violation_code,
    violation_category_code, is_health_based_ind, compliance_status_code,
    CAST(compl_per_begin_date AS DATE) AS compl_begin_date,
    CAST(compl_per_end_date AS DATE) AS compl_end_date
FROM lh_silver.dbo.violations_pa_silver;

SELECT COUNT(*) AS fact_rows FROM dbo.fact_violations;  -- expect 172,851
```

### Part 5 — Analytical queries over Gold

**Theory:** These prove the star schema works and cover "group and aggregate
data" (Domain 2). Pattern the exam tests: JOIN fact→dim to translate codes
into names, GROUP BY the dim attribute, aggregate the fact rows. In the real
world these queries ARE the product — Q1 is AquaProof's core insight, Q4 is a
dashboard trend line, Q5 is a compliance alert.

```sql
-- Q1: Health-based violations by contaminant (JOIN + GROUP BY)
SELECT TOP 10 d.contaminant_name, COUNT(*) AS violation_count
FROM dbo.fact_violations f
JOIN dbo.dim_contaminant d ON f.contaminant_code = d.contaminant_code
WHERE f.is_health_based_ind = 'Y'
GROUP BY d.contaminant_name
ORDER BY violation_count DESC;

-- Q2: Violations by system type (conditional aggregation with CASE)
SELECT s.pws_type_code, COUNT(*) AS violations,
       SUM(CASE WHEN f.is_health_based_ind = 'Y' THEN 1 ELSE 0 END) AS health_based
FROM dbo.fact_violations f
JOIN dbo.dim_water_system s ON f.pwsid = s.pwsid
GROUP BY s.pws_type_code
ORDER BY violations DESC;

-- Q3: Largest systems with health violations (TOP + ORDER BY)
SELECT TOP 10 s.pwsid, s.population_served_count, COUNT(*) AS health_violations
FROM dbo.fact_violations f
JOIN dbo.dim_water_system s ON f.pwsid = s.pwsid
WHERE f.is_health_based_ind = 'Y'
GROUP BY s.pwsid, s.population_served_count
ORDER BY s.population_served_count DESC;

-- Q4: Time trend (date function in GROUP BY)
SELECT YEAR(compl_begin_date) AS viol_year, COUNT(*) AS violations
FROM dbo.fact_violations
GROUP BY YEAR(compl_begin_date)
ORDER BY viol_year DESC;

-- Q5: Open violations (NOT IN filter — compliance alert)
SELECT COUNT(*) AS open_violations
FROM dbo.fact_violations
WHERE compliance_status_code NOT IN ('K', 'R');
```

### Part 6 — T-SQL error drill

**Theory:** Domain 3 "identify and resolve T-SQL errors." Fabric Warehouse
throws the same error classes as SQL Server — read the message, it names
the problem.

```sql
-- Error 1: Invalid column name
SELECT pwsid, nonexistent_column FROM dbo.fact_violations;

-- Error 2: Writes to Lakehouse SQL endpoint fail even from Warehouse
UPDATE lh_silver.dbo.violations_pa_silver SET pwsid = 'X' WHERE 1=0;
```

Lesson: SQL endpoint read-only applies in BOTH directions — 1B proved it from
the Lakehouse side, this proves it from the Warehouse side.