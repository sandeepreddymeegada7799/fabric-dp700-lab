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