# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# Get environment if not provided
env = dbutils.widgets.get("environment")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

import pyspark.sql.functions as F
table = f"{unity_catalog_name}.standardized_db.tbl_isa_inventorysnapshot"
df = spark.table(table)
df.count()

# COMMAND ----------

df = df.withColumn("denom_type", F.col("denom_category"))
df.count()

# COMMAND ----------

# DBTITLE 1,Step 1
# Conversion cases:
#   $25 Specimen note -> Specimen note
#   $1 STC Unfit -> STC Unfit 
df = (
    df.withColumn(
        "denom_type",
        F.when(
            F.col("denom_type").rlike(r"^\$\d+\s*"),               # only matching rows
            F.regexp_replace("denom_type", r"^\$\d+\s*", "")      # strip $number + space
        ).otherwise(F.col("denom_type"))                          # keep original
    )
)
df.count()

# COMMAND ----------

# DBTITLE 1,Step 1 - Results
df.filter(F.col("denom_category").startswith("$")).select("denom_category", "denom_type").distinct().display()

# COMMAND ----------

df.filter(F.col("denom_category").startswith("NV - POLYMER/POLYMERE")).select("denom_category", "denom_type").distinct().display()

# COMMAND ----------

# DBTITLE 1,Step 2
# Conversion cases:
#   100$ - SS - POLYMER/POLYMÈRE -> POLYMER/POLYMÈRE
#   50$ - REISS/REUT -> REISS/REUT
df = (
    df.withColumn(
        "denom_type",
        F.when(
            F.col("denom_type").rlike(r"^\d+\$.*-\s*"),
            F.regexp_replace("denom_type", r"^\d+\$.*-\s*", "") 
        ).otherwise(F.col("denom_type"))
    )
)
df.count()

# COMMAND ----------

# DBTITLE 1,Step 2 - Results
(df
    .filter(F.col("denom_category").rlike(r"^\d+\$"))
    .select("denom_category", "denom_type")
    .distinct()
    .display())
df.count()

# COMMAND ----------

df.filter(F.col("denom_category").startswith("NV - POLYMER/POLYMERE")).select("denom_category", "denom_type").distinct().display()

# COMMAND ----------

# DBTITLE 1,Step 3
# Conversion cases:
#   5$ SPECIAL FIT -> SPECIAL FIT
df = (
    df.withColumn(
        "denom_type",
        F.when(
            F.col("denom_type").rlike(r"^\d+\$+\s*"),                 # number + one or more $ + optional spaces
            F.regexp_replace("denom_type", r"^\d+\$+\s*", "")         # strip them all
        ).otherwise(F.col("denom_type"))
    )
)
df.count()


# COMMAND ----------

# DBTITLE 1,Step 3 - results
(df.filter(F.col("denom_category").rlike(r"^\d+\$"))
    .select("denom_category", "denom_type")
    .distinct()
    .display())

# COMMAND ----------

df.filter(F.col("denom_category").startswith("NV - POLYMER/POLYMERE")).select("denom_category", "denom_type").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Show Conversion Results

# COMMAND ----------

(df
    .select("denom_category", "denom_type")
    .distinct()
    .sort("denom_category")
    .display())

# COMMAND ----------

(df
    .select("denom_type")
    .distinct()
    .sort("denom_type")
    .display())

# COMMAND ----------

df.limit(5).display()
df.count()

# COMMAND ----------

df.write.format("delta").mode("overwrite").partitionBy('file_date').saveAsTable(table)
