# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook will perform following deployment tasks for CUDT-393**
# MAGIC - Fix denom_value and denom_type for denom_category starting with non-number
# MAGIC
# MAGIC ## Parameters
# MAGIC - environment: dev|stg|prd

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()

dbutils.widgets.text("environment", "")

env = dbutils.widgets.get("environment")
print(f"env: {env}")

# COMMAND ----------

# DBTITLE 1,Utility
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

tbl_hstb = f"{unity_catalog_name}.standardized_db.tbl_isa_extracthstbtransfer"
print(f"tbl_hstb: {tbl_hstb}")

# COMMAND ----------

sql = f"""
UPDATE {tbl_hstb}
SET denom_value = NULL,
    denom_type = denom_category
WHERE NOT regexp_like(denom_category, '^[0-9].*');
"""
print(f"sql: {sql}")

# COMMAND ----------

spark.sql(sql)

# COMMAND ----------

#from pyspark.sql import functions as f
#df_inputfile = spark.read.table(tbl_hstb)
#df_calculated=df_inputfile.withColumn('starts_with_number', f.regexp_extract(col('denom_category'), '^\d', #0) != '') \
#                            .withColumn('denom_value', 
#                                when(col('starts_with_number'), substring_index('denom_category', '$', 1))
#                                .otherwise(lit(None))) \
#                            .withColumn('denom_type', 
#                                when(col('starts_with_number'), 
#                                    substring(substring_index('denom_category', '-', -1), 2, len('denom_category')))
#                                .otherwise(col('denom_category'))) \
#                            .drop('starts_with_number')
#df_calculated.write.mode("overwrite").saveAsTable(tbl_hstb)
