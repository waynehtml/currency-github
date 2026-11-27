# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC This notebook will delete file_date=2025-07-14 in standardized_db.tbl_isa_emscontainersnapshot table
# MAGIC
# MAGIC ## Parameters
# MAGIC - environment: dev|stg|prd

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# DBTITLE 1,Utility
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get environment if not provided
env = dbutils.widgets.get("environment")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

table = f"{unity_catalog_name}.standardized_db.tbl_isa_emscontainersnapshot"
print(f"table: {table}")

sql = f"""
DELETE FROM {table}
WHERE file_date = '2025-07-14'
"""
print(f"sql: {sql}")

# COMMAND ----------

spark.sql(sql)
