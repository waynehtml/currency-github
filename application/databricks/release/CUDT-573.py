# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC This notebook will rename columns from convert to covert.
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

# DBTITLE 1,Update in cur_bois_boisp_cft_bank_note_info_t
table = f"{unity_catalog_name}.standardized_db.tbl_sap_brs_qm15"
print(f"table: {table}")


sql = f"""
DELETE FROM {table}
WHERE notification_date BETWEEN DATE '2024-12-01' AND DATE '2025-12-31';
"""

print(f"sql: {sql}")
spark.sql(sql)
