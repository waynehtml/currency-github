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
internal_unity_catalog_name = get_interal_unity_catalog_name(env)
print(f"internal_unity_catalog_name: {internal_unity_catalog_name}")

# COMMAND ----------

sql = f"DROP TABLE IF EXISTS {unity_catalog_name}.standardized_db.tbl_bois_address_loc_info_dim"
print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

sql = f"DROP TABLE IF EXISTS {unity_catalog_name}.standardized_db.tbl_bois_bank_note_dim"
print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

sql = f"DROP TABLE IF EXISTS {unity_catalog_name}.standardized_db.tbl_bois_cft_bank_note_info"
print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,recrate views
# MAGIC %run ../views/vw_bois_cft_bank_note
