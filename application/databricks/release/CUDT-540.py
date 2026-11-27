# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC This notebook will remove following columns from standardized_db.tbl_rcmp_cft_boc_monthly table:
# MAGIC - security_mthd_type_ce
# MAGIC - initl_issue_dt
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

table = f"{unity_catalog_name}.standardized_db.tbl_rcmp_cft_boc_monthly"
print(f"table: {table}")

# COMMAND ----------

# DBTITLE 1,Change table properties
sql = f"""
ALTER TABLE {table} SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
);
"""
print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Drop columns - security_mthd_type_ce, initl_issue_dt
sql = f"""
ALTER TABLE {table} DROP COLUMNS (security_mthd_type_ce, initl_issue_dt);
"""
print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,add new column - bank_note_series_code
sql_add_column = f"""
ALTER TABLE {table} ADD COLUMN bank_note_series_code STRING;
"""
print(f"sql_add_column: {sql_add_column}")
spark.sql(sql_add_column)
