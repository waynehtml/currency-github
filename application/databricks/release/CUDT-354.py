# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook will perform following deployment tasks for CUDT-354**
# MAGIC - Fix invalid_media_type in tbl_isa_preadvisementdata 
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

tbl_isa_preadvisementdata = f"{unity_catalog_name}.standardized_db.tbl_isa_preadvisementdata"
print(f"tbl_isa_preadvisementdata: {tbl_isa_preadvisementdata}")

# COMMAND ----------

sql = f"""
UPDATE {tbl_isa_preadvisementdata}
SET invalid_media_type = CASE WHEN denom_value = 0 THEN true ELSE false END;
"""
print(f"sql: {sql}")

# COMMAND ----------

spark.sql(sql)
