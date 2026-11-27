# Databricks notebook source
# MAGIC %run ../common/utils

# COMMAND ----------

# Create widgets for passing parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("env", "")

# Get environment if not provided
env = dbutils.widgets.get("env")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(env)

# COMMAND ----------

# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vwu_qm_press
AS
SELECT
*
FROM {unity_catalog_name}.standardized_db.tbl_sap_brs_qm15
""")
