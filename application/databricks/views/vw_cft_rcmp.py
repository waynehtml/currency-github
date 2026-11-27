# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

# Get environment if not provided
env = dbutils.widgets.get("environment")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(env)


# COMMAND ----------

# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_cft_rcmp
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.tbl_cft_rcmp
""")

# COMMAND ----------


