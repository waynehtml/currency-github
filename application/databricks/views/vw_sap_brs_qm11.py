# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

env = dbutils.widgets.get("environment")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(env)

# COMMAND ----------

# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_sap_brs_qm11
AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY notification
            ORDER BY file_date DESC
        ) AS rn
    FROM {unity_catalog_name}.standardized_db.tbl_sap_brs_qm11
)
SELECT *
FROM ranked
WHERE rn = 1
""")
