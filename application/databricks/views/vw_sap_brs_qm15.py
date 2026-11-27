# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_sap_brs_qm15
AS
SELECT
*
FROM {unity_catalog_name}.standardized_db.tbl_sap_brs_qm15
""")
