# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
internal_unity_catalog_name = get_interal_unity_catalog_name(dbutils.widgets.get("environment"))


# COMMAND ----------

# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_cur_bois_boisp_invent_boc_daily_snapshot
AS
SELECT
*
FROM {internal_unity_catalog_name}.cur_bois_db.cur_bois_boisp_invent_boc_daily_snapshot
""")
