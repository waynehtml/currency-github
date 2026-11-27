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
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_inventory_daily
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.tbl_inventory_daily
""")

# COMMAND ----------

# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_denom_type_inv_mapping
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.tbl_denom_type_inv_mapping
""")
