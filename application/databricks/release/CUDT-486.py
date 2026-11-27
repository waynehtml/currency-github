# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook fixes the data issue in tbl_rcmp_cft_boc_monthly**
# MAGIC

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters

# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

# catalog name
unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

sql = f"update {unity_catalog_name}.standardized_db.tbl_rcmp_cft_boc_monthly tbl_rcmp_cft_boc_monthly set series_yr='2012' where  partition = '2024-DEC' and series_yr='201?' and cft_classification_ce like 'PF20%'"
spark.sql(sql)
