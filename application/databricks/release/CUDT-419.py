# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC * Tasks: 
# MAGIC   * merge m7 bn list raw unconverted and converted data, standardize it, and write to standardized zone in .parquet format over a period of time from start_date to end_date
# MAGIC   * execute notebook code with the parameters as listed below
# MAGIC * Parameters:
# MAGIC   * container = "restricted"
# MAGIC   * storage = "lakecacnpdstdev01" (modify for environment)
# MAGIC   * land_storage = "lakecacnpdstdevland01" (modify for environment)
# MAGIC   * kv_scope = "lake-kv-currency-scope"
# MAGIC   * spn_id_secret = "spn-currencyservices-id"
# MAGIC   * spn_key_secret = "spn-currencyservices-key"
# MAGIC   * tenant_id_secret = "lake-tenant-id"
# MAGIC   * mode = "overwrite"
# MAGIC   * site = "maoc"
# MAGIC   * start_date = "2023-04-16"
# MAGIC   * end_date = "2024-09-25"
# MAGIC   
# MAGIC * [Other]
# MAGIC   * notebook can be executed from the following path:
# MAGIC   /Workspace/currency/m7/bn/20_standardized/raw_to_standardized_historical
# MAGIC   in the ADB environment:
# MAGIC   lake-cac-npd-dbw-dev01

# COMMAND ----------

# DBTITLE 1,Common
# load utility notebook

# COMMAND ----------

# DBTITLE 1,Get Parameters


# COMMAND ----------

# DBTITLE 1,Deployment tasks
#

