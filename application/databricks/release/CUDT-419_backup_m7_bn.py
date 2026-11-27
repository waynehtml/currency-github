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
# MAGIC   * backup standardized/internal/aoc_processing/m7/bn to standardized/internal/aoc_processing/m7/bn_nov_bak
# MAGIC   * execute notebook code with the parameters as listed below
# MAGIC * Parameters:
# MAGIC   * storage = "lakecacnpdstdev01|lakecacprdststg01|lakecacprdstprd01" (modify for environment)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

# create databricks widgets
storage = dbutils.widgets.text("storage", "", "storage")

# get user input values from widgets/ADF base parameters
container = "restricted"
storage = dbutils.widgets.get("storage")

# COMMAND ----------

# DBTITLE 1,Authentication
kv_scope = "lake-kv-currency-scope"
spn_id_secret = "spn-currencyservices-id"
spn_key_secret = "spn-currencyservices-key"
tenant_id_secret = "lake-tenant-id"

# try to enable access to storage via SPN authentication.
tryEnableSpnAccess(kv_scope, storage, spn_id_secret, spn_key_secret, tenant_id_secret)


# COMMAND ----------

# DBTITLE 1,backup the m7 bn data
# path to raw M7 bnlist data
bn_src = f"abfss://{container}@{storage}.dfs.core.windows.net/standardized/internal/aoc_processing/m7/bn"
bn_bak = f"abfss://{container}@{storage}.dfs.core.windows.net/standardized/internal/aoc_processing/m7/bn_nov_bak"

dbutils.fs.cp(bn_src, bn_bak, recurse=True)
