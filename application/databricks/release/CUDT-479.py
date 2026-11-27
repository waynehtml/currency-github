# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * Deploy Denom mapping file Denom_type_inventory_mapping.csv to load(f"abfss://{config-currency}@{lakecacnpdstdevland01}.dfs.core.windows.net/inventory/mapping/Denom_type_inventory_mapping.csv")
# MAGIC Run Denom loading script
# MAGIC Run /Common/PL_Run_Notebook_UC pipeline with following parameters with parameters:
# MAGIC notebook: /currency/curated/aoc_inventory_backlog_denom_tbl_loading
# MAGIC parameter: {"environment":"stg","land_storage":"lakecacprdststgland01"}
# MAGIC
# MAGIC * Steps:
# MAGIC   * run this notebook for code deployment for CUDT -479
# MAGIC   * run UC_INV_CURATION_branch pipeline with parameters as follows:
# MAGIC     
# MAGIC * Parameters:
# MAGIC     * environment - deployment environment
# MAGIC     * date - date of the file to updated
# MAGIC     * file_type: aoc_inventory_backlog
# MAGIC     * container: restricted
# MAGIC     * land_container: land-currency
# MAGIC     * tenant_id_secret: lake-tenant-id
# MAGIC     * spn_key_secret: spn-currencyservices-key
# MAGIC     * spn_id_secret: spn-currencyservices-id
# MAGIC     * kv_scope: lake-kv-currency-scope
# MAGIC     * load_type: initial_load
# MAGIC     * mode : overwrite
# MAGIC   * on the day of deployment the load_type should be initial_load and from next day the load_type should be incremental_load
# MAGIC

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
internal_unity_catalog_name = get_interal_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Create aoc_inventory views
# MAGIC %run /currency/views/vw_aoc_inventory_curated
