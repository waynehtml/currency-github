# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None 
# MAGIC
# MAGIC * Steps:
# MAGIC   * notebook does not need to be run
# MAGIC     
# MAGIC * Tasks: 
# MAGIC   * transform m7 bn list standardized data to build the 1on and 4on serial numbers for banknote tracking by serial number and write to the curated zone
# MAGIC
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
# MAGIC   * date = "2024-01-15"
# MAGIC   

# COMMAND ----------

# DBTITLE 1,Common
# load utility notebook

# COMMAND ----------

# DBTITLE 1,Get Parameters


# COMMAND ----------

# DBTITLE 1,Deployment tasks
#

