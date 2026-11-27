# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * CUDT-363
# MAGIC * Steps:
# MAGIC   * Run this notebook via ADF 
# MAGIC * Tasks: 
# MAGIC   * Creates two views
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")


# COMMAND ----------

# DBTITLE 1,Deployment tasks
# MAGIC %run /currency/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Create view vw_sap_brs_qm11
# MAGIC %run /currency/views/vw_sap_brs_qm11

# COMMAND ----------

# DBTITLE 1,Create view vw_sap_brs_qm15
# MAGIC %run /currency/views/vw_sap_brs_qm15
