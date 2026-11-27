# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC _[This is a sample deployment instrction. Please replace with your content of the Jira task to be deployed.]_
# MAGIC * Prerequisite:
# MAGIC   * CUDT-414 has to be deployed first
# MAGIC * Steps:
# MAGIC   * Upload the below csv file attached to the JIRA ticket in this location - abfss://restricted@lakecacnpdstdev01.dfs.core.windows.net/standardized/internal/inventory_supply_accounting/mapping/Denom_type_inventory_mapping.csv
# MAGIC     * file: Denom_type_inventory_mapping.csv
# MAGIC

# COMMAND ----------

# DBTITLE 1,Common
# load utility notebook

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("date", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
date = dbutils.widgets.get("date")

# COMMAND ----------

# DBTITLE 1,Deployment tasks
#

