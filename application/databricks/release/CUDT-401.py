# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC _[This is a sample deployment instrction. Please replace with your content of the Jira task to be deployed.]_
# MAGIC * Prerequisite:
# MAGIC   * Deployment is completed.
# MAGIC * Steps:
# MAGIC   *  copy the rcmp folder landing zone of dev  in the folder land_currency/rcmp of the landing zone of the target env ( staging or prod) 
# MAGIC   * run PL_Control_Main_RCMP with parameters as follows:
# MAGIC     * folder: 2024-JAN
# MAGIC     * skip_Land_to_Raw : 0
# MAGIC     * skip_Raw_to_Standard : 0
# MAGIC     * skip_Standard_to_Curation : 1
# MAGIC     * load_type : incremental
# MAGIC     Repeat for 2024-FEB, 2024-MAR .. 2024-OCT
# MAGIC     
# MAGIC   Re-run Again
# MAGIC      
# MAGIC     * run PL_Control_Main_RCMP with parameters as follows:
# MAGIC     * folder: history
# MAGIC     * skip_Land_to_Raw : 0
# MAGIC     * skip_Raw_to_Standard : 0
# MAGIC     * skip_Standard_to_Curation : 0
# MAGIC     * load_type : initial_load
# MAGIC   
# MAGIC    
# MAGIC
# MAGIC     
# MAGIC
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

