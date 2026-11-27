# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC _[This is a sample deployment instrction. Please replace with your content of the Jira task to be deployed.]_
# MAGIC * Prerequisite:
# MAGIC   * CUDT-401 has to be deployed first
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC   * run pipeline with parameters as follows:
# MAGIC     * 
# MAGIC * Tasks: 
# MAGIC   * create curated tbl_cft_rcmp - initial_load means full overwrite, incremental_load deletes and appends partition month
# MAGIC   * create view vw_cft_rcmp
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   * folder - partition ie "2024-JAN"
# MAGIC   * load_type - 1. initial_load  2. incremental_load 
# MAGIC   * date - date of the file to updated
# MAGIC * [Other]
# MAGIC   * anything that needs to be mentioned to the release manager

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run /currency/common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")

# COMMAND ----------


unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------


 

table_name = f"{unity_catalog_name}.curated_db.tbl_cft_rcmp"
stmt = f""" truncate table {table_name};"""
print(stmt)
spark.sql(stmt)


table_name = f"{unity_catalog_name}.standardized_db.tbl_rcmp_cft_boc_monthly"
stmt = f""" truncate table {table_name};"""
print(stmt)
spark.sql(stmt)

table_name = f"{unity_catalog_name}.standardized_db.tbl_bois_address_loc_info_dim"
stmt = f""" truncate table {table_name};"""
print(stmt)
spark.sql(stmt)


table_name = f"{unity_catalog_name}.standardized_db.tbl_bois_bank_note_dim"
stmt = f""" truncate table {table_name};"""
print(stmt)
spark.sql(stmt)


table_name = f"{unity_catalog_name}.standardized_db.tbl_bois_cft_bank_note_info"
stmt = f""" truncate table {table_name};"""
print(stmt)
spark.sql(stmt)



# truncate table stg_currency_cat.standardized_db.tbl_rcmp_cft_boc_monthly;
# truncate table stg_currency_cat.standardized_db.tbl_bois_address_loc_info_dim;
# truncate table stg_currency_cat.standardized_db.tbl_bois_bank_note_dim;
# truncate table stg_currency_cat.standardized_db.tbl_bois_cft_bank_note_info;
