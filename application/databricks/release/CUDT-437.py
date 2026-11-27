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

# DBTITLE 1,Create Curated Table
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.curated_db.tbl_cft_rcmp"
create_table_sql = f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.curated_db.tbl_cft_rcmp (
    received_dt DATE NOT NULL,
    cft_classification_ce STRING NOT NULL,
    cft_recovery_type_ce STRING NOT NULL,
    serial_nb STRING NOT NULL,
    cft_bank_note_ct BIGINT NOT NULL,
    cft_bank_note_value_am BIGINT NOT NULL,
    series_yr STRING,
    bank_note_denom_am INT,
    security_mthd_type_ce STRING,
    initl_issue_dt DATE,
    issuing_country_ce STRING NOT NULL,
    country_ce STRING NOT NULL,
    municipality_nm STRING NOT NULL,
    prov_or_state_ce STRING NOT NULL,
    partition STRING NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
# Execute the SQL command to create the table
spark.sql(create_table_sql)

optimize_sql = f"""OPTIMIZE {unity_catalog_name}.curated_db.tbl_cft_rcmp ZORDER BY (received_dt);"""
spark.sql(optimize_sql)

# COMMAND ----------

# DBTITLE 1,Deployment tasks
#%run /currency/curated/rcmp_cft_standardized_to_curated



# COMMAND ----------

# MAGIC %run /currency/views/vw_cft_rcmp
# MAGIC
