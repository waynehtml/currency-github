# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC _[This is a sample deployment instrction. Please replace with your content of the Jira task to be deployed.]_
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC   *  
# MAGIC     * 
# MAGIC * Tasks: 
# MAGIC   * create table tbl_isa_reissnotes   
# MAGIC   *  
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   *  
# MAGIC   *  
# MAGIC   *  
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
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_reissnotes"
create_table_sql = f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_isa_reissnotes (
    ingestion_date Date,
    bd_shipment_id STRING,
    client_rdp STRING,
    client_fi STRING,
    bd_shipping_date DATE,
    bd_denom INT,
    bd_shipping_aoc STRING,
    bd_4on_id STRING,
    bd_num_notes STRING,
    ivrname STRING
) partitioned by (ingestion_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
# Execute the SQL command to create the table
spark.sql(create_table_sql)
