# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates curated tables for tbl_m7_unfits **
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC * Tasks: 
# MAGIC   * create table tbl_isa_reissnotes
# MAGIC   *  
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   * replace - false or true, replace if tables exist

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("replace", "false")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
replace = dbutils.widgets.get("replace")
print(f"replace: {replace}")

# COMMAND ----------

# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")
 

# COMMAND ----------

# DBTITLE 1,replace or skip if exists
# Define create action
create_action = 'CREATE OR REPLACE TABLE' if replace.lower() == 'true' else 'CREATE TABLE IF NOT EXISTS'
print(f"create_action: {create_action}")

# COMMAND ----------

# DBTITLE 1,tbl_m7_unfits
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.curated_db.tbl_m7_unfits"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    machine_serial_number INTEGER,
    shift_name STRING,
    shift_number INTEGER,
    shift_start_date DATE,
    shift_start_time STRING,
    shift_end_date DATE,
    shift_end_time STRING,
    headercard_id INTEGER,
    headercard_singled INTEGER,
    headercard_start_date DATE,
    headercard_start_time STRING,
    headercard_last_modify STRING,
    headercard_total_amount INTEGER,
    currency STRING,
    denomination STRING,
    machine_counter INTEGER,
    unfit_property STRING,
    unfit_property_count INTEGER,
    site_name STRING,
    ingestion_date DATE,
    nesid STRING,
    pcd STRING,
    created_date TIMESTAMP,
    nes_deposit_date DATE,
    nes_shipping_date TIMESTAMP,
    aoc_received_date TIMESTAMP,
    client_rdp STRING,
    client_fi STRING,
    cash_centre STRING,
    status STRING,
    pass_number INTEGER,
    status_date DATE,
    `Unfit Reason` STRING,
    `Unfit Grouping` STRING
) TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)
