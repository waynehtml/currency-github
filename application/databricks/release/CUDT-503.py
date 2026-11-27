# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates curated tables for tbl_m7_isa_note_processing **
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

# DBTITLE 1,tbl_m7_isa_note_processing
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.curated_db.tbl_m7_isa_note_processing"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    machine_date TIMESTAMP,
    valuecounting LONG,
    denom_value INTEGER,
    nbdenom INTEGER,
    nbreject INTEGER,
    nbkeyin INTEGER,
    count_value STRING,
    reject_value STRING,
    prep_value STRING,
    denom_type_en STRING,
    denom_type_fr STRING,
    deposit_rule_en STRING,
    deposit_rule_fr STRING,
    special_stacker BOOLEAN,
    invalid_media_type BOOLEAN,
    nesid STRING,
    pcd STRING,
    created_date TIMESTAMP,
    nes_deposit_date DATE,
    nes_shipping_date TIMESTAMP,
    aoc_received_date TIMESTAMP,
    client_rdp STRING,
    client_fi STRING,
    cash_centre STRING,
    ingestion_date DATE
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
