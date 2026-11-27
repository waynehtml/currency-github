# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates tables for 4ons curated data **
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC * Tasks: 
# MAGIC   * create table tbl_4ons
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

# DBTITLE 1,4ons
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.curated_db.tbl_4ons"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    isa_file_date DATE,
    isa_ingestion_date DATE,
    isa_seal_id STRING,
    isa_created_date TIMESTAMP,
    isa_denom_category STRING,
    isa_denom_value STRING,
    isa_denom_type STRING,
    isa_1on_sn_base36 STRING,
    isa_1on_sn STRING,
    bn_file_date DATE,
    bn_ingestion_date DATE,
    bn_1on_sn STRING,
    bn_date DATE,
    bn_time STRING,
    bn_msec INT,
    bn_machine_serialnumber STRING,
    bn_boc_machine_num STRING,
    bn_shift_number STRING,
    bn_stacker STRING,
    bn_block_seq_num INT,
    bn_serial_number STRING,
    bn_sorting_class STRING,
    bn_denomination STRING,
    bn_header_card_number STRING,
    bn_site_name STRING
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
