# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates tables for QA Lab data **
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
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

# DBTITLE 1,tbl_qm_labdata
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_qm_labdata"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    denomination STRING,
    batch_order STRING,
    batch_processing_date DATE,
    report_date DATE,
    number_of_special_fit INTEGER,
    visual_inspection_failed INTEGER,
    infrared_inspection_failed INTEGER,
    consol_1 STRING,
    consol_2 STRING,
    consol_3 STRING,
    qcd_used STRING,
    sampling_regime STRING,
    qa_lab_recommendation STRING,
    serial_number STRING,
    soft_mag INTEGER,
    hard_mag DECIMAL(10, 2),
    sc_f INTEGER,
    sc_b INTEGER,
    dr STRING,
    bnr_dnm INTEGER,
    ir_a DECIMAL(10, 2),
    ir_b DECIMAL(10, 2),
    ir_c DECIMAL(10, 2),
    hlas INTEGER,
    tf_1 INTEGER,
    tf_2 INTEGER,
    tf_3 INTEGER,
    tf_4 INTEGER,
    gloss_f DECIMAL(10, 2),
    gloss_b DECIMAL(10, 2),
    vis_insp STRING,
    bn_size STRING,
    foil STRING,
    doe3 STRING,
    bnr STRING,
    litho STRING,
    intag STRING,
    number STRING,
    tf_reg STRING,
    tape STRING,
    doe3_test STRING,
    comment STRING,
    process STRING,
    process_2 STRING,
    process_3 STRING,
    ir_insp STRING,
    batch STRING,
    file_name String,
    update_time TIMESTAMP
)
USING DELTA
partitioned by (file_date)
TBLPROPERTIES (
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
