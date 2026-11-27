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

# DBTITLE 1,tbl_cbn_processlitho
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cbn_processlitho"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    record INTEGER,
    date DATE,
    lot INTEGER,
    dr_marks STRING,
    x_a DECIMAL(10, 3),
    x_b DECIMAL(10, 3),
    x_c DECIMAL(10, 3),
    x_d DECIMAL(10, 3),
    x_e DECIMAL(10, 3),
    x_a2 DECIMAL(10, 3),
    x_b2 DECIMAL(10, 3),
    x_c2 DECIMAL(10, 3),
    x_d2 DECIMAL(10, 3),
    x_e2 DECIMAL(10, 3),
    x_a3 DECIMAL(10, 3),
    x_b3 DECIMAL(10, 3),
    x_c3 DECIMAL(10, 3),
    x_d3 DECIMAL(10, 3),
    x_e3 DECIMAL(10, 3),
    scf_a INTEGER,
    scf_b INTEGER,
    scf_c INTEGER,
    scf_d INTEGER,
    scf_e INTEGER,
    scb_a INTEGER,
    scb_b INTEGER,
    scb_c INTEGER,
    scb_d INTEGER,
    scb_e INTEGER,
    file_name String,
    update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
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

# COMMAND ----------

# DBTITLE 1,tbl_cbn_processintaglio
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cbn_processintaglio"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    record INTEGER,
    date DATE,
    lot INTEGER,
    dr_marks STRING,
    st_a DECIMAL(10, 3),
    st_b DECIMAL(10, 3),
    st_c DECIMAL(10, 3),
    st_d DECIMAL(10, 3),
    st_e DECIMAL(10, 3),
    nm_a DECIMAL(10, 3),
    nm_b DECIMAL(10, 3),
    nm_c DECIMAL(10, 3),
    nm_d DECIMAL(10, 3),
    nm_e DECIMAL(10, 3),
    mag_a INTEGER,
    mag_b INTEGER,
    mag_c INTEGER,
    mag_d INTEGER,
    mag_e INTEGER,
    file_name String,
    update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
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

# COMMAND ----------

# DBTITLE 1,tbl_cbn_processtype
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cbn_processtype"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    record INTEGER,
    date DATE,
    lot INTEGER,
    num_a DECIMAL(10, 3),
    num_b DECIMAL(10, 3),
    num_c DECIMAL(10, 3),
    num_d DECIMAL(10, 3),
    num_e DECIMAL(10, 3),
    file_name String,
    update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
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

# COMMAND ----------

# DBTITLE 1,tbl_cbn_processtactile
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cbn_processtactile"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    record INTEGER,
    date DATE,
    lot INTEGER,
    covert STRING,
    thgt_a1 DECIMAL(10, 3),
    thgt_a2 DECIMAL(10, 3),
    thgt_a3 DECIMAL(10, 3),
    thgt_a4 DECIMAL(10, 3),
    thgt_b1 DECIMAL(10, 3),
    thgt_b2 DECIMAL(10, 3),
    thgt_b3 DECIMAL(10, 3),
    thgt_b4 DECIMAL(10, 3),
    thgt_c1 DECIMAL(10, 3),
    thgt_c2 DECIMAL(10, 3),
    thgt_c3 DECIMAL(10, 3),
    thgt_c4 DECIMAL(10, 3),
    thgt_d1 DECIMAL(10, 3),
    thgt_d2 DECIMAL(10, 3),
    thgt_d3 DECIMAL(10, 3),
    thgt_d4 DECIMAL(10, 3),
    thgt_e1 DECIMAL(10, 3),
    thgt_e2 DECIMAL(10, 3),
    thgt_e3 DECIMAL(10, 3),
    thgt_e4 DECIMAL(10, 3),
    file_name String,
    update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
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

# COMMAND ----------

# DBTITLE 1,tbl_cbn_processvarnish
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cbn_processvarnish"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    record INTEGER,
    lot INTEGER,
    date DATE,
    gf_a DECIMAL(10, 3),
    gf_b DECIMAL(10, 3),
    gf_c DECIMAL(10, 3),
    gf_d DECIMAL(10, 3),
    gf_e DECIMAL(10, 3),
    gf2_a DECIMAL(10, 3),
    gf2_b DECIMAL(10, 3),
    gf2_c DECIMAL(10, 3),
    gf2_d DECIMAL(10, 3),
    gf2_e DECIMAL(10, 3),
    gb_a DECIMAL(10, 3),
    gb_b DECIMAL(10, 3),
    gb_c DECIMAL(10, 3),
    gb_d DECIMAL(10, 3),
    gb_e DECIMAL(10, 3),
    gb2_a DECIMAL(10, 3),
    gb2_b DECIMAL(10, 3),
    gb2_c DECIMAL(10, 3),
    gb2_d DECIMAL(10, 3),
    gb2_e DECIMAL(10, 3),
    file_name String,
    update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
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

# COMMAND ----------

# DBTITLE 1,tbl_cbn_finishedsheet
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cbn_finishedsheet"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    record STRING,
    lot INTEGER,
    date DATE,
    covert STRING,
    x_a DECIMAL(10, 3),
    x_b DECIMAL(10, 3),
    x_c DECIMAL(10, 3),
    x_d DECIMAL(10, 3),
    x_e DECIMAL(10, 3),
    scf_a INTEGER,
    scf_b INTEGER,
    scf_c INTEGER,
    scf_d INTEGER,
    scf_e INTEGER,
    scb_a INTEGER,
    scb_b INTEGER,
    scb_c INTEGER,
    scb_d INTEGER,
    scb_e INTEGER,
    mag_a INTEGER,
    mag_b INTEGER,
    mag_c INTEGER,
    mag_d INTEGER,
    mag_e INTEGER,
    st_a DECIMAL(10, 3),
    st_b DECIMAL(10, 3),
    st_c DECIMAL(10, 3),
    st_d DECIMAL(10, 3),
    st_e DECIMAL(10, 3),
    nm_a DECIMAL(10, 3),
    nm_b DECIMAL(10, 3),
    nm_c DECIMAL(10, 3),
    nm_d DECIMAL(10, 3),
    nm_e DECIMAL(10, 3),
    thght_a1 DECIMAL(10, 3),
    thght_a2 DECIMAL(10, 3),
    thght_a3 DECIMAL(10, 3),
    thght_a4 DECIMAL(10, 3),
    thght_b1 DECIMAL(10, 3),
    thght_b2 DECIMAL(10, 3),
    thght_b3 DECIMAL(10, 3),
    thght_b4 DECIMAL(10, 3),
    thght_c1 DECIMAL(10, 3),
    thght_c2 DECIMAL(10, 3),
    thght_c3 DECIMAL(10, 3),
    thght_c4 DECIMAL(10, 3),
    thght_d1 DECIMAL(10, 3),
    thght_d2 DECIMAL(10, 3),
    thght_d3 DECIMAL(10, 3),
    thght_d4 DECIMAL(10, 3),
    thght_e1 DECIMAL(10, 3),
    thght_e2 DECIMAL(10, 3),
    thght_e3 DECIMAL(10, 3),
    thght_e4 DECIMAL(10, 3),
    num_a DECIMAL(10, 3),
    num_b DECIMAL(10, 3),
    num_c DECIMAL(10, 3),
    num_d DECIMAL(10, 3),
    num_e DECIMAL(10, 3),
    gf_a DECIMAL(10, 3),
    gf_b DECIMAL(10, 3),
    gf_c DECIMAL(10, 3),
    gf_d DECIMAL(10, 3),
    gf_e DECIMAL(10, 3),
    gb_a DECIMAL(10, 3),
    gb_b DECIMAL(10, 3),
    gb_c DECIMAL(10, 3),
    gb_d DECIMAL(10, 3),
    gb_e DECIMAL(10, 3),
    file_name String,
    update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
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
