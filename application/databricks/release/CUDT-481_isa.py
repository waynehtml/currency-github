# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates tables for isa and m7 standardized data **
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

# DBTITLE 1,tbl_isa_4000PerParcel
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_4000PerParcel"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    nesid STRING,
    parcelid STRING,
    deposit_rule_en STRING,
    deposit_rule_fr STRING,
    client_rdp STRING,
    client_fi STRING,
    nbdenom INT,
    closed TIMESTAMP
) partitioned by (file_date)
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

# DBTITLE 1,4ons
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_4ons"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    seal_id STRING,
    created_date TIMESTAMP,
    denom_category STRING,
    denom_value STRING,
    denom_type STRING,
    cash_centre STRING
) partitioned by (file_date)
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

# DBTITLE 1,acdptrace
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_acdptrace"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    created_date TIMESTAMP,
    current_scan TIMESTAMP,
    user STRING,
    seq_number INT,
    header_card STRING,
    counting_link STRING,
    counting_state INT,
    machine_scan TIMESTAMP
) partitioned by (file_date)
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

# DBTITLE 1,deposittohc
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_deposittohc"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
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
    closed TIMESTAMP
) partitioned by (file_date)
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

# DBTITLE 1,emscontainersnapshot
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_emscontainersnapshot"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    snapshot_date TIMESTAMP,
    cash_centre STRING,
    ems_container_id STRING,
    ems_seal_id STRING,
    isa_container_id STRING,
    isa_seal_id STRING,
    container_type STRING,
    container_code INT,
    isa_location STRING,
    ems_positioncode STRING,
    ems_position INT,
    ems_vaultlocation STRING,
    ems_invaultflag INT,
    ems_roompositionid INT,
    ems_locationid INT
) partitioned by (file_date)
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

# DBTITLE 1,extracthstbtransfer
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_extracthstbtransfer"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    denom_category STRING,
    denom_value STRING,
    denom_type STRING,
    nbdenommachine INT,
    nbdenomtransfer INT,
    nbdenompcd INT,
    nbdenomreject INT,
    nbdenomsubtract INT,
    nbcorrected INT,
    hstb_transfer STRING,
    pcd_name STRING,
    status INT,
    rejnotes INT,
    hspdevname INT,
    creation TIMESTAMP,
    lastupdate TIMESTAMP,
    luserupdate STRING,
    seqnumber INT,
    lhstbtransfer STRING,
    cash_centre STRING
) partitioned by (file_date)
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

# DBTITLE 1,hctoclient
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_hctoclient"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    counting_link STRING,
    pcd STRING,
    created_date TIMESTAMP,
    machine_date TIMESTAMP,
    client_rdp STRING,
    client_fi STRING,
    valuecounting BIGINT,
    denom_category STRING,
    denom_value INT,
    nbdenom INT,
    nbreject INT,
    nbkeyin INT,
    count_value STRING,
    reject_value STRING,
    prep_value STRING,
    cash_centre STRING,
    denom_type_en STRING,
    denom_type_fr STRING,
    deposit_rule_en STRING,
    deposit_rule_fr STRING,
    special_stacker BOOLEAN,
    invalid_media_type BOOLEAN,
    closed TIMESTAMP
) partitioned by (file_date)
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

# DBTITLE 1,incompletedepositinfo
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_incompletedepositinfo"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    snapshot_date TIMESTAMP,
    sealid STRING,
    pcd STRING,
    client_code STRING,
    client_rdp STRING,
    client_fi STRING,
    cash_centre STRING,
    actual_counted_value DOUBLE,
    transaction_id STRING,
    deposit_rule STRING,
    deposit_rule_en STRING,
    deposit_rule_fr STRING,
    complete_flag STRING,
    nes_shipping_date TIMESTAMP,
    created_date TIMESTAMP,
    denom_category STRING,
    denom_value_source DOUBLE,
    denom_value DOUBLE,
    denom_type STRING,
    denom_type_en STRING,
    denom_type_fr STRING,
    invalid_media_type BOOLEAN,
    special_stacker BOOLEAN,
    counted_denom_pieces INT,
    counted_denom_total DOUBLE
) partitioned by (file_date)
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

# DBTITLE 1,inventorysnapshot
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_inventorysnapshot"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    snapshot_date DATE,
    cash_centre STRING,
    denom_category STRING,
    denom_count INT,
    denom_value DOUBLE,
    total_value DOUBLE,
    type_value STRING,
    denom_value_source DOUBLE,
    denom_type STRING,
    denom_type_en STRING,
    denom_type_fr STRING,
    invalid_media_type BOOLEAN,
    special_stacker BOOLEAN    
) partitioned by (file_date)
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

# DBTITLE 1,extractreissnotes
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_extractreissnotes"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    bd_shipment_id STRING,
    client_rdp STRING,
    client_fi STRING,
    bd_shipping_date DATE,
    bd_denom INT,
    bd_shipping_aoc STRING,
    bd_4on_id STRING,
    bd_num_notes STRING,
    ivrname STRING   
) partitioned by (file_date)
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

# DBTITLE 1,preadvisementdata
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_preadvisementdata"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    ingestion_date DATE,
    nesid STRING,
    created_date TIMESTAMP,
    nes_deposit_date DATE,
    nes_shipping_date TIMESTAMP,
    aoc_received_date TIMESTAMP,
    client_code STRING,
    client_rdp STRING,
    client_fi STRING,
    total_declared DOUBLE,
    pieces INT,
    declared_denom_value DOUBLE,
    denom_category STRING,
    denom_value DOUBLE,
    denom_type STRING,
    denom_type_en STRING,
    denom_type_fr STRING,
    invalid_media_type BOOLEAN,
    special_stacker BOOLEAN
) partitioned by (file_date)
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
