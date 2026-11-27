# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * Create azure key vault for sap-sftp-user-password.  It's the same password as in dev.
# MAGIC * Steps:
# MAGIC   * Create new tables in env_currency_cat 
# MAGIC * Tasks: 
# MAGIC   * Creates two tables
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC * Notes:
# MAGIC   * Currently, the ADF pipeline does NOT remove the source file.  This would be coordinated with the current feed in production that will continue to run.
# MAGIC   * The global parameter for pgm_sap_sftp_host will be the same for Dev & Stg, but it should be mapped to the value OPP9LSAPEFT01 when migrated to Prod.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")


# COMMAND ----------

# DBTITLE 1,Deployment tasks
# MAGIC %run /currency/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Create Standardized Table
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_sap_brs_qm11"
create_table_sql = f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_sap_brs_qm11 (
    file_date STRING,
    notification STRING NOT NULL,
    `required_start` DATE,
    `required_end` DATE,
    `notification_status` STRING NOT NULL,
    description STRING,
    customer STRING,
    `created_by` STRING NOT NULL,
    `changed_by` STRING,
    `created_on` DATE NOT NULL,
    `changed_on` DATE,
    `notification_date` DATE,
    `reference_date` DATE NOT NULL,
    `coding_code` STRING,
    `claimed_value` STRING,
    `return_delivery_qty` INT,
    `date_of_ret_delivery` DATE,
    `reference_number` DECIMAL(15,2),
    `substrate_type` STRING,
    `coding_code_text` STRING,
    `postal_Code` STRING,
    city STRING,
    `country_key` STRING,
    region STRING,
    `fi/acc/cu/police` STRING NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.logRetentionDuration' = 'interval 7 days'
);
"""
# Execute the SQL command to create the table
spark.sql(create_table_sql)

optimize_sql = f"""OPTIMIZE {unity_catalog_name}.standardized_db.tbl_sap_brs_qm11 ZORDER BY (reference_date);"""
spark.sql(optimize_sql)

# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_sap_brs_qm15"
create_table_sql = f"""

CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_sap_brs_qm15 (
    file_date STRING,
    `item_number` STRING NOT NULL,
    notification STRING NOT NULL,
    `notification_date` DATE NOT NULL,
    customer STRING,
    `required_start` DATE,
    `required_end` DATE,
    `defect_class` STRING,
    assembly STRING,
    `description_of_assembly` STRING,
    `defective_qty_external` DECIMAL(15,3) NOT NULL,
    `unit_of_measure` STRING,
    `defective_qty_internal` DECIMAL(15,3) NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.logRetentionDuration' = 'interval 7 days'
);
"""

# Execute the SQL command to create the table
spark.sql(create_table_sql)

optimize_sql = f"""OPTIMIZE {unity_catalog_name}.standardized_db.tbl_sap_brs_qm11 ZORDER BY (notification_date);"""
spark.sql(optimize_sql)
