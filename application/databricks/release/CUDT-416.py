# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * Create Azure Key Vault secret:
# MAGIC     crdp-dscurr-oppl-s-pass which holds the password for the user crdp-dscurr-oppl-s required to read the source file from the R Drive
# MAGIC     same password in all environment, it exists in Dev
# MAGIC * Steps:
# MAGIC   * Create new tables in env_currency_cat 
# MAGIC * Tasks: 
# MAGIC   * Creates two tables
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC * Notes:
# MAGIC   * Will need to coordinate to get historical data loaded.  It will use the same software, but will be a one off execution.  At this moment, we do not have this file
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
# MAGIC

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Create Standardized Table
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cision_media_bank_note_media_info"
create_table_sql = f"""
CREATE OR REPLACE TABLE {table_name} (
    media_info_id string, -- 16-bit uuid represented as string
    media_docno string,
    media_title string,
    media_publication_dt timestamp,
    media_source_nm string,
    media_type string,
    media_sub_type string,
    continent string,
    prov_state string,
    country string,
    town string,
    read_area_nb int,
    tone string,
    ingestion_date TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 5 day',
    'delta.deletedFileRetentionDuration' = 'interval 5 day',
    'delta.checkpointInterval' = '100'
)
"""

# Execute the SQL command to create the table
spark.sql(create_table_sql)

# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_cision_media_tag_association"
create_table_sql = f"""
CREATE OR REPLACE TABLE {table_name} (
    media_info_id string, -- 16-bit uuid represented as string
    group string,
    label string,
    ingestion_date TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 5 day',
    'delta.deletedFileRetentionDuration' = 'interval 5 day',
    'delta.checkpointInterval' = '100'
)
"""

# Execute the SQL command to create the table
spark.sql(create_table_sql)

