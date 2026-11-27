# Databricks notebook source
# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "dev")

# COMMAND ----------

# MAGIC %run /currency/common/utils

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

