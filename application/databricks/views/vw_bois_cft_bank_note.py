# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

env = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

internal_unity_catalog_name = get_interal_unity_catalog_name(env)
print(f"internal_unity_catalog_name: {internal_unity_catalog_name}")


# COMMAND ----------

# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_bois_cft_bank_note_info
AS
SELECT
    CAST(RECEIVED_DT AS DATE) AS RECEIVED_DT,
    CAST(BANK_NOTE_ID AS INTEGER) AS BANK_NOTE_ID,
    CAST(ADDRESS_LOC_ID AS INTEGER) AS ADDRESS_LOC_ID,
    CAST(CFT_CLASSIFICATION_CE AS STRING) AS CFT_CLASSIFICATION_CE,
    CAST(CFT_RECOVERY_TYPE_CE AS STRING) AS CFT_RECOVERY_TYPE_CE,
    CAST(SERIAL_NB AS STRING) AS SERIAL_NB,
    CAST(CFT_BANK_NOTE_CT AS INTEGER) AS CFT_BANK_NOTE_CT,
    CAST(CFT_BANK_NOTE_VALUE_AM AS INTEGER) AS CFT_BANK_NOTE_VALUE_AM,
    CAST(LOAD_ID AS INTEGER) AS LOAD_ID
FROM {internal_unity_catalog_name}.cur_bois_db.CUR_BOIS_BOISP_CFT_BANK_NOTE_INFO_T
""")

# COMMAND ----------

# DBTITLE 1,vw_bois_bank_note_dim
# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_bois_bank_note_dim
AS
SELECT
    CAST(BANK_NOTE_ID AS INTEGER) AS BANK_NOTE_ID,
    CAST(SERIES_YR AS INTEGER) AS SERIES_YR,
    CAST(BANK_NOTE_DENOM_AM AS INTEGER) AS BANK_NOTE_DENOM_AM,
    CAST(SECURITY_MTHD_TYPE_CE AS STRING) AS SECURITY_MTHD_TYPE_CE,
    CAST(INITL_ISSUE_DT AS DATE) AS INITL_ISSUE_DT,
    CAST(ISSUING_COUNTRY_CE AS STRING) AS ISSUING_COUNTRY_CE,
    CAST(LOAD_ID AS INTEGER) AS LOAD_ID
FROM {internal_unity_catalog_name}.cur_bois_db.cur_bois_boisp_bank_note_dim
""")

# COMMAND ----------

# DBTITLE 1,vw_bois_address_loc_info_dim
# create view
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_bois_address_loc_info_dim
AS
SELECT
    CAST(ADDRESS_LOC_ID AS INTEGER) AS ADDRESS_LOC_ID,
    CAST(COUNTRY_CE AS STRING) AS COUNTRY_CE,
    CAST(MUNICIPALITY_NM AS STRING) AS MUNICIPALITY_NM,
    CAST(PROV_OR_STATE_CE AS STRING) AS PROV_OR_STATE_CE,
    CAST(LOAD_ID AS INTEGER) AS LOAD_ID
FROM {internal_unity_catalog_name}.cur_bois_db.cur_bois_boisp_address_loc_info_dim_t
""")
