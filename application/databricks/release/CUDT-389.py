# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * Make sure that CUDT-414 has already deployed 
# MAGIC
# MAGIC * Tasks :
# MAGIC   * create curated tbl_inventory_daily
# MAGIC   * create curated tbl_denom_type_inv_mapping
# MAGIC   * create view vw_tbl_inventory_daily
# MAGIC   * create view vw_denom_type_inv_mapping
# MAGIC * Steps:
# MAGIC   * run this notebook for code deployment for CUDT -389
# MAGIC   * run UC_INV_CURATION_branch pipeline with parameters as follows:
# MAGIC     
# MAGIC * Parameters:
# MAGIC     * environment - deployment environment
# MAGIC     * date - date of the file to updated
# MAGIC     * file_type: aoc_inventory_backlog
# MAGIC     * container: restricted
# MAGIC     * land_container: land-currency
# MAGIC     * tenant_id_secret: lake-tenant-id
# MAGIC     * spn_key_secret: spn-currencyservices-key
# MAGIC     * spn_id_secret: spn-currencyservices-id
# MAGIC     * kv_scope: lake-kv-currency-scope
# MAGIC     * load_type: initial_load
# MAGIC     * mode : overwrite
# MAGIC   * on the day of deployment the load_type should be initial_load and from next day the load_type should be incremental_load
# MAGIC
# MAGIC
# MAGIC
# MAGIC *As part of the JIRA ticket CUDT - 389, we have created views under this location 
# MAGIC   /currency/views/aoc_inventory_curated_views
# MAGIC   * Please deploy the views after we run the below notebook

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
internal_unity_catalog_name = get_interal_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Deployment tasks
# Define the Unity Catalog table path
table_path = f"{unity_catalog_name}.curated_db.tbl_inventory_daily"

# SQL query to create an empty Delta table with the specified schema
query = f"""
CREATE OR REPLACE TABLE {table_path}
(
    INVENT_SNAPSHOT_DAY_DT DATE,
    INVENT_TYPE_CE STRING,
    BANK_NOTE_DENOM_AM DECIMAL(10, 2),
    BOC_LOC_ID STRING,
    SOURCE_CE STRING,
    INVENT_NOTE_VALUE_AM DECIMAL(38, 2),  -- Ensuring precision
    INVENT_NOTE_QT DECIMAL(38, 2),  -- Ensuring precision
    oldest_nes_deposit_date DATE,
    oldest_aoc_received_date DATE,
    inventory_type_code STRING,
    inventory_type_category STRING,
    inventory_type_subcategory STRING,
    inventory_type_name STRING
)
USING DELTA
"""

# Execute the query to create the empty table
spark.sql(query)
print(f"Empty table '{table_path}' has been created successfully.")


# COMMAND ----------

# Define the Unity Catalog table path
table_path = f"{unity_catalog_name}.curated_db.tbl_denom_type_inv_mapping"

# SQL query to create an empty Delta table with the specified schema
query = f"""
CREATE OR REPLACE TABLE {table_path}
(
    inventory_type_code STRING,
    inventory_type_category STRING,
    inventory_type_subcategory STRING,
    inventory_type_name STRING
)
USING DELTA
"""

# Execute the query to create the empty table
spark.sql(query)
print(f"Empty table '{table_path}' has been created successfully.")


# COMMAND ----------

# DBTITLE 1,Create aoc_inventory views
# MAGIC %run /currency/views/vw_aoc_inventory_curated
