# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Tasks :
# MAGIC   * create curated tbl_ems_containers_daily
# MAGIC   * create view tbl_ems_containers_daily
# MAGIC
# MAGIC * Steps:
# MAGIC   * run this notebook for code deployment for CUDT -440
# MAGIC   * run pipeline with parameters as follows:
# MAGIC     
# MAGIC * Parameters:
# MAGIC     * environment - deployment environment
# MAGIC     * date - date of the file to updated
# MAGIC     * container: restricted
# MAGIC     * storage: lake storage
# MAGIC     * load_type: initial_load or incremental_load
# MAGIC   * on the day of deployment the load_type should be initial_load and from next day the load_type should be incremental_load
# MAGIC
# MAGIC
# MAGIC *As part of the JIRA ticket CUDT - 440, we have created views under this location 
# MAGIC   /currency/views/ems_curated_views
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

# COMMAND ----------

# DBTITLE 1,Deployment tasks
# Define the Unity Catalog table path
table_path = f"{unity_catalog_name}.curated_db.tbl_ems_containers_daily"

# Define the SQL query to create the table with the specified columns and data types
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_path} (
    snapshot_date DATE,
    cash_centre STRING,
    container_type STRING,
    isa_location STRING,
    containers LONG
)
USING DELTA
"""

# Execute the query to create the table
spark.sql(create_table_query)
print(f"Table '{table_path}' has been created successfully with the specified schema.")


# COMMAND ----------

# MAGIC %run /currency/views/vw_ems_containers_curated

# COMMAND ----------


