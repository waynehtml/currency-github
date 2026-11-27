# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook will perform following deployment tasks for CUDT-393**
# MAGIC - Replace wrong site name
# MAGIC
# MAGIC ## Parameters
# MAGIC - environment: dev|stg|prd
# MAGIC - update: false|true, only update when it is true, otherwise, only show the row count to be updated.

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()

dbutils.widgets.text("environment", "")
dbutils.widgets.dropdown("update", "false", ["true", "false"])

env = dbutils.widgets.get("environment")
print(f"env: {env}")
update = dbutils.widgets.get("update")
print(f"update: {update}")

# COMMAND ----------

# DBTITLE 1,Utility
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

# DBTITLE 1,Replacement definitions
replacements = [
    {
        "index": "1",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "machine_site",
        "new": "Toronto",
        "old": "Torronto"
    }
]

# COMMAND ----------

replace_column_value(unity_catalog_name, replacements, update)
