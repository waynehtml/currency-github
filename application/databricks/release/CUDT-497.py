# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC ** This notebook will perform following deployment tasks for CUDT-497**
# MAGIC - Create views for bois tables.

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")
env = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")


unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

internal_unity_catalog_name = get_interal_unity_catalog_name(env)
print(f"internal_unity_catalog_name: {internal_unity_catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create aoc_inventory views
# MAGIC %run ../views/vw_bois_cft_bank_note
