# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook will remove all mis-matched converted folders **

# COMMAND ----------

# DBTITLE 1,functions
from pyspark.sql.functions import col, concat, conv, countDistinct, explode, length, lit, lpad, map_keys, map_values, split, substring, when, expr, to_date, coalesce, array

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

def path_exists(path):
    try:
        # try to list the contents of the path; if it exists, it will return a list
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        # if an exception is raised, the path does not exist
        return False

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("env", "")
dbutils.widgets.text("date", "")

# Get parameters from widgets
env = dbutils.widgets.get("env")
print(f"env: {env}")
date = dbutils.widgets.get("date")
print(f"date: {date}")

# COMMAND ----------

# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# lake storage
storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# path to raw converted data
raw_c_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/converted"
print(f"raw_c_bnlist_path: {raw_c_bnlist_path}")

# date list
date_list = date.split(",")
print(f"date_list: {date_list}")

# COMMAND ----------

for date in date_list:
    print(f"processing date: {date}")

    # path to raw converted data for date
    raw_c_bnlist_date_path = f"{raw_c_bnlist_path}/{date}"
    print(f"raw_c_bnlist_date_path: {raw_c_bnlist_date_path}")

    # delete converted path
    if path_exists(raw_c_bnlist_date_path):
        print(f"deleting converted folder for {date}: {raw_c_bnlist_date_path}")
        dbutils.fs.rm(raw_c_bnlist_date_path, True)
