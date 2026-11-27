# Databricks notebook source
# MAGIC %md
# MAGIC This script restores files from stroage to UC.
# MAGIC * table_n%md
# MAGIC This script backs up tables from UC to storage.
# MAGIC * env: environment, dev|stg|prd. If omitted, will infered automatically.
# MAGIC * storage: storage account
# MAGIC * container: container on the storage
# MAGIC * path: the file path for the backup in the container on the storage
# MAGIC * table_name: table names as [schema].[table] and seperated by ',', ex. standardized_db.tbl_standardized_cds_fips_clean_chris_v4_test_legacydata,standardized_db.test_liq_trans. If empty, all tables in the path on the storage will be restored.
# MAGIC * include: comma seperated keywords which must contain in table names
# MAGIC * exclude: comma sepearted keywords which must not contain in table names
# MAGIC * generate_sql_only: no or yes, generate sql only or run sql
# MAGIC table names as [schema].[table] and seperated by ',', ex. standardized_db.tbl_standardized_cds_fips_clean_chris_v4_test_legacydata,standardized_db.test_liq_trans. If empty, all tables in the storage path will be restored.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("env", "")
dbutils.widgets.text("storage", "")
dbutils.widgets.text("container", "restricted")
dbutils.widgets.text("path", "standardized/internal/inventory_supply_accounting/sync_uc")
dbutils.widgets.text("catalog", "currency_cat")
dbutils.widgets.text("table", "")
dbutils.widgets.text("include", "")
dbutils.widgets.text("exclude", "")
dbutils.widgets.text("suffix", "")
dbutils.widgets.dropdown("generate_sql_only", "no", ["no", "yes"])

env = dbutils.widgets.get("env").strip().lower();
print(f"env={env}");
storage = dbutils.widgets.get("storage").strip().lower();
print(f"storage={storage}");
container = dbutils.widgets.get("container").strip().lower();
print(f"container={container}");
path = dbutils.widgets.get("path").strip().lower();
print(f"path={path}");
catalog = dbutils.widgets.get("catalog").strip().lower();
print(f"catalog={catalog}");
table = dbutils.widgets.get("table").strip().lower();
print(f"table={table}");
include = dbutils.widgets.get("include").strip().lower();
print(f"include={include}");
exclude = dbutils.widgets.get("exclude").strip().lower();
print(f"exclude={exclude}");
suffix = dbutils.widgets.get("suffix").strip().lower();
print(f"suffix={suffix}");
generate_sql_only = dbutils.widgets.get("generate_sql_only")
print(f"generate_sql_only={generate_sql_only}");

# COMMAND ----------

# DBTITLE 1,Utility
# MAGIC %run ../utils

# COMMAND ----------

def filter_table_list(table_list, include_list, exclude_list):
    """
    Filters a list of table names based on inclusion and exclusion keywords.

    Args:
        table_list: A list of strings representing table names.
        include_list: A list of strings. Table names must contain at least one of these keywords to be included.
        exclude_list: A list of strings. Table names must *not* contain any of these keywords to be included.

    Returns:
        A new list of strings containing only the table names that meet the inclusion and exclusion criteria.
    """

    filtered_list = []
    for table_name in table_list:
        # Inclusion Check
        if include_list:  # Check if include_list is not empty
            include_match = False
            for include_keyword in include_list:
                if include_keyword.lower() in table_name.lower():
                    include_match = True
                    break  # Stop checking if one keyword matches
            if not include_match:
                continue  # Skip to the next table if no include keywords match

        # Exclusion Check
        exclude_match = False
        for exclude_keyword in exclude_list:
            if exclude_keyword.lower() in table_name.lower():
                exclude_match = True
                break  # Stop checking if one keyword matches
        if exclude_match:
            continue  # Skip to the next table if an exclude keyword matches

        filtered_list.append(table_name)  # Add the table to the filtered list

    return filtered_list

# COMMAND ----------

# Get environment
env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# sync_path
sync_path = f"abfss://{container}@{storage}.dfs.core.windows.net/{path}"
print(f"sync_path: {sync_path}")

# full catalog name
if not catalog:
    raise Exception("catalog can not be empty.")
else:
    catalog = f"{env}_{catalog}"
print(f"catalog: {catalog}")

# include
if len(include) > 0:
    include_list = [name.strip().lower() for name in include.split(',')]
else:
    include_list = []

# exclude
if len(exclude) > 0:
    exclude_list = [name.strip().lower() for name in exclude.split(',')]
else:
    exclude_list = []

# COMMAND ----------

def get_subdirs(base_path):
    subdirs = [f.name.rstrip('/') for f in dbutils.fs.ls(base_path) if f.isDir()]
    return subdirs

# COMMAND ----------

# DBTITLE 1,find all tables
if len(table) > 0:
    table_list = [name.strip().lower() for name in table.split(',')]
else:
    table_list = get_subdirs(sync_path)
    table_list = filter_table_list(table_list, include_list, exclude_list)

print(f"Final tables: {table_list}")

# COMMAND ----------

# DBTITLE 1,Sync tables
for table in table_list:
    print(f"Restoring: {table}")
    
    clone_sql = f"""
    CREATE OR REPLACE TABLE {catalog}.{table}{suffix} DEEP CLONE  delta.`{sync_path}/{table}`
    """
    print(f"clone_sql: {clone_sql}")
    if generate_sql_only == 'no':
        spark.sql(clone_sql)
