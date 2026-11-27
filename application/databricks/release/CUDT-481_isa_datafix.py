# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates tables for isa and m7 standardized data **
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC * Tasks: 
# MAGIC   * create table tbl_isa_reissnotes
# MAGIC   *  
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   * replace - false or true, replace if tables exist

# COMMAND ----------

# DBTITLE 1,Functions
from pyspark.sql.functions import col, concat, conv, countDistinct, explode, length, lit, lpad, map_keys, map_values, split, substring, when, expr, to_date, coalesce, array

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("replace", "false")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
replace = dbutils.widgets.get("replace")
print(f"replace: {replace}")

# COMMAND ----------

from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.functions import split, element_at

# COMMAND ----------

# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

storage = get_lake_storage_account(environment)
print(f"storage: {storage}")
 

# COMMAND ----------

# DBTITLE 1,function: load_data
def load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition=None):
    print(f"loading data from {file_rel_path} to {table_short_name}")

    # Compose the file path   
    file_full_path = f'abfss://restricted@{storage}.dfs.core.windows.net/standardized/internal/{file_rel_path}/*/'
    print(f"file_path: {file_full_path}")

    # Load data from file path
    df = spark.read.format("parquet").option("mergeSchema", "true").load(file_full_path)

    # add file_date column from file name
    df = df.withColumn("file_date", to_date(element_at(split(F.input_file_name(), "/"), -2), "yyyy-MM-dd"))

    if table_short_name == "tbl_isa_4ons":
        # drop duplicate records
        print(f"dropping duplicate records for {table_short_name}")
        df = df.withColumn("rank", expr("row_number() over(partition by seal_id order by created_date)"))
        df = df.filter(expr("rank = 1")).drop('rank')

    # Compose the table name
    table_full_name = f"{unity_catalog_name}.standardized_db.{table_short_name}"
    print(f"table_full_name: {table_full_name}")

    # Clear the table
    table = delta.DeltaTable.forName(spark, table_full_name)
    table.delete()

    # Write data to the table
    if partition is None:
        df.write.format('delta').mode('overwrite').saveAsTable(table_full_name)
    else:
        df.write.format('delta').mode('overwrite').partitionBy(partition).saveAsTable(table_full_name)
    
    print(f"{spark.table(table_full_name).count()} records loaded to {table_full_name}")

    return df

# COMMAND ----------

# DBTITLE 1,4000perparcel
file_name = "4000perparcel"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,4ons
file_name = "4ons"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,acdptrace
file_name = "acdptrace"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,deposittohc
file_name = "deposittohc"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,emscontainersnapshot
file_name = "emscontainersnapshot"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,extracthstbtransfer
file_name = "extracthstbtransfer"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,hctoclient
file_name = "hctoclient"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,incompletedepositinfo
file_name = "incompletedepositinfo"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,inventorysnapshot
file_name = "inventorysnapshot"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,extractreissnotes
file_name = "extractreissnotes"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,preadvisementdata
file_name = "preadvisementdata"
file_rel_path = f"inventory_supply_accounting/{file_name}"
table_short_name = f"tbl_isa_{file_name}"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)
