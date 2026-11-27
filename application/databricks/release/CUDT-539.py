# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook reloads m7 shift data **
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

# DBTITLE 1,functions
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
    print(f"reading data from {file_full_path}")
    df = spark.read.format("parquet").option("mergeSchema", "true").load(file_full_path)
    # add file_date column from file name
    df = df.withColumn("file_date", to_date(element_at(split(F.input_file_name(), "/"), -2), "yyyy-MM-dd"))
    
    # Type conversions
    if table_short_name == "tbl_m7_counts":
        df = df.withColumn("value", col("value").cast(IntegerType()))


    # Compose the table name
    table_full_name = f"{unity_catalog_name}.standardized_db.{table_short_name}"
    print(f"table_full_name: {table_full_name}")

    # Clear the table
    print(f"clearing {table_full_name}")
    table = delta.DeltaTable.forName(spark, table_full_name)
    table.delete()

    # Write data to the table
    print(f"writing data to {table_full_name}")
    if partition is None:
        df.write.format('delta').mode('overwrite').saveAsTable(table_full_name)
    else:
        df.write.format('delta').mode('overwrite').partitionBy(partition).saveAsTable(table_full_name)
    print(f"{spark.table(table_full_name).count()} records loaded to {table_full_name}")

# COMMAND ----------

# DBTITLE 1,counters
file_rel_path = f"aoc_processing/m7/counters"
table_short_name = f"tbl_m7_counts"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,rejections
file_rel_path = f"aoc_processing/m7/rejections"
table_short_name = f"tbl_m7_rejects"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

# COMMAND ----------

# DBTITLE 1,unfits
file_rel_path = f"aoc_processing/m7/unfit"
table_short_name = f"tbl_m7_unfits"
partition = "file_date"
load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)
