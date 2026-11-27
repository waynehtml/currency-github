# Databricks notebook source
# MAGIC %md
# MAGIC ## Import library, function and notebook

# COMMAND ----------

# DBTITLE 1,Import functions
from pyspark.sql import functions as f
from pyspark.sql.functions import current_timestamp
import pandas as pd
from io import BytesIO
from functools import reduce
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Load utility function
# MAGIC %run ../../common/utils

# COMMAND ----------

# MAGIC %run ../common/lab_schema

# COMMAND ----------

# MAGIC %run ../common/process

# COMMAND ----------

# DBTITLE 1,Retrieve parameters
# Create parameter widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("env","")
dbutils.widgets.text("storage","")
dbutils.widgets.text("file_date","")

# Get environment if not provided
env = dbutils.widgets.get("env")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
storage = dbutils.widgets.get("storage")
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Check file date if not provided
file_date = dbutils.widgets.get("file_date")
if (len(file_date.strip()) == 0):
    raise Exception("Parameter file_date is required.")
print(f"file_date: {file_date}")

# COMMAND ----------

# DBTITLE 1,Process files
# input file path in raw zone
raw_base_path = "raw/internal/qm/lab"
raw_full_path = f"abfss://restricted@{storage}.dfs.core.windows.net/{raw_base_path}/{file_date}"
print(f"raw_full_path: {raw_full_path}")

# Find all files
files = list_files(raw_full_path, '.csv')
print(f"files: {files}")

### FOR TESTING ONLY ###
#files = ['QA_Lab_Data_Batch_N250001_20250528.csv']

dfs = {}
for file_name in files:
    header(f"Processing file: {file_name}")

    ### Read file into spark binary file
    binary_file = spark.read.format("binaryFile").load(f"{raw_full_path}/{file_name}")

    ### Read metadata
    header("Read metadata from file", level=2)
    metadata = read_csv_cells(binary_file, ds_map['metadata'])
    print(f"original metadata ({len(metadata)}): {metadata}")
    # compose sampling_regime
    metadata['sampling_regime'] = f"{metadata.pop('sampling_regime_1')}{metadata.pop('sampling_regime_2')}"
    print(f"composed metadata ({len(metadata)}): {metadata}")

    ### Read table from csv file
    header("Read table from file", level=2)
    df = read_csv_table(binary_file, skiprows=8, header=0)
    print(f"original table ({len(df.columns)}): {df.columns})")
    df.display()

    ### standardize column names
    header("Standardize Column Names", level=2)
    df = clean_column_names(df)
    print(f"Standardized table ({len(df.columns)}): {df.columns})")

    #### Merge metadata columns
    header("Merge metadata columns", level=2)
    for key, value in metadata.items():
        df = add_lit_columns(df, **{key: value})
        #df = df.withColumn(key, F.lit(value))
    print(f"Merged table ({len(df.columns)}): {df.columns})")

    ### schema conversion
    header("Schema Conversion", level=2)
    match = ds_map['match'] if 'match' in ds_map else "rough"
    print(f"Match by : {match}")
    df = schema_convert_by_name(df, ds_map['schema'], match)
    print(f"Converted table ({len(df.columns)}): {df.columns})")

    ### add management columns
    header("Add management columns", level=2)
    df = df.withColumn('file_date', to_date(lit(file_date), "yyyy-MM-dd"));
    df = df.withColumn("file_name", F.lit(file_name))
    df = df.withColumn("update_time", current_timestamp())
    print(f"With management columns ({len(df.columns)}): {df.columns})")

    # Final dataframe
    header("Final dataframe", level=2)
    dfs[file_name] = df
    df.display()

### Write to UC
# Use reduce to union all DataFrames
combined_df = reduce(DataFrame.union, list(dfs.values()))
# catalog name
unity_catalog_name = get_unity_catalog_name(env)
# table name
uc_table_name = f"{unity_catalog_name}.standardized_db.{ds_map['table']}"
# write data to Unity Catalog
write_to_unity_catalog(combined_df, uc_table_name, file_date, "file_date", "append")
