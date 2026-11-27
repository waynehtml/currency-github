# Databricks notebook source
# MAGIC %md
# MAGIC ## Import library, function and notebook

# COMMAND ----------

# DBTITLE 1,Import functions
from pyspark.sql import functions as f
import pandas as pd
from io import BytesIO
import os

# COMMAND ----------

# DBTITLE 1,Load utility function
# MAGIC %run ../../common/utils

# COMMAND ----------

# MAGIC %run ../common/press_schema

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

# DBTITLE 1,Read raw files
# input file path in raw zone
raw_base_path = "raw/external/cbn/press"
raw_full_path = f"abfss://restricted@{storage}.dfs.core.windows.net/{raw_base_path}/{file_date}/*"
print(f"raw_full_path: {raw_full_path}")

# Read in as Pandas
binary_file = spark.read.format("binaryFile").load(raw_full_path)
file_name = os.path.basename(binary_file.select("path").first()[0])
pds = pd.read_excel(BytesIO(binary_file.select("content").first()[0]), sheet_name=None, header=0, dtype=str)

# COMMAND ----------

# DBTITLE 1,Process Sheets
# catalog name
unity_catalog_name = get_unity_catalog_name(env)

# Interate each dataset defined in schema
for ds in ds_map:
    header(f"Processing sheet: {ds['name']}")

    # Find matching sheet in the file
    header("Finding matching sheet", level=2)
    sheet = match_names_by_tags(ds['tag'].split(','), pds.keys(),'rough')
    if sheet is None:
        raise Exception(f"{ds['name']} not found in file")
    else:
        print(f"{ds['name']} found in file with sheet name: {sheet}")

    # Get top n columns from the sheet according to the column count in schema
    # add extra 10 for extra columns in sheet
    # limit columns from the sheet according to schema since varnish sheet have over 16384 columns, which will cause out of memory error
    pdf = pds[sheet].iloc[:,:len(ds['schema'])+10]
    print(pdf)
    
    # Convert pands dataframe to spark
    header("Convert Panda to Spark", level=2)
    df = spark.createDataFrame(pdf)
    df.printSchema()
    print(f"Original: {len(df.columns)} columns")

    # standardize column names - remove spaces and lower case
    header("Standardize Column Names", level=2)
    df = clean_column_names(df)
    df.printSchema()
    print(f"Standardized: {len(df.columns)} columns")

    # schema conversion - rename, cast and fill null columns
    header("Schema Conversion", level=2)
    #the_selector = ds['selector'] if 'selector' in ds else selector
    #print(f"Match columns by : {the_selector}")
    match = ds['match'] if 'match' in ds else "exact"
    print(f"Match by : {match}")
    df = schema_convert_by_name(df, ds['schema'], match)
    df.printSchema()
    print(f"Converted: {len(df.columns)} columns")

    # add management columns
    header("Add management columns", level=2)
    df = add_lit_columns(df, file_date=to_date(lit(file_date), "yyyy-MM-dd"))
    df = add_lit_columns(df, file_name=file_name)
    df = add_lit_columns(df, update_time=current_timestamp())
    df.printSchema()
    print(f"With lit columns: {len(df.columns)} columns")

    # Show final Dataframe
    header("Final Dataframe", level=2)
    df.printSchema()
    print(f"Final: {len(df.columns)} columns")
    df.display()

    ### Write to UC
    header("Write to UC", level=2)
    # table name
    uc_table_name = f"{unity_catalog_name}.standardized_db.{ds['table']}"
    # write data to Unity Catalog
    # append: delete and insert, overwrite: overwrite the table
    write_to_unity_catalog(df, uc_table_name, file_date, "file_date", "append")
