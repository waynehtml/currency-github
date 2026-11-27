# Databricks notebook source
# MAGIC %md
# MAGIC ## Import library, function and notebook

# COMMAND ----------

# DBTITLE 1,Import functions
from pyspark.sql import functions as f

# COMMAND ----------

# DBTITLE 1,Load utility function
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,load Common Definition
# MAGIC %run ./common

# COMMAND ----------

# DBTITLE 1,Load Dataset Definition
# MAGIC %run ./dataset

# COMMAND ----------

# DBTITLE 1,Retrieve parameters
# Create parameter widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("env","")
dbutils.widgets.text("storage","")
dbutils.widgets.dropdown("file_type", "ssn", ["ssn", "opp", "dailyprocessing", "bpsreport", "productionstats","containeraoc", "containerqa"])
dbutils.widgets.text("file_date","")
dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"])

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

# Check file type if not provided
file_type = dbutils.widgets.get("file_type")
if (len(file_type.strip()) == 0):
    raise Exception("Parameter file_type is required.")
print(f"file_type: {file_type}")

# Check file date if not provided
file_date = dbutils.widgets.get("file_date")
if (len(file_date.strip()) == 0):
    raise Exception("Parameter file_date is required.")
print(f"file_date: {file_date}")

# Get widget parameters
mode = dbutils.widgets.get("mode")
print(f"mode: {mode}")

# COMMAND ----------

# DBTITLE 1,Read raw files
# input file path in raw zone
raw_base_path = "raw/external/cbn"
raw_full_path = f"abfss://restricted@{storage}.dfs.core.windows.net/{raw_base_path}/{file_type}/{file_date}/*"
print(f"raw_full_path: {raw_full_path}")

# 
format = dataset[file_type]['format']
print(f"format: {format}")
if (format == "xml"):
    # root tag
    root_tag = dataset[file_type]['rootTag']
    print(f"root_tag: {root_tag}")
    # row tag
    row_tag = dataset[file_type]['rowTag']
    print(f"row_tag: {row_tag}")
    # schema
    schema = dataset[file_type]['schema']
    print(f"schema: {schema}")
    df = read_xml_files(raw_full_path, schema, root_tag, row_tag)
else:
    raise Exception(f"Unsupported file type: {file_type}")

# COMMAND ----------

# DBTITLE 1,Rename columns
df = rename_columns(df, schema)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Perform Standard Transformation
df = perform_standard_transform(df, file_type, file_date)
df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Perfrom Dataset Specific Transformation
df = perform_dataset_transform(df, file_type, file_date)
df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Write data to UC
# catalog name
unity_catalog_name = get_unity_catalog_name(env)
# table name
uc_table_name = f"{unity_catalog_name}.standardized_db.tbl_cbn_{file_type}"

# write data to Unity Catalog
write_to_unity_catalog(df, uc_table_name, file_date, "file_date", mode)
