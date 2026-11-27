# Databricks notebook source
# DBTITLE 1,Notebook Parameters
# Create parameter widgets
dbutils.widgets.text("env","")
dbutils.widgets.text("land_container","land-currency", "land_container")
dbutils.widgets.text("land_storage","", "land_storage")
dbutils.widgets.text("container","restricted", "container")
dbutils.widgets.text("storage","", "storage")
dbutils.widgets.text("file","4000PerParcel", "file")
dbutils.widgets.text("date","", "date")

# Get widget parameters
env = dbutils.widgets.get("env")
print(f"env: {env}")
land_container = dbutils.widgets.get("land_container")
print(f"land_container: {land_container}")
land_storage = dbutils.widgets.get("land_storage")
print(f"land_storage: {land_storage}")
container = dbutils.widgets.get("container")
print(f"container: {container}")
storage = dbutils.widgets.get("storage")
print(f"storage: {storage}")
input_file = dbutils.widgets.get("file")
print(f"input_file: {input_file}")
input_date = dbutils.widgets.get("date")
print(f"input_date: {input_date}")

# COMMAND ----------

# DBTITLE 1,Load utility function
# MAGIC %run /currency/common/utils

# COMMAND ----------

# DBTITLE 1,load common definition
# MAGIC %run /currency/ISA/common/definition

# COMMAND ----------

# DBTITLE 1,Import functions
from pyspark.sql import functions as f

# COMMAND ----------

# DBTITLE 1,Prepare variables
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get land storage if not provided
if len(land_storage.strip()) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage: {land_storage}")

# Get lake storage if not provided
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Compose input and output file path
input_file_type = input_file.lower()
print(f"input_file_type: {input_file_type}")

# input file path in raw zone
input_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_file_type}/{input_date}/"
print(f"input_directory: {input_directory}")

# output file path in standardized zone
target_directory =  f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{input_file_type}/{input_date}/"
print(f"target_directory: {target_directory}")

# target table name
unity_catalog_name = get_unity_catalog_name(env)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_{input_file_type}"

# COMMAND ----------

# DBTITLE 1,Load and process the input file
df_inputfile = spark.read.option("header",True).schema(ISA_File_Headers[input_file_type]).csv(input_directory)

df_calculated=df_inputfile.withColumn('ingestion_date',current_date() ) \
                            .withColumnRenamed('closed','raw_closed') \
                            .withColumnRenamed('client_fi','client_fi_raw') \
                            .withColumn("closed",f.to_timestamp(from_unixtime(unix_timestamp('raw_closed',format=ISA_File_Headers[input_file_type][6].metadata['format'])))) \
                            .withColumn("deposit_rule_en", f.split(f.col("deposit_rule"), " / ").getItem(0)).withColumn("deposit_rule_fr_calc", f.trim(f.split(f.col("deposit_rule"), " / ").getItem(1))) \
                            .withColumn("deposit_rule_fr",f.regexp_replace(f.col("deposit_rule_fr_calc"), 'M7', '')) \
                            .withColumn('client_fi',f.regexp_replace(f.col("client_fi_raw"), '-M7', ''))

# COMMAND ----------

# DBTITLE 1,Write data to Lake
df_calculated.select('ingestion_date','nesid','parcelid','deposit_rule_en','deposit_rule_fr','client_rdp','client_fi','nbdenom','closed').write.format("parquet").mode("overwrite").save(target_directory)

# COMMAND ----------

# DBTITLE 1,Write data to UC
# Remove existing data with same file_date
try :
    dt = delta.DeltaTable.forName(spark, target_table_name)
    dt.delete(col("file_date") == to_date(lit(input_date), "yyyy-MM-dd"))
except (Exception) as ex:
    print(f"Failed to remove the existing data from {target_table_name}, due to {ex}")

# Write data to target table
df_calculated = df_calculated.withColumn("file_date", to_date(lit(input_date), "yyyy-MM-dd"))
df_calculated.select('file_date', 'ingestion_date','nesid','parcelid','deposit_rule_en','deposit_rule_fr','client_rdp','client_fi','nbdenom','closed').write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
