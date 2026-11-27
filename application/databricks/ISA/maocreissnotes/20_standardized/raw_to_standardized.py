# Databricks notebook source
# DBTITLE 1,Notebook Parameters
# Create parameter widgets
dbutils.widgets.text("env","")
dbutils.widgets.text("land_container","land-currency", "land_container")
dbutils.widgets.text("land_storage","", "land_storage")
dbutils.widgets.text("container","restricted", "container")
dbutils.widgets.text("storage","", "storage")
dbutils.widgets.text("file","maocreissnotes", "file")
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
if input_file_type == 'maocreissnotes':
    t_input_file_type = 'extractreissnotes'
else:
    t_input_file_type = input_file
print(f"input_file_type: {t_input_file_type}")

# input file path in raw zone
input_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{t_input_file_type}/{input_date}/"
print(f"input_directory: {input_directory}")

# output file path in standardized zone
target_directory =  f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{t_input_file_type}/{input_date}/"
print(f"target_directory: {target_directory}")

# target table name
unity_catalog_name = get_unity_catalog_name(env)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_{t_input_file_type}"

# COMMAND ----------

# DBTITLE 1,Load and process the input file
df_inputfile = spark.read.option("header",True).schema(ISA_File_Headers[input_file_type]).csv(input_directory)

df_calculated=df_inputfile.withColumn('ingestion_date',current_date() ) \
                            .withColumn('client_rdp', substring(substring_index('bd_rdp_id','-',-1),1,len('bd_rdp_id')) )  \
                            .withColumn('client_fi', substring_index('bd_rdp_id','-',1) ) \
                            .withColumnRenamed('bd_shipping_date','bd_shipping_dates') \
                            .withColumn('bd_shipping_date',to_date(col("bd_shipping_dates"),ISA_File_Headers[input_file_type][2].metadata['format']))

# COMMAND ----------

# DBTITLE 1,Write data to Lake
df_calculated.select('ingestion_date','bd_shipment_id','client_rdp','client_fi','bd_shipping_date','bd_denom','bd_shipping_aoc','bd_4on_id','bd_num_notes','ivrname').write.format("parquet").mode("overwrite").save(target_directory) 

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
df_calculated.select('file_date', 'ingestion_date','bd_shipment_id','client_rdp','client_fi','bd_shipping_date','bd_denom','bd_shipping_aoc','bd_4on_id','bd_num_notes','ivrname').write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
