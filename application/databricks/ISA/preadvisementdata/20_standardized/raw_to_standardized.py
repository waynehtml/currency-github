# Databricks notebook source
# DBTITLE 1,Notebook Parameters
dbutils.widgets.removeAll()

# Create parameter widgets
dbutils.widgets.text("env","")
dbutils.widgets.text("container","restricted", "container")
dbutils.widgets.text("storage","", "storage")
dbutils.widgets.text("file","preadvisementdata", "file")
dbutils.widgets.text("date","", "date")

# Get widget parameters
env = dbutils.widgets.get("env")
print(f"env: {env}")
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
                                    .withColumnRenamed('denom','denom_category') \
                                    .withColumn("created_date",to_timestamp(from_unixtime(unix_timestamp('created_date',format=ISA_File_Headers[input_file_type][1].metadata['format']))))\
                                    .withColumn("nes_deposit_date", to_date(col("nes_deposit_date"), ISA_File_Headers[input_file_type][2].metadata['format']))   \
                                    .withColumn("nes_shipping_date",to_timestamp(from_unixtime(unix_timestamp('nes_shipping_date',format=ISA_File_Headers[input_file_type][3].metadata['format']))) )\
                                    .withColumn("aoc_received_date",to_timestamp( from_unixtime(unix_timestamp('aoc_received_date',format=ISA_File_Headers[input_file_type][4].metadata['format'])))) \
                                    .withColumn("client_rdp",substring('client_code',1,2))\
                                    .withColumn("client_fi",substring('client_code',4,3))\
                                    .withColumn("total_declared",col("total_declared").cast("double") )\
                                    .withColumn("pieces",col("pieces").cast("integer"))\
                                    .withColumn("declared_denom_value",col("declared_denom_value").cast("double"))\
                                    .withColumn('denom_values',substring_index('denom_category','$',1).cast("double") ) \
                                    .withColumn('invalid_media_type',col("denom_values").cast("int").isNull())\
                                    .withColumn('denom_value',when ( col("denom_values").cast("int").isNull(),0 ).otherwise(col("denom_values")) )\
                                    .withColumn("denom_type", split(col("denom_category"), "-").getItem(1))\
                                    .withColumn("denom_type_en", split(col("denom_type"), "/").getItem(0)).withColumn("denom_type_fr", trim(split(col("denom_category"), "/").getItem(1)))\
                                    .withColumn('special_stacker', col('denom_category').contains('- SS -'))

# COMMAND ----------

# DBTITLE 1,Write data to Lake
df_calculated.select('ingestion_date','nesid','created_date','nes_deposit_date','nes_shipping_date','aoc_received_date','client_code','client_rdp','client_fi','total_declared','pieces','declared_denom_value','denom_category','denom_value','denom_type','denom_type_en','denom_type_fr','invalid_media_type','special_stacker').write.format("parquet").mode("overwrite").save(target_directory)

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
df_calculated.select('file_date','ingestion_date','nesid','created_date','nes_deposit_date','nes_shipping_date','aoc_received_date','client_code','client_rdp','client_fi','total_declared','pieces','declared_denom_value','denom_category','denom_value','denom_type','denom_type_en','denom_type_fr','invalid_media_type','special_stacker').write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
