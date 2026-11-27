# Databricks notebook source
# DBTITLE 1,Notebook Parameters
# Create parameter widgets
dbutils.widgets.text("env","")
dbutils.widgets.text("land_container","land-currency", "land_container")
dbutils.widgets.text("land_storage","", "land_storage")
dbutils.widgets.text("container","restricted", "container")
dbutils.widgets.text("storage","", "storage")
dbutils.widgets.text("file","hctoclient", "file")
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
                            .withColumn('denom_value',  f.when (col("denom").cast("int").isNull(),0 ) .otherwise(col("denom").cast("int"))) \
                            .withColumnRenamed('denom_name','denom_category' ) \
                            .withColumn('invalid_media_type',col("denom").cast("int").isNull()) \
                            .withColumn('denom_type', f.when ( col("denom").cast("int").isNotNull() , substring(substring_index('denom_category','-',-1),2,len('denom_category')) )  
                                            .otherwise(col("denom"))) \
                            .withColumnRenamed('created_date','created_dates' ) \
                            .withColumnRenamed('machine_date','machine_dates' ) \
                            .withColumn("created_date",f.to_timestamp(from_unixtime(unix_timestamp('created_dates',format=ISA_File_Headers[input_file_type][2].metadata['format'])))) \
                            .withColumn("machine_date",f.to_timestamp(from_unixtime(unix_timestamp('machine_dates',format=ISA_File_Headers[input_file_type][3].metadata['format'])))) \
                            .withColumnRenamed('cashcentre','cash_centre' ) \
                            .withColumnRenamed('closed','closed_date' ) \
                            .withColumn("closed",f.to_timestamp(from_unixtime(unix_timestamp('closed_date',format=ISA_File_Headers[input_file_type][17].metadata['format'])))) \
                            .withColumnRenamed('valuecounting','valuecountings' ) \
                            .withColumn('valuecounting',col("valuecountings").cast("long") ) \
                            .drop('valuecountings')
                            
df_calculated = df_calculated.withColumn("deposit_rule_en", f.split(f.col("deposit_rule"), " / ").getItem(0)).withColumn("deposit_rule_fr", f.trim(f.split(f.col("deposit_rule"), " / ").getItem(1)))
df_calculated = df_calculated.withColumn("denom_type_en", f.split(f.col("denom_type"), "/").getItem(0)).withColumn("denom_type_fr", f.trim(f.split(f.col("denom_category"), "/").getItem(1)))
df_calculated = df_calculated.drop("deposit_rule", "denom_type")
df_calculated = df_calculated.withColumn('deposit_rule_fr', f.regexp_replace(f.col("deposit_rule_fr"), 'M7', ''))
df_calculated = df_calculated.withColumn('special_stacker', f.col('denom_category').contains('- SS -')) 

# COMMAND ----------

# DBTITLE 1,Write data to Lake
df_calculated.select('ingestion_date','counting_link','pcd','created_date','machine_date','client_rdp','client_fi','valuecounting','denom_category','denom_value','nbdenom','nbreject','nbkeyin','count_value','reject_value','prep_value','cash_centre','denom_type_en', 'denom_type_fr', 'deposit_rule_en', 'deposit_rule_fr','special_stacker','invalid_media_type','closed').write.format("parquet").mode("overwrite").save(target_directory) 

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
df_calculated.select('file_date', 'ingestion_date','counting_link','pcd','created_date','machine_date','client_rdp','client_fi','valuecounting','denom_category','denom_value','nbdenom','nbreject','nbkeyin','count_value','reject_value','prep_value','cash_centre','denom_type_en', 'denom_type_fr', 'deposit_rule_en', 'deposit_rule_fr','special_stacker','invalid_media_type','closed').write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
