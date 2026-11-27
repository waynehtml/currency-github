# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
# Create parameter widgets
dbutils.widgets.text("env","")
dbutils.widgets.text("land_container","land-currency", "land_container")
dbutils.widgets.text("land_storage","", "land_storage")
dbutils.widgets.text("container","restricted", "container")
dbutils.widgets.text("storage","", "storage")
dbutils.widgets.text("file","inventorysnapshot", "file")
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
                                    .withColumnRenamed('CASH_CENTRE_NAME','cash_centre' ) \
                                    .withColumn('denom_count',col("denom_count").cast("integer")) \
                                    .withColumnRenamed('denom_name','denom_category' )\
                                    .withColumnRenamed('TYPE_VALUE','type_value') \
                                    .withColumnRenamed('denom_value','denom_value_source') \
                                    .withColumn('denom_values',substring_index('denom_category','$',1).cast("double") ) \
                                    .withColumn('denom_type', when ( col("denom_values").cast("int").isNotNull(),substring(substring_index('denom_category','-',-1),2,len('denom_category'))).otherwise(col("denom_category"))) \
                                    .withColumn('total_value',col("total_value").cast("double")) \
                                    .withColumn('denom_value_source',col("denom_value_source").cast("double")) \
                                    .withColumn("denom_type_en", split(col("denom_type"), "/").getItem(0)).withColumn("denom_type_fr", trim(split(col("denom_type"), "/").getItem(1)))\
                                    .withColumn('invalid_media_type',col("denom_values").cast("int").isNull()) \
                                    .withColumn('special_stacker', col('denom_category').contains('- SS -')) \
                                    .withColumn('denom_value',when ( col("denom_values").cast("int").isNull(),0 ).otherwise(col("denom_values")) )\
                                    .withColumn("snapshot_date",to_date(col("snapshot_date"), ISA_File_Headers[input_file_type][0].metadata['format']))

# COMMAND ----------

(df_calculated
    .select("denom_type")
    .distinct()
    .sort("denom_type")
    .display())

# COMMAND ----------

# Overwrite denom_type with denom_category
df_calculated = df_calculated.withColumn("denom_type", F.col("denom_category"))

# Conversion cases:
#   $25 Specimen note -> Specimen note
#   $1 STC Unfit -> STC Unfit 
df_calculated = (
    df_calculated.withColumn(
        "denom_type",
        F.when(
            F.col("denom_type").rlike(r"^\$\d+\s*"),              # only matching rows
            F.regexp_replace("denom_type", r"^\$\d+\s*", "")      # strip $number + space
        ).otherwise(F.col("denom_type"))                          # keep original
    )
)

# Conversion cases:
#   100$ - SS - POLYMER/POLYMÈRE -> POLYMER/POLYMÈRE
#   50$ - REISS/REUT -> REISS/REUT
df_calculated = (
    df_calculated.withColumn(
        "denom_type",
        F.when(
            F.col("denom_type").rlike(r"^\d+\$.*-\s*"),               # only matching rows
            F.regexp_replace("denom_type", r"^\d+\$.*-\s*", "")      # strip $number + space
        ).otherwise(F.col("denom_type"))                          # keep original
    )
)

# Conversion cases:
#   5$ SPECIAL FIT -> SPECIAL FIT
df_calculated = (
    df_calculated.withColumn(
        "denom_type",
        F.when(
            F.col("denom_type").rlike(r"^\d+\$+\s*"),                 # number + one or more $ + optional spaces
            F.regexp_replace("denom_type", r"^\d+\$+\s*", "")         # strip them all
        ).otherwise(F.col("denom_type"))
    )
)

# COMMAND ----------

# DBTITLE 1,Write data to Lake
df_calculated.select('snapshot_date','cash_centre','denom_category','denom_count','denom_value','total_value','type_value','denom_value_source','denom_type','denom_type_en','denom_type_fr','invalid_media_type','special_stacker','ingestion_date').write.format("parquet").mode("overwrite").save(target_directory)

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
df_calculated.select('file_date', 'snapshot_date','cash_centre','denom_category','denom_count','denom_value','total_value','type_value','denom_value_source','denom_type','denom_type_en','denom_type_fr','invalid_media_type','special_stacker','ingestion_date').write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
