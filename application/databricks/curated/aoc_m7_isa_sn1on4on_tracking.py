# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC - Process standardized m7 BNList data and ISA 4on Data for banknote tracking

# COMMAND ----------

# MAGIC %md
# MAGIC #Setup

# COMMAND ----------

# DBTITLE 1,import statements
import io
from datetime import datetime
from pyspark.sql.functions import col, concat, conv, countDistinct, explode, length, lit, lpad, map_keys, map_values, split, substring, when, expr, to_date, coalesce, array, udf
from pyspark.sql.types import BooleanType, MapType, StringType

# COMMAND ----------

# DBTITLE 1,parameter input setup
# create databricks widgets
env = dbutils.widgets.text("env", "")
land_storage = dbutils.widgets.text("land_storage", "", "land_storage")
file_date = dbutils.widgets.text("date", "", "date")
load_type = dbutils.widgets.dropdown("load_type", "initial_load", ["initial_load", "incremental_load"])
debug = dbutils.widgets.dropdown("debug", "false", ["false", "true"])

# get user input values from widgets/ADF base parameters
env = dbutils.widgets.get("env")
print(f"env: {env}")
land_storage = dbutils.widgets.get("land_storage")
print(f"land_storage: {land_storage}")
file_date = dbutils.widgets.get("date")
print(f"file_date: {file_date}")
load_type = dbutils.widgets.get("load_type")
print(f"load_type: {load_type}")
debug = dbutils.widgets.get("debug")
print(f"debug: {debug}")

# COMMAND ----------

# DBTITLE 1,load utils notebook
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,file paths
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get land storage if not provided
if len(land_storage.strip()) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage: {land_storage}")

# machine mapping data
machine_data_path = f"abfss://config-currency@{land_storage}.dfs.core.windows.net/m7/machine_data.json"
print(f"machine_data_path: {machine_data_path}")

# catalog name
unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# M7 bnlist table
m7_bnlist_table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_bnlist"
print(f"m7_bnlist_table_name: {m7_bnlist_table_name}")

# ISA 4ons table
isa_4ons_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_4ons"
print(f"isa_4ons_table_name: {isa_4ons_table_name}")

# curated 4ons table name
curated_4ons_table_name = f"{unity_catalog_name}.curated_db.tbl_4ons"
print(f"curated_4ons_table_name: {curated_4ons_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Process M7 bnlist data

# COMMAND ----------

# DBTITLE 1,bn - Reading machine number mapping file
# read machine number mapping file from config-currency blob container
machine_data_df = spark.read.json(machine_data_path, multiLine=True)
machine_data_df = machine_data_df.withColumnRenamed("boc_machine_number","boc_machine_num")
machine_data_df.display()

# COMMAND ----------

# DBTITLE 1,bn - Reading data
# Read bn data - always read all bn data no matter what load_type is
bnlist_sql = f"""SELECT * FROM {m7_bnlist_table_name}"""
print(f"bnlist_sql: {bnlist_sql}")
sdz_bnlist_df_full = spark.sql(bnlist_sql)
print(f"Count: {sdz_bnlist_df_full.count()}")

# if no data for the given date, exit the notebook
if sdz_bnlist_df_full.count() == 0:
        dbutils.notebook.exit('no_date')

# display for debugging
sdz_bnlist_df_full.limit(100).display()

# COMMAND ----------

# DBTITLE 1,bn - filtering and merging
## Filter out records based on sorting class
# (i.e. "All Paper","Unfit","Reject")
sorting_classes_to_exclude = ["All Paper","Unfit","Reject"]
sdz_bnlist_df = sdz_bnlist_df_full.filter(~sdz_bnlist_df_full["sorting_class"].isin(sorting_classes_to_exclude))
print(f"Count after filtering by sorting classes: {sdz_bnlist_df.count()}")

## Merge reserved14, reserved148, reserved149 into block_seq_num
# Commented out since merge is done in standardized table
#sdz_bnlist_df = sdz_bnlist_df.withColumn("block_seq_num", coalesce(
#    sdz_bnlist_df["reserved14"], sdz_bnlist_df["reserved148"], sdz_bnlist_df["reserved149"]))

# display for debugging
sdz_bnlist_df.limit(100).display()

# COMMAND ----------

# DBTITLE 1,define UDF function to generate 1on serial number
@udf(returnType=StringType())
def generate_1on_sn(date, boc_machine_num, shift_number, stacker, block_seq_num):
    try:
        # Date processing
        date_parts = str(date).split("-")
        year = date_parts[0][-2:]    # Last 2 digits of year
        month = date_parts[1]        # Month
                
        # Get stacker number
        stacker_num = stacker.split(" ")[1]
        
        ## Build 1ON serial number
        # 1on serial number format:
        # year 2 digit
        # month 2 digit, 0 padded
        # machine 1 digit
        # shift 4 digit, 0 padded
        # stacker 2 digit, 0 padded
        # block seq 3 digit, 0 padded
        str_1on_sn = f"{year}{str(month).zfill(2)}{str(boc_machine_num)}{str(shift_number).zfill(4)}{str(stacker_num).zfill(2)}{str(block_seq_num).zfill(3)}"
        return (str_1on_sn)
    except Exception as e:
        return None

# COMMAND ----------

# DBTITLE 1,Generate 1on serial number
## machine number
# get boc_machine_num from machine data mapping by joining machine_sn with machine_serial_number
# only join dataframes if not already joined (useful for debugging during development, shouldn't affect production code execution)
if "boc_machine_num" not in sdz_bnlist_df.columns:
  sdz_bnlist_df = sdz_bnlist_df.join(machine_data_df, sdz_bnlist_df["machine_serialnumber"] == machine_data_df["machine_serial_number"],"left")

# Apply single UDF to generate 1on serial number
sdz_bnlist_df = sdz_bnlist_df.withColumn(
    "1on_sn",
    generate_1on_sn(
        col("date"),
        col("boc_machine_num"),
        col("shift_number"),
        col("stacker"),
        col("block_seq_num")
    )
)

# for debugging
cols_1on_sn = ("1on_sn","date","boc_machine_num","shift_number","stacker","block_seq_num")
sdz_bnlist_df.select(*cols_1on_sn).limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Process ISA 4on data

# COMMAND ----------

# DBTITLE 1,4on - read data
isa_4ons_sql = f"""SELECT * FROM {isa_4ons_table_name}"""
print(f"isa_4ons_sql: {isa_4ons_sql}")

sdz_4on_df = spark.sql(isa_4ons_sql)
print(f"Total 4on loaded: {sdz_4on_df.count()}")

# display for debugging
sdz_4on_df.limit(100).display()

# COMMAND ----------

# DBTITLE 1,4on - remove duplicate
# Remove duplicate 4ons added by Venkata
# if dupes pick the oldest record as per the created_date_4on
sdz_4on_df = sdz_4on_df.withColumn("rank", expr("row_number() over(partition by seal_id order by created_date)"))
sdz_4on_df = sdz_4on_df.filter(expr("rank = 1")).drop('rank')
print(f"Total 4on after removing duplicate: {sdz_4on_df.count()}")

# COMMAND ----------

# DBTITLE 1,4on - filter in with valid 4on serial number
# filter in rows with a valid m7 machine 36-char 'seal id' (4on serial number)
sdz_4on_df = sdz_4on_df.filter(length("seal_id") == 36)
print(f"Total 4on after removing invalid serial number: {sdz_4on_df.count()}")

# COMMAND ----------

# DBTITLE 1,4on - expand 4on to 1on
## Expand 4on serial number into 4 chunks and decode each chunk back to decimal (base 10) to reveal 1on serial number

# split 4on serial number into 4 chunks
sdz_4on_df = sdz_4on_df.withColumn(
    'split_seal_ids',
    array(
        substring(col('seal_id'), 1, 9),
        substring(col('seal_id'), 10, 9),
        substring(col('seal_id'), 19, 9),
        substring(col('seal_id'), 28, 9)
    )
)

# Explode the array into separate rows
sdz_4on_1on_df = sdz_4on_df.select('*', explode('split_seal_ids').alias('1on_sn_base36')).drop('split_seal_ids')
print(f"Total 4on after splitting into 4 chunks: {sdz_4on_df.count()}")

# decode base36 into base10
sdz_4on_1on_df = sdz_4on_1on_df.withColumn('1on_sn', conv(col("1on_sn_base36"), 36, 10))
sdz_4on_1on_df.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Join M7 bnlist with ISA 4ons

# COMMAND ----------

# DBTITLE 1,Prepare bn for joining
# Choose columns for joining
bn_cols = ("file_date", "ingestion_date", "1on_sn", "date", "time", "msec", "machine_serialnumber", "boc_machine_num", "shift_number","stacker", "block_seq_num", "serial_number", "sorting_class", "denomination","header_card_number", "site_name")
sdz_bnlist_df = sdz_bnlist_df.select(*bn_cols)

## Prefix bn columns with "bn_"
sdz_bnlist_df = sdz_bnlist_df.select([col(c).alias(f"bn_{c}") for c in sdz_bnlist_df.columns])

# display for debugging
sdz_bnlist_df.limit(100).display()

# COMMAND ----------

# DBTITLE 1,Prepare isa for joining
# Choose columns for joining
isa_cols = ("file_date", "ingestion_date", "seal_id", "created_date", "denom_category", "denom_value", "denom_type", "1on_sn_base36", "1on_sn")
sdz_4on_1on_df = sdz_4on_1on_df.select(*isa_cols)

## Prefix bn columns with "bn_"
sdz_4on_1on_df = sdz_4on_1on_df.select([col(c).alias(f"isa_{c}") for c in sdz_4on_1on_df.columns])

# display for debugging
sdz_4on_1on_df.limit(100).display()

# COMMAND ----------

# DBTITLE 1,Perform outer join on bn and 4on-1on
# the big join: perform outer join on all valid banknote records from the decoded ISA 4on serial numbers df (left) with the m7 bnlist 1on df (right)
bn_4on_df = sdz_4on_1on_df.join(sdz_bnlist_df, sdz_4on_1on_df["isa_1on_sn"] == sdz_bnlist_df["bn_1on_sn"], "outer")

# for debugging
print(f"Join row count: {bn_4on_df.count()}")
bn_4on_df.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to curated table

# COMMAND ----------

# Remove existing data with same file_date
print(f"Removing existing data from {curated_4ons_table_name}")
try :
    dt = delta.DeltaTable.forName(spark, curated_4ons_table_name)
    dt.delete()
except (Exception) as ex:
    print(f"Failed to remove the existing data from {curated_4ons_table_name}, due to {ex}")

# Write data to target table
print(f"Writing data to {curated_4ons_table_name}")
bn_4on_df.write.format("delta").mode("append").saveAsTable(curated_4ons_table_name)

# COMMAND ----------

# Signal successful completion and return a value to ADF
dbutils.notebook.exit("pass")
