# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC - Join Standardized ISA and M7 unfit data for AOC-Unfit deposits use case

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Connection Information & Packages
# MAGIC

# COMMAND ----------

## import required libraries
#from pyspark.sql import functions as f
from pyspark.sql.functions import *
import pyspark.sql.types as t
from datetime import date as py_date
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

dbutils.widgets.text("env","")
dbutils.widgets.text("date","")
storage = dbutils.widgets.text("storage","")
dbutils.widgets.dropdown("load_type", "initial_load", ["initial_load", "incremental_load"])
dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"])

env = dbutils.widgets.get("env").strip()
print(f"env is {env}")
file_date = dbutils.widgets.get("date").strip()
print(f"file_date is {file_date}")
storage = dbutils.widgets.get("storage").strip()
print(f"storage is {storage}")
load_type = dbutils.widgets.get("load_type").strip()
print(f"load_type is {load_type}")
mode = dbutils.widgets.get("mode").strip()
print(f"mode is {mode}")

# COMMAND ----------

# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Catalog name
unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# input tables
tbl_std_deposittohc = f"{unity_catalog_name}.standardized_db.tbl_isa_deposittohc"
print(f"tbl_std_deposittohc: {tbl_std_deposittohc}")
tbl_std_hctoclient = f"{unity_catalog_name}.standardized_db.tbl_isa_hctoclient"
print(f"tbl_std_hctoclient: {tbl_std_hctoclient}")

# output path 
cur_deposits_path = "abfss://restricted@"+storage+".dfs.core.windows.net/curated/currency/aoc/isa_m7_note_processing"
print(f"cur_deposits_path: {cur_deposits_path}")

# output table
tbl_cur_deposits = f"{unity_catalog_name}.curated_db.tbl_m7_isa_note_processing"
print(f"tbl_cur_deposits: {tbl_cur_deposits}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assigning full load data or incremental load data

# COMMAND ----------

# columsn to extract
deposittohc_col_list = ['ingestion_date','nesid','pcd','created_date','nes_deposit_date','nes_shipping_date','aoc_received_date','client_rdp','client_fi','cash_centre']

hctoclient_col_list = ['ingestion_date','counting_link','pcd','created_date','machine_date','client_rdp','client_fi','valuecounting','denom_category','denom_value','nbdenom','nbreject','nbkeyin','count_value','reject_value','prep_value','cash_centre','denom_type_en','denom_type_fr','deposit_rule_en','deposit_rule_fr','special_stacker','invalid_media_type']

# read input data from std tables
sdz_deposittohc_df = spark.read.table(tbl_std_deposittohc)
sdz_hctoclient_df = spark.read.table(tbl_std_hctoclient)

# filter in only the date for incremental load
if load_type == "incremental_load":
    print(f"filtering data for date: {file_date}")
    sdz_deposittohc_df = sdz_deposittohc_df.filter(f"file_date = '{file_date}'")
    sdz_hctoclient_df = sdz_hctoclient_df.filter(f"file_date = '{file_date}'")

# if empty on incremental load, exit the notebook with no_date
sdz_deposittohc_count = sdz_deposittohc_df.count()
print(f"sdz_deposittohc_count: {sdz_deposittohc_count}")
sdz_hctoclient_count = sdz_hctoclient_df.count()
print(f"sdz_hctoclient_count: {sdz_hctoclient_count}")

if (load_type == "incremental_load" and (sdz_deposittohc_count == 0 or  sdz_hctoclient_count == 0)):
    dbutils.notebook.exit("no_date")

# Extract columns
sdz_deposittohc_df = sdz_deposittohc_df.select(*deposittohc_col_list)
sdz_hctoclient_df = sdz_hctoclient_df.select(*hctoclient_col_list)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Join ISA_hctoclient and ISA_deposittohc = ISA_unfits_data

# COMMAND ----------

sdz_deposittohc_df.createOrReplaceTempView("DepositToHC_Table")
sdz_hctoclient_df.createOrReplaceTempView("HCToClient_Table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC   select
# MAGIC     --b.pcd,
# MAGIC     --b.created_date,
# MAGIC     b.machine_date,
# MAGIC     --b.client_rdp,
# MAGIC     --b.client_fi,
# MAGIC     b.valuecounting,
# MAGIC     b.denom_value,
# MAGIC     b.nbdenom,
# MAGIC     b.nbreject,
# MAGIC     b.nbkeyin,
# MAGIC     b.count_value,
# MAGIC     b.reject_value,
# MAGIC     b.prep_value,
# MAGIC     --b.cash_centre,
# MAGIC     b.denom_type_en,
# MAGIC     b.denom_type_fr,
# MAGIC     b.deposit_rule_en,
# MAGIC     b.deposit_rule_fr,
# MAGIC     b.special_stacker,
# MAGIC     b.invalid_media_type,
# MAGIC     a.nesid,
# MAGIC     a.pcd,
# MAGIC     a.created_date,
# MAGIC     a.nes_deposit_date,
# MAGIC     a.nes_shipping_date,
# MAGIC     a.aoc_received_date,
# MAGIC     a.client_rdp,
# MAGIC     a.client_fi,
# MAGIC     a.cash_centre,
# MAGIC     a.ingestion_date
# MAGIC   from
# MAGIC     HCToClient_Table b
# MAGIC     left join DepositToHC_Table a on b.pcd = a.pcd
# MAGIC     and b.created_date = a.created_date) as x
# MAGIC where x.client_fi is not null

# COMMAND ----------

# MAGIC %md
# MAGIC ##Append the new ISA data to the existing ISA data

# COMMAND ----------

#saving matching ISA unfit data into dataframe
Curated_ISA_unfit_df = _sqldf

if load_type == "initial_load":
    #If it's initial load, we will overwrite in the curated path
    Curated_ISA_unfit_df.write.format("parquet").mode("overwrite").save(cur_deposits_path)

    # Write to UC
    Curated_ISA_unfit_df.write.format("delta").mode("overwrite").saveAsTable(tbl_cur_deposits)
else:
    #If it's incremental, we will append in the curated path
    # There is no proper way to remove existing data in the lake
    Curated_ISA_unfit_df.write.format("parquet").mode(mode).save(cur_deposits_path)

    # Write to UC
    print(f"Writing to {tbl_cur_deposits}")
    Curated_ISA_unfit_df.write.format("delta").mode(mode).saveAsTable(tbl_cur_deposits)

# COMMAND ----------

dbutils.notebook.exit('pass')
