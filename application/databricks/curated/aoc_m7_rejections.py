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
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import pyspark.sql.types as t
from datetime import date as py_date

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

dbutils.widgets.text("env","")
dbutils.widgets.text("date","")
land_storage = dbutils.widgets.text("land_storage","")
storage = dbutils.widgets.text("storage","")
dbutils.widgets.dropdown("load_type", "initial_load", ["initial_load", "incremental_load"])
dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"])

env = dbutils.widgets.get("env").strip()
print(f"env is {env}")
file_date = dbutils.widgets.get("date").strip()
print(f"file_date is {file_date}")
land_storage = dbutils.widgets.get("land_storage").strip()
print(f"land_storage is {land_storage}")
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

# Get land storage if not provided
if len(land_storage.strip()) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage: {land_storage}")

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

tbl_std_rejects = f"{unity_catalog_name}.standardized_db.tbl_m7_rejects"
print(f"tbl_std_rejects: {tbl_std_rejects}")

# output lake path
cur_rejects_path = "abfss://restricted@"+storage+".dfs.core.windows.net/curated/currency/aoc/m7_rejections"
print(f"cur_rejects_path: {cur_rejects_path}")

# output UC table
tbl_cur_rejects = f"{unity_catalog_name}.curated_db.tbl_m7_rejects"
print(f"tbl_cur_rejects: {tbl_cur_rejects}")

# COMMAND ----------

reject_mapping_df = spark.read.format("csv").load(f"abfss://config-currency@{land_storage}.dfs.core.windows.net/m7/mapping/m7_reject_reasons_mapping.csv", header = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assigning full load data or incremental load data

# COMMAND ----------

deposittohc_col_list = ['ingestion_date','nesid','pcd','created_date','nes_deposit_date','nes_shipping_date','aoc_received_date','client_rdp','client_fi','cash_centre']

rejections_col_list = ['machine_serial_number','shift_name','shift_number','shift_start_date','shift_start_time','shift_end_date','shift_end_time','headercard_id','headercard_singled','headercard_start_date','headercard_start_time','headercard_last_modify','headercard_total_amount','volume','reject_category','reject_property','reject_property_count','ingestion_date','site_name']

# COMMAND ----------

# load data
sdz_deposittohc_df = spark.read.table(tbl_std_deposittohc)
sdz_rejections_df = spark.read.table(tbl_std_rejects)

if load_type == "initial_load":
    sdz_deposittohc_df = sdz_deposittohc_df.select(*deposittohc_col_list)
    sdz_rejections_df = sdz_rejections_df.select(*rejections_col_list)
else:
    sdz_deposittohc_df = sdz_deposittohc_df.filter(f"file_date = '{file_date}'")
    sdz_rejections_df = sdz_rejections_df.filter(f"file_date = '{file_date}'")
    
    if ((sdz_deposittohc_df.count() == 0) or (sdz_rejections_df.count() == 0)):
        dbutils.notebook.exit("no_date")

    sdz_deposittohc_df = sdz_deposittohc_df.select(*deposittohc_col_list)
    sdz_rejections_df = sdz_rejections_df.select(*rejections_col_list)

    existing_m7_rejections_df = spark.read.table(tbl_cur_rejects)
    existing_m7_rejections_closed_df = existing_m7_rejections_df.filter(existing_m7_rejections_df.status == 'closed') 
    existing_m7_rejections_open_df = existing_m7_rejections_df.filter(existing_m7_rejections_df.status == 'open') 
    existing_m7_rejections_open_df = existing_m7_rejections_open_df.select(*rejections_col_list)
    
    sdz_rejections_df = sdz_rejections_df.union(existing_m7_rejections_open_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Saving data frame result as a table

# COMMAND ----------


sdz_deposittohc_df.createOrReplaceTempView("deposit_hc_table")
sdz_rejections_df.createOrReplaceTempView("m7_rejections_table")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Data (M7 & ISA)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from deposit_hc_table 
# MAGIC --3496--

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from m7_rejections_table 
# MAGIC --27379 (count)
# MAGIC --startdate & last modify date 
# MAGIC --

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join M7 rejections + Deposit to HC 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select cols from Deposit_to_HC for initial join with M7_rejections

# COMMAND ----------

deposittohc_join_df = sdz_deposittohc_df.select("pcd", "created_date") \
    .withColumnRenamed("pcd", "pcd_join") \
    .withColumnRenamed("created_date", "created_date_join") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Joining depositHCjoin data with M7 rejections data

# COMMAND ----------

m7_rejections_df = sdz_rejections_df.join(deposittohc_join_df, \
                                    (sdz_rejections_df.headercard_id == deposittohc_join_df.pcd_join) & \
                                    (sdz_rejections_df.headercard_start_date >= deposittohc_join_df.created_date_join), \
                                    "left") \
        .withColumn("hc_datediff", datediff("headercard_start_date", "created_date_join")) \
            .drop("ingestion_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create minimum values dataframe to determine which are open/close

# COMMAND ----------


min_values_df = m7_rejections_df.groupBy("headercard_id","headercard_start_date") \
                                .agg(min(m7_rejections_df.hc_datediff).alias("min_datediff"))

min_values_df2 = m7_rejections_df.groupBy("headercard_id","created_date_join") \
                                .agg(min(m7_rejections_df.hc_datediff).alias("min_datediff")) \
                                .withColumnRenamed("headercard_id", "headercard_id2") \
                                .withColumnRenamed("created_date_join", "created_date_join2") \
                                .withColumnRenamed("min_datediff", "min_datediff2")

min_join = min_values_df.join(min_values_df2, \
    (min_values_df.headercard_id == min_values_df2.headercard_id2) &\
    (min_values_df.min_datediff == min_values_df2.min_datediff2),
    "left")

min_join = min_join.withColumn("diff", datediff("headercard_start_date", "created_date_join2"))
min_join = min_join.filter( (min_join.min_datediff2 == min_join.diff) | (min_join.diff.isNull()) )

# COMMAND ----------

display(min_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding the Deposit_to_HC remaining cols to min values join data

# COMMAND ----------

depositjoin = min_join.join(sdz_deposittohc_df, \
    (min_join.headercard_id == sdz_deposittohc_df.pcd) &\
    (min_join.created_date_join2 == sdz_deposittohc_df.created_date), \
    "left")

depositjoin = depositjoin.withColumnRenamed("headercard_id", "headercard_id3").withColumnRenamed("headercard_start_date", "headercard_start_date3")


# COMMAND ----------

display(depositjoin)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding the M7_rejections remaining cols to deposit join data

# COMMAND ----------

m7_rejections_df = m7_rejections_df.join(depositjoin, \
    (m7_rejections_df.headercard_id == depositjoin.headercard_id3) &\
    (m7_rejections_df.headercard_start_date == depositjoin.headercard_start_date3) & \
    (m7_rejections_df.hc_datediff == depositjoin.min_datediff), \
    "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding new column validate and filter validate ==1

# COMMAND ----------


m7_rejections_df = m7_rejections_df.withColumn("validate", when( ( (m7_rejections_df.min_datediff.isNotNull()) & (m7_rejections_df.headercard_id2.isNull()) ) | (m7_rejections_df.diff == m7_rejections_df.min_datediff2) | (m7_rejections_df.pcd_join.isNull()), 1))
m7_rejections_df = m7_rejections_df.filter(m7_rejections_df.validate == 1)

# COMMAND ----------

m7_rejections_df = m7_rejections_df.drop("machine_variant","machine_version_info","pcd_join", "created_date_join","hc_datediff", "headercard_id3", "headercard_start_date3", "min_datediff", "headercard_id2", "created_date_join2", "min_datediff2","diff", "validate") \
    .withColumn("status", when(m7_rejections_df.pcd.isNotNull(), "closed").when(m7_rejections_df.pcd.isNull(), "open"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding pass_number column to the m7_rejections final data

# COMMAND ----------

m7_rejections_df.createOrReplaceTempView("m7_rejections_final_table")


# COMMAND ----------

# MAGIC %sql
# MAGIC WITH DateAndTimeGroups AS (
# MAGIC   SELECT DISTINCT
# MAGIC     headercard_start_date,
# MAGIC     headercard_start_time,
# MAGIC     headercard_id
# MAGIC   FROM m7_rejections_final_table
# MAGIC )
# MAGIC
# MAGIC SELECT t.*,
# MAGIC   DENSE_RANK() OVER (PARTITION BY g.headercard_id ORDER BY g.headercard_start_date, g.headercard_start_time) AS pass_number,
# MAGIC   current_date() AS status_date
# MAGIC FROM
# MAGIC   m7_rejections_final_table t
# MAGIC JOIN
# MAGIC   DateAndTimeGroups g
# MAGIC ON
# MAGIC   t.headercard_start_date = g.headercard_start_date
# MAGIC   AND t.headercard_start_time = g.headercard_start_time
# MAGIC   AND t.headercard_id = g.headercard_id;
# MAGIC

# COMMAND ----------

m7_rejections_final_df = _sqldf

# COMMAND ----------

m7_rejections_final_df.count() #27355

# COMMAND ----------

m7_rejections_join_df = m7_rejections_final_df.join(
    reject_mapping_df,
    (reject_mapping_df['reject reason'] == m7_rejections_final_df['reject_property']),
    "left"
)

# COMMAND ----------

m7_rejections_join_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Appending the m7_rejections data of previous day

# COMMAND ----------

if load_type == "initial_load":
    # If existing m7 counters data is empty, assign the result set of m7_counters
    m7_rejections_join_df = m7_rejections_join_df
else:
    # If existing m7 counters data is not empty, append m7_counters closed to the result set
    m7_rejections_join_df = m7_rejections_join_df.union(existing_m7_rejections_closed_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Overwrite the parquet file in curated m7_rejections_path

# COMMAND ----------

# DBTITLE 1,Write to lake
m7_rejections_join_df.write.format("parquet").mode("overwrite").save(cur_rejects_path)
#status_date
#we have to overwrite


# COMMAND ----------

# DBTITLE 1,Write to UC
# use overwrite as Lake
m7_rejections_join_df.write.format("delta").mode("overwrite").saveAsTable(tbl_cur_rejects)

# COMMAND ----------

dbutils.notebook.exit('pass')
