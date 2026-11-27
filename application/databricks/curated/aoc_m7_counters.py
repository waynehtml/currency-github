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

tbl_std_counts = f"{unity_catalog_name}.standardized_db.tbl_m7_counts"
print(f"tbl_std_counts: {tbl_std_counts}")

# output lake path
cur_counts_path = "abfss://restricted@"+storage+".dfs.core.windows.net/curated/currency/aoc/m7_counters"
print(f"cur_counts_path: {cur_counts_path}")

# output UC table
tbl_cur_counts = f"{unity_catalog_name}.curated_db.tbl_m7_counts"
print(f"tbl_cur_counts: {tbl_cur_counts}")

# COMMAND ----------

counters_col_list = ['machine_serial_number','shift_name','shift_number','shift_start_date','shift_start_time','shift_end_date','shift_end_time','headercard_id','headercard_singled','headercard_start_date','headercard_start_time','headercard_last_modify','headercard_total_amount','rejects','parsect_number','parsect_container_id','operation_mode','input_mode','stacking_mode','sorting_mode','parsect_expected','parsect_denom_id','parsect_numberau','parsect_start_date','parsect_start_time','parsect_end_date','parsect_end_time','denom_id','denom_name','currency','value','quality','fitness','output','number','ingestion_date',       'site_name']

deposittohc_col_list = ['ingestion_date','nesid','pcd','created_date','nes_deposit_date','nes_shipping_date','aoc_received_date','client_rdp','client_fi','cash_centre']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assigning full load data or incremental load data

# COMMAND ----------

# load data
sdz_deposittohc_df = spark.read.table(tbl_std_deposittohc)
sdz_counters_df = spark.read.table(tbl_std_counts)

if load_type == "initial_load":
    sdz_deposittohc_df = sdz_deposittohc_df.select(*deposittohc_col_list)
    sdz_counters_df = sdz_counters_df.select(*counters_col_list)
else:
    sdz_deposittohc_df = sdz_deposittohc_df.filter(f"file_date = '{file_date}'")
    sdz_counters_df = sdz_counters_df.filter(f"file_date = '{file_date}'")
    
    if ((sdz_deposittohc_df.count() == 0) or (sdz_counters_df.count() == 0)):
        dbutils.notebook.exit("no_date")

    sdz_deposittohc_df = sdz_deposittohc_df.select(*deposittohc_col_list)
    sdz_counters_df = sdz_counters_df.select(*counters_col_list)

    existing_m7_counters_df = spark.read.table(tbl_cur_counts)
    existing_m7_counters_closed_df = existing_m7_counters_df.filter(existing_m7_counters_df.status == 'closed') 
    existing_m7_counters_open_df = existing_m7_counters_df.filter(existing_m7_counters_df.status == 'open') 
    existing_m7_counters_open_df = existing_m7_counters_open_df.select(*counters_col_list)

    sdz_counters_df = sdz_counters_df.union(existing_m7_counters_open_df)  

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Saving data frame result as a table

# COMMAND ----------


sdz_deposittohc_df.createOrReplaceTempView("deposit_hc_table")
sdz_counters_df.createOrReplaceTempView("m7_counters_table")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Data (M7 & ISA)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from deposit_hc_table 
# MAGIC --3496--

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from m7_counters_table 
# MAGIC --24400 (count)
# MAGIC --startdate & last modify date 
# MAGIC --

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join M7 Counters + Deposit to HC 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select cols from Deposit_to_HC for initial join with M7_counters

# COMMAND ----------

deposittohc_join_df = sdz_deposittohc_df.select("pcd", "created_date") \
    .withColumnRenamed("pcd", "pcd_join") \
    .withColumnRenamed("created_date", "created_date_join") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Joining depositHCjoin data with M7 counters data

# COMMAND ----------

m7_counters_df = sdz_counters_df.join(deposittohc_join_df, \
                                    (sdz_counters_df.headercard_id == deposittohc_join_df.pcd_join) & \
                                    (sdz_counters_df.headercard_start_date >= deposittohc_join_df.created_date_join), \
                                    "left") \
        .withColumn("hc_datediff", datediff("headercard_start_date", "created_date_join")) \
            .drop("ingestion_date")

# COMMAND ----------

display(m7_counters_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create minimum values dataframe to determine which are open/close

# COMMAND ----------


min_values_df = m7_counters_df.groupBy("headercard_id","headercard_start_date") \
                                .agg(min(m7_counters_df.hc_datediff).alias("min_datediff"))

min_values_df2 = m7_counters_df.groupBy("headercard_id","created_date_join") \
                                .agg(min(m7_counters_df.hc_datediff).alias("min_datediff")) \
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
# MAGIC ##### Adding the M7_counters remaining cols to deposit join data

# COMMAND ----------

m7_counters_df = m7_counters_df.join(depositjoin, \
    (m7_counters_df.headercard_id == depositjoin.headercard_id3) &\
    (m7_counters_df.headercard_start_date == depositjoin.headercard_start_date3) & \
    (m7_counters_df.hc_datediff == depositjoin.min_datediff), \
    "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding new column validate and filter validate ==1

# COMMAND ----------


m7_counters_df = m7_counters_df.withColumn("validate", when( ( (m7_counters_df.min_datediff.isNotNull()) & (m7_counters_df.headercard_id2.isNull()) ) | (m7_counters_df.diff == m7_counters_df.min_datediff2) | (m7_counters_df.pcd_join.isNull()), 1))
m7_counters_df = m7_counters_df.filter(m7_counters_df.validate == 1)

# COMMAND ----------

display(m7_counters_df)

# COMMAND ----------

m7_counters_df = m7_counters_df.drop("machine_variant","machine_version_info","pcd_join", "created_date_join","hc_datediff", "headercard_id3", "headercard_start_date3", "min_datediff", "headercard_id2", "created_date_join2", "min_datediff2","diff", "validate") \
    .withColumn("status", when(m7_counters_df.pcd.isNotNull(), "closed").when(m7_counters_df.pcd.isNull(), "open"))

# COMMAND ----------

m7_counters_df.count()
#24398

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding pass_number column to the m7_counter final data

# COMMAND ----------

m7_counters_df.createOrReplaceTempView("m7_counters_final")


# COMMAND ----------

# MAGIC %sql
# MAGIC WITH DateAndTimeGroups AS (
# MAGIC   SELECT DISTINCT
# MAGIC     headercard_start_date,
# MAGIC     headercard_start_time,
# MAGIC     headercard_id
# MAGIC   FROM m7_counters_final
# MAGIC )
# MAGIC
# MAGIC SELECT t.*,
# MAGIC   DENSE_RANK() OVER (PARTITION BY g.headercard_id ORDER BY g.headercard_start_date, g.headercard_start_time) AS pass_number,
# MAGIC   current_date() AS status_date
# MAGIC FROM
# MAGIC   m7_counters_final t
# MAGIC JOIN
# MAGIC   DateAndTimeGroups g
# MAGIC ON
# MAGIC   t.headercard_start_date = g.headercard_start_date
# MAGIC   AND t.headercard_start_time = g.headercard_start_time
# MAGIC   AND t.headercard_id = g.headercard_id;
# MAGIC

# COMMAND ----------

m7_counters_final_df = _sqldf

# COMMAND ----------

m7_counters_final_df.count() #24400

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Appending the m7_counters data of previous day

# COMMAND ----------

if load_type == "initial_load":
    # If existing m7 counters data is empty, assign the result set of m7_counters
    m7_counters_final_df = m7_counters_final_df
else:
    # If existing m7 counters data is not empty, append m7_counters closed to the result set
    m7_counters_final_df = m7_counters_final_df.union(existing_m7_counters_closed_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Overwrite the parquet file in curated m7_counters_path

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Write to lake
m7_counters_final_df.write.format("parquet").mode("overwrite").save(cur_counts_path)
#status_date
#we have to overwrite

# COMMAND ----------

# DBTITLE 1,Write to UC
# use overwrite as Lake
m7_counters_final_df = m7_counters_final_df.withColumn('value', f.col('value').cast(IntegerType()))
m7_counters_final_df.write.format("delta").mode("overwrite").saveAsTable(tbl_cur_counts)

# COMMAND ----------

dbutils.notebook.exit('pass')
