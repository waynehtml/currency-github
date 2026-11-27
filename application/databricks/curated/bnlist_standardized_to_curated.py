# Databricks notebook source
# MAGIC %md
# MAGIC #bnlist_standardized_to_curated

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook, we will:
# MAGIC - Ingest standardized data(bnlist, acdptrace, hctoclient, deposittohc, hstbtransfer)
# MAGIC - Transform standardized data per requirements
# MAGIC - Join standardized data into a single curated table
# MAGIC - Write to CUR's UC curated_db

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Parameters, libraries and Tables

# COMMAND ----------

## import required libraries
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_date, to_timestamp, col, lit, when, unix_timestamp, current_timestamp, date_format, year, month, dayofmonth, dayofweek, hour, minute, second, concat, regexp_replace
from pyspark.sql.types import DateType, IntegerType, StringType


# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")
dbutils.widgets.text("date","")
dbutils.widgets.dropdown("load_type", "initial_load", ["initial_load", "incremental_load"])

environment = dbutils.widgets.get("environment").strip()
print(f"Environment: {environment}")
date = dbutils.widgets.get("date").strip()
print(f"Date: {date}")
load_type = dbutils.widgets.get("load_type")
print(f"Load type: {load_type}")

# COMMAND ----------

# DBTITLE 1,Table names
# Get environment if not provided
if len(environment) == 0:
    environment = get_env()
print(f"environment: {environment}")

# catalog name
unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

# define standardized table name
std_bnlist_table = f"{unity_catalog_name}.standardized_db.tbl_m7_bnlist"
print(f"std_bnlist_table: {std_bnlist_table}")
std_hctoclient_table = f"{unity_catalog_name}.standardized_db.tbl_isa_hctoclient"
print(f"std_hctoclient_table: {std_hctoclient_table}")
std_deposittohc_table = f"{unity_catalog_name}.standardized_db.tbl_isa_deposittohc"
print(f"std_deposittohc_table: {std_deposittohc_table}")
std_extracthstbtransfer_table = f"{unity_catalog_name}.standardized_db.tbl_isa_extracthstbtransfer"
print(f"std_extracthstbtransfer_table: {std_extracthstbtransfer_table}")
std_acdptrace_table = f"{unity_catalog_name}.standardized_db.tbl_isa_acdptrace"
print(f"std_acdptrace_table: {std_acdptrace_table}")

# Define the curated table name
cur_bnlist_table = f"{unity_catalog_name}.curated_db.tbl_bnlist"
print(f"cur_bnlist_table: {cur_bnlist_table}")
cur_bnlist_temp_table = f"{unity_catalog_name}.curated_db.tbl_bnlist_incremental_temp"
print(f"cur_bnlist_temp_table: {cur_bnlist_temp_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read bn Standardized Data
# MAGIC

# COMMAND ----------

## FOR BNList:
## -- if initial_load, load all data from ADLS standardized folder
## -- if incremental_load, load data for the specified date, plus all curated data where the join_status is null


try:
    # Define the path based on load type
    if load_type == "initial_load":
        # Path to bnlist folder for all dates
        print("Initial load: loading all data from bnlist.")
        
        #load standardizeddata from full path
        df_bnlist_sdz = spark.sql(f"SELECT * FROM {std_bnlist_table}")

        #count rows in bnlist sdz
        bnlist_sdz_count = df_bnlist_sdz.count()
        print(f'number of rows in df_bnlist_sdz: {bnlist_sdz_count}')

    else:
        # Incremental load: load only for the specified date 
        print(f"Incremental load: loading data for {date}.")

        #load standardized data from path with specified date
        df_bnlist_sdz = spark.sql(f"SELECT * FROM {std_bnlist_table} where file_date = '{date}'")

        # count rows in bnlist_sdz on the specified date 
        bnlist_sdz_count = df_bnlist_sdz.count()
        print(f'number of rows in incremental load of df_bnlist_sdz: {bnlist_sdz_count}')

        #load curated data curated data where the join_status is null, but remove rows date is the current incremental load
        stmt_read_curated_null = f'SELECT * FROM {cur_bnlist_table} WHERE join_status = 0'
        df_bnlist_curated_null = spark.sql(stmt_read_curated_null)
        df_bnlist_curated_null = df_bnlist_curated_null.filter(col("date") != lit(date))

        # count number of rows in bnlist_curated where join status is null
        bnlist_curated_null_count = df_bnlist_curated_null.count()
        print(f'number of rows in df_bnlist_curated_null: {bnlist_curated_null_count}')

        # add incremental load and null curated data for total incremental count
        total_incremental_load_count = bnlist_sdz_count + bnlist_curated_null_count
        print(f'total number of rows ingested in incremental load: {total_incremental_load_count}')

except Exception as e:
    # Handle exceptions and exit notebook if an error occurs
    print(f"An unexpected error occurred: {e}")
    dbutils.notebook.exit("no_date")


# COMMAND ----------

# MAGIC %md
# MAGIC # Load and Transform Standardized Data: httoclient, deptohc, hstb and trace

# COMMAND ----------

# DBTITLE 1,Read standardized data
df_hctoclient_sdz = spark.read.table(std_hctoclient_table)
df_deptohc_sdz = spark.read.table(std_deposittohc_table)
df_hstb_sdz = spark.read.table(std_extracthstbtransfer_table)
df_trace_sdz = spark.read.table(std_acdptrace_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform BNList sdz

# COMMAND ----------

#validate number of rows in standardized
print(f'num bnlist sdz rows: {bnlist_sdz_count}')

# Drop: 2 cols
# better solution is to select a subset of columns from the standardized tables
df_bnlist_sdz = df_bnlist_sdz.drop("file_date", "_denomination")

# Rename: 2 cols
df_bnlist_sdz = df_bnlist_sdz.withColumnRenamed("_headercardstatus", "headercard_status") \
                             .withColumnRenamed("_orientation", "orientation")

# Convert: 3 cols
df_bnlist = df_bnlist_sdz.withColumn("header_card_number", col("header_card_number").cast("int")) \
                         .withColumn("bps_created", to_date(col("bps_created"), "yyyy-MM-dd HH:mm:ss")) \
                         .withColumn("shift_starttime", to_date(col("shift_starttime")))

#filter bnlist where header_card_number is not null or 0
#df_bnlist = df_bnlist.filter(col("header_card_number").isNotNull()) \
#                        .filter(col("header_card_number") != 0)

#count the number of rows in filtered bnlist 
df_bnlist_count = df_bnlist.count() 
print(f'num bnlist rows: {df_bnlist_count}')

# COMMAND ----------

## if incremental load, append bnlist sdz data to null-joined curated data

if load_type == "initial_load":
    print("no union for initial load")

else:
    #drop cols from curated bnlist data with missing joins
    drop_curated_cols = ["client_rdp", "client_fi", "nesid", "nes_deposit_date",  "nes_shipping_date", "aoc_received_date", "rdc_id", "join_status"]
    df_bnlist_curated_null = df_bnlist_curated_null.drop(*drop_curated_cols)
    
    
    #union bnlist sdz data with null-joined curated data
    df_bnlist = df_bnlist.unionByName(df_bnlist_curated_null)

    bnlist_sdz_union_count = df_bnlist.count() #### -test
    print(f'number of rows after the union of bnlist_sdz and df_bnlist_curated_null: {bnlist_sdz_union_count}')

    #validate ###### - test how to count 
    #if total_incremental_load_count != bnlist_sdz_union_count:
    #    raise ValueError("there is an incorrect number of rows after the union of bnlist_sdz and df_bnlist_curated_null. there are {total_incremental_load_count} rows loaded, but {bnlist_sdz_union_count} after the union")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform ACDP Trace

# COMMAND ----------

# select columns required from ACDP Trace, rename and cast columns, and drop unnecessary columns
df_trace = df_trace_sdz.select("header_card", "counting_link", "machine_scan") \
                        .withColumn("pcd", col("header_card").cast("int")) \
                        .withColumnRenamed("counting_link", "lcounting") \
                        .withColumn("machine_date", to_date("machine_scan", "yyyy-MM-dd")) \
                        .drop("header_card", "counting_link", "machine_scan")

# drop duplicates
df_trace = df_trace.dropDuplicates(['lcounting', 'pcd', 'machine_date'])

#filter where pcd is not 0, and where machine_date is greater than Jan 1, 2023 (some records show machine_date as Jan 2, 1970 which is invalid)
df_trace = df_trace.filter(col("machine_date") > "2023-01-01") \
                    .filter(col('pcd') != 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform hctoclient

# COMMAND ----------

# select columns required from hctoclient, rename and cast columns, and drop unnecessary columns
df_hctoclient = df_hctoclient_sdz.select("counting_link", "pcd", "machine_date", "client_rdp", "client_fi", "created_date") \
                                .withColumn("client_fi", regexp_replace("client_fi", "177-M7", "177")) \
                                .withColumn("pcd_hctoclient", col("pcd").cast("int")) \
                                .withColumn("machine_date_isa", to_date("machine_date", "yyyy-MM-dd")) \
                                .withColumn("client_rdp", col("client_rdp").cast("int")) \
                                .withColumn("client_fi", col("client_fi").cast("int")) \
                                .withColumn("created_date", to_date("created_date", "yyyy-MM-dd")) \
                                .drop("pcd", "machine_date")


#count rows before and after deduplication
print(f'Before dedup: {df_hctoclient.count()}')
df_hctoclient = df_hctoclient.dropDuplicates()
print(f'After dedup: {df_hctoclient.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform deposittohc

# COMMAND ----------

# select required columns from deposittohc, rename and cast columns, and drop unnecessary columns
df_deposittohc = df_deptohc_sdz.select("nesid", "pcd", "nes_deposit_date", "nes_shipping_date", "aoc_received_date", "created_date") \
                                .withColumn("pcd_deptohc", col("pcd").cast("int")) \
                                .withColumn("nes_shipping_date", to_date("nes_shipping_date", "yyyy-MM-dd")) \
                                .withColumn("aoc_received_date", to_date("aoc_received_date", "yyyy-MM-dd")) \
                                .withColumn("created_date_deptohc", to_date("created_date", "yyyy-MM-dd")) \
                                .drop("pcd", "created_date")

#count rows before and after deduplication
print(f'Before dedup: {df_deposittohc.count()}')
df_deposittohc = df_deposittohc.dropDuplicates()
print(f'After dedup: {df_deposittohc.count()}')

#display(df_deposittohc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform hstbtransfer

# COMMAND ----------

#Ingest HSTBTransfer data

# select required columns from extracthstbtransfer, rename and cast columns, and drop unnecessary columns
df_hstb = df_hstb_sdz.select("pcd_name", "creation", "cash_centre") \
                    .withColumn("pcd", col("pcd_name").cast("int")) \
                    .withColumn("machine_date", to_date("creation", "yyyy-MM-dd")) \
                    .withColumn("client_fi", lit(177).cast(IntegerType())) \
                    .withColumn("client_rdp", \
                        when(col("cash_centre") == "MAOC", 20) \
                        .when(col("cash_centre") == "TAOC", 21)) \
                    .withColumn("rdc_id", concat(col("client_fi"), lit("_"), col("client_rdp"))) \
                    .withColumn("nesid", lit(None).cast(StringType())) \
                    .withColumn("nes_deposit_date", lit(None).cast(DateType())) \
                    .withColumn("nes_shipping_date", lit(None).cast(DateType())) \
                    .withColumn("aoc_received_date", lit(None).cast(DateType())) \
                    .withColumn("join_status", lit(1)) \
                    .drop("pcd_name", "creation", "cash_centre")

#                    .withColumn("closed", lit(None).cast(DateType())) \

#count rows before and after deduplication
print(f'Before dedup: {df_hstb.count()}')
df_hstb = df_hstb.dropDuplicates()
print(f'After dedup: {df_hstb.count()}')

###validate select statement order if needed  -- check if we need 'closed' or other dates
df_hstb = df_hstb.select("pcd", "machine_date", "client_rdp", "client_fi", "nesid", "nes_deposit_date", "nes_shipping_date", "aoc_received_date", "rdc_id", "join_status")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Join sdz data for curated and transform

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join hctoclient & deposittohc on pcd & closed
# MAGIC
# MAGIC  

# COMMAND ----------

isa_drop = ["pcd_deptohc", "created_date_deptohc"]

# left join hctoclient and deposittohc to create df_isa, drop columns not needed, and add rdc_id
df_isa  = df_hctoclient.join(df_deposittohc, \
                            (df_hctoclient.pcd_hctoclient == df_deposittohc.pcd_deptohc) & \
                            (df_hctoclient.created_date == df_deposittohc.created_date_deptohc), \
                            "left") \
                        .drop(*isa_drop) \
                        .withColumn("rdc_id", concat(col("client_fi"), lit("_"), col("client_rdp")))


# validate there was no duplication of rows for the join:
hctoclient_count = df_hctoclient.count()
df_isa_count = df_isa.count()

# If  are any rows where count > 1, raise an error
if hctoclient_count != df_isa_count:
    raise ValueError("there is an incorrect number of rows in df_isa after the join. there are {hctoclient_count} rows before the join and {df_isa_count} after the join")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join ACDP Trace with isa_df

# COMMAND ----------

#columns to drop after join
trace_drop = ["lcounting", "counting_link", "pcd_hctoclient", "machine_date_isa", "created_date"]

# left join df_trace with df_isa, and drop columns not needed
df_trace_isa = df_trace.join(df_isa, (df_trace.lcounting == df_isa.counting_link) & (df_trace.pcd == df_isa.pcd_hctoclient), "left").drop(*trace_drop)
#drop duplicates
df_trace_isa = df_trace_isa.dropDuplicates()
print(f'number of rows in df_trace_isa: {df_trace_isa.count()}')

#remove rows where client_fi is null, add_join_status = 1
df_trace_isa = df_trace_isa.filter(df_trace_isa.client_fi.isNotNull()) \
                            .withColumn('join_status', lit(1))

#count rows in df_trace_isa and save to variable
df_trace_isa_count = df_trace_isa.count()
print(f'number of rows in df_trace_isa after filtering out null client_rdp: {df_trace_isa_count}')

# COMMAND ----------

# Validate if there are any groups in df_trace_isa with more than one row before join to avoid duplication

# Group by 'pcd' and 'machine_date', and count the number of rows in each group
grouped_df_trace_isa = df_trace_isa.groupBy('pcd', 'machine_date').count().orderBy('count', ascending=False)

# Check if any group has more than 1 row
exceeding_rows = grouped_df_trace_isa.filter(grouped_df_trace_isa['count'] > 1).count()

# If there are any rows where count > 1, raise an error
if exceeding_rows > 0:
    raise ValueError("There are groups with more than one row. This will fail the notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join BNList with df_trace_isa

# COMMAND ----------

#columns to drop after join
bnlist_drop = ["pcd", "machine_date", "cash_centre"]

#left join bnlist with df_trace_isa, and drop columns not needed
df_bnlist_isa = df_bnlist.join(df_trace_isa, \
                                    (df_bnlist.header_card_number == df_trace_isa.pcd) & \
                                    (df_bnlist.date == df_trace_isa.machine_date), \
                                    "left") \
                                    .drop(*bnlist_drop)

#count number of rows
df_bnlist_isa_count = df_bnlist_isa.count()
print(f"df_bnlist_isa_count = {df_bnlist_isa_count}")

# Check if there an incorrect number of rows after the join 
if load_type == "initial_load":
    if df_bnlist_count != df_bnlist_isa_count:
        raise ValueError("there is an incorrect number of rows in df_bnlist_isa after the join. there are {df_bnlist_count} rows before the join and {df_bnlist_isa_count} after the join")
else:
    if bnlist_sdz_union_count != df_bnlist_isa_count:
        raise ValueError("there is an incorrect number of rows in df_bnlist_isa after the join. there are {df_bnlist_count} rows before the join and {df_bnlist_isa_count} after the join")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split Bnlist into JOINED and NULL

# COMMAND ----------

#split bnlist into null and joined

df_bnlist_isa_null = df_bnlist_isa.filter(df_bnlist_isa.rdc_id.isNull())
df_bnlist_isa_joined = df_bnlist_isa.filter(df_bnlist_isa.rdc_id.isNotNull())

#count number of rows in NULL and JOINED, print each and the total
bnlist_isa_null_count = df_bnlist_isa_null.count()
bnlist_isa_joined_count = df_bnlist_isa_joined.count()

print(f'bnlist nulls = {bnlist_isa_null_count} + joined {bnlist_isa_joined_count} = total {bnlist_isa_null_count + bnlist_isa_joined_count}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join BNList(null joins) with Hstbtransfer

# COMMAND ----------

# DBTITLE 1,drop cols for bnlist_isa_null+ hstb join
# drop columns from null join 
drop_cols = [
    'client_rdp',
    'client_fi',
    'nesid',
    'nes_deposit_date',
    'nes_shipping_date',
    'aoc_received_date',
    'rdc_id',
    'join_status'
]

#drop columns from bnlist_isa_null to prepare for join with HSTB 
df_bnlist_isa_null = df_bnlist_isa_null.drop(*drop_cols)


# COMMAND ----------

# DBTITLE 1,join bnlist_isa_null + hstb = bnlist_hstb
#columns to drop from hstb after join with bnlist_isa_null
bnlist_drop = ["pcd", "machine_date"]

# left join bnlist_isa_null with hstb, drop columns
df_bnlist_hstb = df_bnlist_isa_null.join(df_hstb, \
                                        (df_bnlist_isa_null.header_card_number == df_hstb.pcd) & \
                                        (df_bnlist_isa_null.date == df_hstb.machine_date), \
                                        "left") \
                                    .drop(*bnlist_drop)

#count number of rows in df_bnlist_hstb and print
df_bnlist_hstb_count = df_bnlist_hstb.count()
print(f"df_bnlist_hstb_count = {df_bnlist_hstb_count}")

# Check if there is the sa
if bnlist_isa_null_count != df_bnlist_hstb_count:
    raise ValueError("there is an incorrect number of rows in df_bnlist_isa after the join. there are {bnlist_isa_null_count} rows before the join and {df_bnlist_hstb_count} after the join")

# COMMAND ----------

#validate number of rows in bnlist_hstb_null and bnlist_hstb
bnlist_hstb_null_count = df_bnlist_hstb.filter(col('rdc_id').isNull()).count()
bnlist_hstb_joined_count = df_bnlist_hstb.filter(col('rdc_id').isNotNull()).count()

print(f'bnlist after hstb: null = {bnlist_hstb_null_count}, joined = {bnlist_hstb_joined_count}, total = {bnlist_hstb_null_count + bnlist_hstb_joined_count}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Append BNLIST_ISA_JOIN with BNLIST_HSTB_JOIN for final BNLIST table

# COMMAND ----------

# DBTITLE 1,Append bnlist_final with bnlist_isa_joined
# append df_bnlist_isa_joined and df_bnlist_hstb for all data 
df_bnlist_final = df_bnlist_isa_joined.union(df_bnlist_hstb)

# replace nulls in join_status with 0
df_bnlist_final = df_bnlist_final.withColumn('join_status', when(col('join_status').isNull(), 0).otherwise(col('join_status')))

#count number of rows in bnlist_final and print
bnlist_final_count = df_bnlist_final.count()
print(f'number of rows in bnlist_final: {bnlist_final_count}' )

# validate number of rows in bnlist_final vs df_bnlist_count
if load_type == "initial_load":
    if df_bnlist_count != bnlist_final_count:
        raise ValueError(f"there is an incorrect number of rows in df_bnlist_isa after the join. there are {bnlist_isa_null_count} rows and {bnlist_final_count} after the union")
else:
    if bnlist_sdz_union_count != bnlist_final_count:
        raise ValueError(f"there is an incorrect number of rows in df_bnlist_isa after the join. there are {bnlist_sdz_union_count} rows and {bnlist_final_count} after the union")


#df_bnlist_final.printSchema()

# COMMAND ----------

# DBTITLE 1,Write incremental load to temp table and read back to dataframe
if load_type == "initial_load":
    print("no temp table needed")
else:
    #write bnlist_final to temp table
    df_bnlist_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cur_bnlist_temp_table)
    
    #return temp table to df_bnlist_temp
    get_bnlist_temp_stmt = f"select * from {cur_bnlist_temp_table}"
    df_bnlist_temp = spark.sql(get_bnlist_temp_stmt)

    # count rows in temp
    bnlist_temp_count = df_bnlist_temp.count()
    print(f'number of rows in tbl_bnlist_incremental_temp: {bnlist_temp_count}')

    if bnlist_final_count != bnlist_temp_count:
        raise ValueError(f"there is an incorrect number of rows in tbl_bnlist_incremental_temp. there are {bnlist_final_count} in df_bnlist_final and {bnlist_temp_count} in tbl_bnlist_incremental_temp")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Load data into the curated table

# COMMAND ----------

# DBTITLE 1,Write to curated
try:
    if load_type == "initial_load":
        
        #validate that standardized has the same number of rows as curated
        if bnlist_sdz_count == bnlist_final_count:
            print(f'The number of rows to write to curated is {bnlist_final_count}, which is equal to the number of rows in bnlist standardized')
            
            # write to curated 
            print(f"Writing data to {cur_bnlist_table}")
            df_bnlist_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cur_bnlist_table)
            print(f"Inital load or overwrite of {bnlist_final_count} rows to {cur_bnlist_table}")
        else:
            raise ValueError(f'The number of rows is not equal between df_bnlist_final and standardized. df_bnlist_final has {bnlist_final_count} rows and standardized has {bnlist_sdz_count} rows.')
    else:   
        #validate number of rows expected in curated after delete and write
        # 1. count rows in curated before delete
        # 2. count rows in curated to be deleted
        # 3. count rows to write to curated
        # 4. validate that the expected number of rows in curated is the same as the number of rows in standardized (exluding headercard is null or 0)

        # count rows in curated before delete
        curated_count_pre_delete_stmt = f"select count(*) as count from {cur_bnlist_table}"
        curated_count_pre_delete = spark.sql(curated_count_pre_delete_stmt).collect()[0]['count']

        # count rows in curated to be deleted
        curated_count_rows_to_delete_stmt = f"select count(*) as count from {cur_bnlist_table} where date = '{date}' OR join_status = 0"
        curated_count_rows_to_delete = spark.sql(curated_count_rows_to_delete_stmt).collect()[0]['count']

        # print number of rows in curated before delete, number of rows to be deleted, and number of rows to write
        print(f'The number of rows already in curated is {curated_count_pre_delete} and the number of rows to be deleted is {curated_count_rows_to_delete}. The number of rows to write is: {bnlist_temp_count}.')

        # calculate the expected number of rows in curated after delete & write
        expected_bnlist_curated_count = curated_count_pre_delete - curated_count_rows_to_delete + bnlist_temp_count
        print(f'expected number of rows in curated after delete & write: {expected_bnlist_curated_count}')
        
        #count number of rows in standardized
        bn_sdz_validation = spark.read.table(std_bnlist_table)
        #count rows in bnlist sdz - filter bnlist where header_card_number is not null or 0
        bn_sdz_validation_count = bn_sdz_validation.select(col("header_card_number")) \
                                        .withColumn("header_card_number", col("header_card_number").cast("int")) \
                                        .count() 
        print(f'number of rows in df_bnlist_sdz: {bn_sdz_validation_count}')

        if expected_bnlist_curated_count == bn_sdz_validation_count:
            #if the count of rows to delete is the same as the count of rows to write, delete the rows, then append the new data to curated 
            stmt_delete = f"delete from {cur_bnlist_table} where date = '{date}' OR join_status = 0"
            print(stmt_delete)
            spark.sql(stmt_delete)

            # write to temp table
            df_bnlist_temp.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(cur_bnlist_table)
            print("Data has been appended to the existing table for the incremental load and the previous null joins from curated, some of which may have been closed.")
            print(f"Appended {bnlist_final_count} rows to {cur_bnlist_table}")

            # count rows in curated before deleting temp table
            curated_count_stmt = f"select count(*) as count from {cur_bnlist_table}"
            curated_count = spark.sql(curated_count_stmt).collect()[0]['count']

            #validate row count in curated after incremental load, comparing to standardized before deleting temp table
            if bn_sdz_validation_count == curated_count:
                print(f'The number of rows in curated after incremental load is {curated_count}, which is equal to the number of rows in bnlist standardized')
            else:
                raise ValueError(f'The number of rows is not equal between curated and standardized. Curated has {curated_count} rows and standardized has {bn_sdz_validation_count} rows.')

            # truncate temp table for incremental load to delete all rows
            stmt_truncate_temp = f"TRUNCATE TABLE {cur_bnlist_temp_table}"
            print(stmt_truncate_temp)
            spark.sql(stmt_truncate_temp)

            # validate that temp table has been truncated and is empty
            count_truncate_temp_stmt = f"select count(*) as count from {cur_bnlist_temp_table}"
            count_truncate_temp = spark.sql(count_truncate_temp_stmt).collect()[0]['count']

            if count_truncate_temp == 0:
                print(f'tbl_bnlist_incremental_temp has been successfully truncated. The number of rows in tbl_bnlist_incremental_temp is {count_truncate_temp}')
            else:
                raise ValueError(f'tbl_bnlist_incremental_temp is not empty. The number of rows in tbl_bnlist_incremental_temp is {count_truncate_temp}')

        else:
            raise ValueError(f"difference in number of rows to delete ({curated_count_rows_to_delete}) and rows to write ({bnlist_final_count})")

except AnalysisException as e:
    # Handle exceptions and exit notebook if an error occurs
    print(f"A value error occurred: {e}")
    dbutils.notebook.exit("no_date")

# COMMAND ----------

# Define the return value
result_value = "pass"

# Signal successful completion and return a value
dbutils.notebook.exit(result_value)
