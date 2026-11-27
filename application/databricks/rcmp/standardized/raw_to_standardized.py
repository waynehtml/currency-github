# Databricks notebook source
# DBTITLE 1,Import required libraries
## import required libraries
from pyspark.sql.functions import col, when
import pyspark.sql.types as t
import pandas as pd
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../../common/utils

# COMMAND ----------

# MAGIC %run ../common/definition

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")
dbutils.widgets.text("storage","")
dbutils.widgets.text("container","restricted")
dbutils.widgets.text("land_storage","")
dbutils.widgets.text("folder","2024-JAN")

# COMMAND ----------

# DBTITLE 1,Get Parameters
env = dbutils.widgets.get("environment")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

storage = dbutils.widgets.get("storage")
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

container = dbutils.widgets.get("container")
print(f"container: {container}")

land_storage = dbutils.widgets.get("land_storage")
if len(land_storage.strip()) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage: {land_storage}")

input_folder = dbutils.widgets.get("folder")
print(f"input_folder: {input_folder}")

# unity catalog name
unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

# DBTITLE 1,function - get_series_year
def get_series_year (dataframe, bank_denom_am,last_fld):
    year = dataframe.select('year').where((dataframe.DENOM == bank_denom_am) & (dataframe.SERIES == last_fld)).collect() 
    return year[0][0]

# COMMAND ----------

# DBTITLE 1,Read config file
# config file location
config_path = f"abfss://config-currency@{land_storage}.dfs.core.windows.net/rcmp/config_rcmp.txt"
print(f"config_path: {config_path}") 

# read config file
df_config  = spark.read.option('header',True).csv(config_path)
# display
df_config.display()

# COMMAND ----------

# DBTITLE 1,Read source files
# source file location
source_directory =  f"abfss://{container}@{storage}.dfs.core.windows.net/{RCMP_Base_Location}/{input_folder}/"
print(f"source_directory: {source_directory}")

# Read source file
df_file = spark.read.text(source_directory)
df_file.display()

# COMMAND ----------

# DBTITLE 1,Read fixed positioned columns
# row list to keep parsed rows
rows_list = []

# Convert rdd to Python iterator
file_rows = df_file.rdd.toLocalIterator()

# Process each row in the dataset
schema = RCMP_CFT['boc1']       # file config ID, only use fixed boc1 now
for file_row in file_rows:
    # Extract columns from the row into a dictionary
    row_map = {}
    for name in schema.names :
        if schema[name].metadata["include_in_fact"] == 'Y' :
            # slice characters from the row based on the start and end positions in the definition
            value = file_row[0][schema[name].metadata["start"]: schema[name].metadata["end"]]
            # remove leading and trailing spaces
            value = value.strip()
            # add the column name and value to the dictionary
            row_map[name] = value
    
    # Extract series year
    if row_map.get("series_yr") == 'NIL' or  "?" in (row_map.get("series_yr")) :
        try:
            cal_series_year = get_series_year (df_config, row_map.get("bank_denom_am"), row_map.get("last_fld"))
            row_map["series_yr"] =  cal_series_year
        except Exception as e:
            print(row_map.get("bank_denom_am"))
            print(row_map.get("last_fld"))
 
    # Append the row dictionary to row list
    rows_list.append(row_map)

# print parsed rows
print(f"Parsed rows: {rows_list}")

# convert to spark dataframe
df_out = spark.createDataFrame(rows_list)
df_out.display()

# COMMAND ----------

# DBTITLE 1,Transformation
df_final_out = (
    df_out
    .withColumn('country_ce', lit('CA'))
    .withColumn('issuing_country_ce', lit('CA'))
    #.withColumn('initl_issue_dt', lit('NAN'))
    .withColumn('partition', lit(input_folder))
    .withColumn('load_dates', current_date())
    .withColumnRenamed('city', 'municipality_nm')
    .withColumnRenamed('prov_state_ce', 'prov_or_state_ce')
    .withColumnRenamed('class', 'cft_classification_ce')
    .withColumnRenamed('recovery_mthd_type', 'cft_recovery_type_ce')
    .withColumnRenamed('volume', 'cft_bank_note_ct')
    .withColumnRenamed('recv_dt', 'received_dt')
    .withColumnRenamed('last_fld', 'bank_note_series_code')
    #.withColumn('load_id', f.regexp_replace(col('load_dates'), '-', '').cast('int'))
    #.withColumn(
    #    'key_address',
    #    f.concat_ws('|', 'country_ce', 'municipality_nm', 'prov_or_state_ce')
    #)
    .withColumn('bank_note_denom_am', df_out["bank_denom_am"])
    #.withColumn(
    #    'security_mthd_type_ce',
    #    f.expr("""
    #        CASE WHEN series_yr > 0 THEN
    #            CASE WHEN series_yr > 1987 AND bank_note_denom_am > 49 THEN 'OSD'
    #                 ELSE
    #                     CASE WHEN series_yr > 1990 AND bank_note_denom_am > 19 THEN 'OSD'
    #                          ELSE 'NONOSD'
    #                     END
    #            END
    #        ELSE ''
    #        END AS value_desc
    #    """)
    #)
    #.withColumn(
    #    'key_banknote',
    #    f.concat_ws('|', 'series_yr', 'bank_note_denom_am', 'security_mthd_type_ce')
    #)
)

# display
df_final_out.display()

# COMMAND ----------

# DBTITLE 1,Type casting
# Casting columns
df_final_out = (
    df_final_out
    .withColumn('received_dt', to_date(col('received_dt'), 'yyyyMMdd'))
    .withColumn('cft_bank_note_ct', col('cft_bank_note_ct').cast('int'))
    .withColumn('bank_note_denom_am', col('bank_note_denom_am').cast('int'))
    .withColumn('cft_bank_note_value_am', col('bank_note_denom_am') * col('cft_bank_note_ct'))
)

# display
df_final_out.display()

# COMMAND ----------

# DBTITLE 1,Select output columns
df_final_out = df_final_out.select(
        'received_dt',
        'cft_classification_ce',
        'cft_recovery_type_ce',
        'serial_nb',
        'cft_bank_note_ct',
        'cft_bank_note_value_am',
        'series_yr',
        'bank_note_denom_am',
        #'security_mthd_type_ce',
        #'initl_issue_dt',
        'issuing_country_ce',
        'country_ce',
        'municipality_nm',
        'prov_or_state_ce',
        'partition',
        'bank_note_series_code'
    )

# display
df_final_out.display()

# COMMAND ----------

# DBTITLE 1,Write to UC
## Write data to UC
rcmp_table = f"{unity_catalog_name}.standardized_db.tbl_rcmp_cft_boc_monthly"
# delete existing records
stmt = f"delete from {rcmp_table} where partition = '{input_folder}'"
spark.sql(stmt)
# write to UC table
df_final_out.write.format("delta").mode("append").saveAsTable(rcmp_table)

# COMMAND ----------

# DBTITLE 1,Exit Notebook
dbutils.notebook.exit("success")
