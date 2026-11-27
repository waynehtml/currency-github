# Databricks notebook source
import json
from pyspark.sql import functions as f
from pyspark.sql.types import *
import datetime
import pyspark.sql.types as t
import zipfile
import io
import pandas as pd
import openpyxl
import xml.etree.ElementTree as ET
from pyspark.sql import Row
import re
from pyspark.sql.dataframe import DataFrame

# COMMAND ----------

# DBTITLE 1,Parameters
env = dbutils.widgets.text("env","", "env")
container = dbutils.widgets.text("container","restricted", "container")
storage = dbutils.widgets.text("storage","", "storage")
input_date = dbutils.widgets.text("date","", "date")
site = dbutils.widgets.text("site","", "site")
mode = dbutils.widgets.text("mode","", "mode")

env = dbutils.widgets.get("env")
print(f"env: {env}")
container = dbutils.widgets.get("container")
print(f"container: {container}")
storage = dbutils.widgets.get("storage")
print(f"storage: {storage}")
input_date = dbutils.widgets.get("date")
print(f"input_date: {input_date}")
site = dbutils.widgets.get("site")
print(f"site: {site}")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

# DBTITLE 1,Load utility
# MAGIC %run /currency/common/utils

# COMMAND ----------

site_list = []

# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# mode
if mode == 'append':
  date_raw = input_date +'/'+site
else:
  date_raw = input_date +'/all_sites'

## Raw directory
# balance directory to [date]
base_balance_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/balance/"+ input_date
print(f"base_balance_path: {base_balance_path}")

# hc directory to [date]
base_hc_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/hc/"+ input_date
print(f"base_hc_path: {base_hc_path}")

# hc directory to [date]/[site]
raw_hc_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/hc/"+ date_raw
print(f"raw_hc_path: {raw_hc_path}")

# balance directory to [date]/[site]
raw_balance_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/balance/"+ date_raw
print(f"raw_balance_path: {raw_balance_path}")

# bn unconverted directory to [date]/[site]
raw_bin_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/unconverted/"+ date_raw
print(f"raw_bin_path: {raw_bin_path}")

## Standardized directory
standardized_unfit_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/standardized/internal/aoc_processing/m7/unfit/"+ input_date
print(f"standardized_unfit_path: {standardized_unfit_path}")

# target table name
unity_catalog_name = get_unity_catalog_name(env)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_unfits"
print(f"target_table_name: {target_table_name}")

# COMMAND ----------

# MAGIC %run ./m7_functions

# COMMAND ----------

def standardized_column_names(df: DataFrame) -> DataFrame:
    
    for column in df.columns:
        df = df.withColumnRenamed(column, column.lower())
    
    df = df.withColumnRenamed("downcount", "unfit_down_count").withColumnRenamed("machinecounter", "machine_counter").withColumnRenamed("dtotalamount", "headercard_total_amount").withColumnRenamed("headercardid", "headercard_id").withColumnRenamed("singled", "headercard_singled").withColumnRenamed("propdowncount", "unfit_property_count")
    
    df = df.drop("downcountperc")
    
    #maybe re add later
    df = df.drop("unfit_down_count", "quality", "millisec")
    
    df = df.withColumn("ingestion_date", f.current_date())
    
    df = df.select(["machine_serial_number","machine_variant","machine_version_info","shift_name","shift_number","shift_start_date","shift_start_time","shift_end_date","shift_end_time","headercard_id","headercard_singled","headercard_start_date","headercard_start_time","headercard_last_modify","headercard_total_amount","currency", "denomination","machine_counter","unfit_property","unfit_property_count", "ingestion_date","site_name"])
    
    
    return df

# COMMAND ----------

def get_unfit_rows(shift_tag_attributes: dict, machine_attributes: dict, hcunit_with_unfits: list,site_atrribute: dict) -> list:
    
    unfit_row_data = []
     
    for hcunit in hcunit_with_unfits:
         
        header_card_data = get_header_card_data(hcunit)
         
        unfit_input_data = hcunit.findall("UnfitInput")
         
        for i in range(len(unfit_input_data)):


            currency_input_tag_data = unfit_input_data[i].find("CurrencyInput")
            quality_input_tag_data = unfit_input_data[i].find("QualityInput")


            #no rows to append, return empty data list
            if not quality_input_tag_data:
                continue


            property_tag_data = unfit_input_data[i].find("QualityInput").findall("Properties")

            currency_input_tag_data = currency_input_tag_data.attrib
            currency_input_tag_data["currency"] = currency_input_tag_data.pop("Name")


            quality_input_tag_data = quality_input_tag_data.attrib
            quality_input_tag_data["quality"] = quality_input_tag_data.pop("Name")

            for property_tag in property_tag_data:

                curr_property = property_tag.attrib
                curr_property["unfit_property"] = curr_property.pop("Name")


                row_data = {**shift_tag_attributes, **header_card_data, **machine_attributes, **currency_input_tag_data, **quality_input_tag_data, **curr_property,**site_atrribute}
                unfit_row_data.append(row_data)




    return unfit_row_data


# COMMAND ----------

def standardized_data_types(df: DataFrame) -> DataFrame:

    
    for column in df.columns:
        
        if "DATE" in column.upper():
            df = df.withColumn(column, f.col(column).cast(t.DateType()))
            
        # elif ("TIME" in column.upper()) or ("LAST_MODIFY" in column.upper()): 
        #     df = df.withColumn(column , f.to_timestamp(column)) 
        
        elif column.upper() in ["MACHINE_SERIAL_NUMBER", "SHIFT_NUMBER", "MACHINE_COUNTER", "HEADERCARD_ID", "HEADERCARD_SINGLED", "HEADERCARD_TOTAL_AMOUNT", "UNFIT_PROPERTY_COUNT"]:
            df = df.withColumn(column, f.col(column).cast(t.IntegerType()))
        
        
    
    
    return df

# COMMAND ----------

def get_site(file_name,mode) -> dict:
  temp  = dict()
  if (mode =='append'):
    temp['site_name'] = site
    return temp
  else: 
    for _site in site_list:
      path = base_balance_path+'/'+_site
      files = dbutils.fs.ls(path)
      for inner_files in files:
        print('Site files are '+inner_files.name)
        if ( file_name == inner_files.name):
          print('Match '+_site)
          temp['site_name'] = _site
          return temp

# COMMAND ----------

all_files = dbutils.fs.ls(raw_balance_path)

all_dirs =  dbutils.fs.ls(base_balance_path)

for _site in all_dirs:
  print(_site.name)
  if (_site.name != 'all_sites/'):
      site_id = _site.name.rstrip(_site.name[-1])
      print(site_id)
      site_list.append(site_id)


unfit_rows = []
for file in all_files:
    
    site_atrribute = get_site(file.name,mode)
    shift_xml_root = get_xml_root(file)
    machine_attributes = get_machine_tag(shift_xml_root)
    hcunit_list = shift_xml_root.find("MACHINE").find("SHIFT").findall("HCUNIT")
    
    if len(hcunit_list) > 0:
         
        hcunit_with_unfits = []
        
        for i in range(len(hcunit_list)):
             
                
            if len(hcunit_list[i].findall("UnfitInput")) > 0:
                hcunit_with_unfits.append(hcunit_list[i])
        
        
       
        if len(hcunit_with_unfits) > 0:
           
            print("Processing shift file:", file.name,".................\n")
            
            shift_tag_attributes = parse_shift_balance(shift_xml_root) 
            unfit_data = get_unfit_rows(shift_tag_attributes, machine_attributes, hcunit_with_unfits,site_atrribute)
            unfit_rows = unfit_rows + unfit_data
             
         
    
if not unfit_rows:
    dbutils.notebook.exit("No unfit rows to write")    

df = spark.createDataFrame(data=unfit_rows)  
df = standardized_column_names(df)
df = standardized_data_types(df)

# COMMAND ----------

# DBTITLE 1,Write to lake
print(f"Writing to {standardized_unfit_path}")
df.write.format("parquet").mode(mode).save(standardized_unfit_path) 

# COMMAND ----------

# DBTITLE 1,Write to Delta
# Remove existing data with same file_date
if mode == 'overwrite':
    print(f"Removing existing data from {target_table_name}")
    try :
        dt = delta.DeltaTable.forName(spark, target_table_name)
        dt.delete(col("file_date") == to_date(lit(input_date), "yyyy-MM-dd"))
    except (Exception) as ex:
        print(f"Failed to remove the existing data from {target_table_name}, due to {ex}")

# Write data to target table
print(f"Writing to {target_table_name}")
df = df.withColumn("file_date", to_date(lit(input_date), "yyyy-MM-dd"))
df.write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
