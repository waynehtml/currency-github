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
from xml.etree.ElementTree import Element

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

# DBTITLE 1,load utils
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

if mode == 'append':
  date_raw = input_date +'/'+site
else:
  date_raw = input_date +'/all_sites'
print(f"date_raw: {date_raw}")

## Raw directory
# balance directory to [date]
base_balance_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/balance/"+input_date
print(f"base_balance_path: {base_balance_path}")

# hc directory to [date]
base_hc_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/hc/"+input_date
print(f"base_hc_path: {base_hc_path}")

# hc directory to [date]/[site]
raw_hc_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/hc/"+ date_raw

# balance directory to [date]/[site]
raw_balance_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/raw/internal/aoc_processing/m7/shift/balance/"+ date_raw

## Standardized directory
# counters directory to [date]
standardized_counters_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/standardized/internal/aoc_processing/m7/counters/"+input_date

# target table name
unity_catalog_name = get_unity_catalog_name(env)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_counts"
print(f"target_table_name: {target_table_name}")

# COMMAND ----------

# MAGIC %run ./m7_functions

# COMMAND ----------

def parse_parsect_data(hcunit: Element) -> dict:

    try:      
        header_card_list = [file for file in dbutils.fs.ls(raw_hc_path) if hcunit.attrib["HEADERCARD_START_TIME"].replace(":", "") in file.name]
    except:
        print("This shift has no headercards, nullify missing parsect")

        parsect_attributes = {"parsect_number":"", "containerid":"","operationmode":"","inputmode":"","stackingmode":"" ,"sortingmode":"" ,"expected":"" ,"PARSECT_DENOM_ID":"" ,"numberau":"","parsect_startdate":"","parsect_starttime":"" ,"parsect_enddate":"","parsect_endtime":""}
        return parsect_attributes
    

    print("\n THe list count of the header_card_list is"+str(len(header_card_list)))

    print(header_card_list)
    
    if len(header_card_list) > 0 :

          #headercard01, headercard02, headercard03 all have same metadata, use only 1
          header_card = header_card_list[0]

          print("\n I am printing the header_Card")
          print(header_card)
                
          header_card_root = get_xml_root(header_card)
          

          #get PARSECT and HCUNIT attribute values 
          parsect_attributes = header_card_root.find("MACHINE").find("PARSECT").attrib


          parsect_attributes["PARSECT_DENOM_ID"] = parsect_attributes.pop("denomid")
          parsect_attributes["parsect_number"] = parsect_attributes.pop("number")
          parsect_attributes["parsect_startdate"] = parsect_attributes.pop("startdate")
          parsect_attributes["parsect_starttime"] = parsect_attributes.pop("starttime")
          parsect_attributes["parsect_enddate"] = parsect_attributes.pop("enddate")
          parsect_attributes["parsect_endtime"] = parsect_attributes.pop("endtime")
    else:
          parsect_attributes = {"parsect_number":"", "containerid":"","operationmode":"","inputmode":"","stackingmode":"" ,"sortingmode":"" ,"expected":"" ,"PARSECT_DENOM_ID":"" ,"numberau":"","parsect_startdate":"","parsect_starttime":"" ,"parsect_enddate":"","parsect_endtime":""}

    return parsect_attributes

# COMMAND ----------

def counter_remove_duplicates(counter_tag_attributes: list) -> list:
    
    rows, visits = [], {}
    for pair in counter_tag_attributes:
        
         
        key = pair["denomid"] + pair["denomname"] + pair["quality"]  
        
        if key not in visits.keys():
            
            rows.append(pair)
            visits[key] = len(rows) - 1
            
             
        else:
        
            rows[visits[key]]["number"] = str(int(rows[visits[key]]["number"]) + int(pair["number"]))
            
         
        
    return rows

# COMMAND ----------

def standardized_column_names(df: DataFrame) -> DataFrame:
    
    
    for column in df.columns:
        df = df.withColumnRenamed(column, column.lower())
        
    df = df.withColumnRenamed("containerid", "parsect_container_id").withColumnRenamed("dtotalamount", "headercard_total_amount").withColumnRenamed("denomname", "denom_name")\
        .withColumnRenamed("parsect_enddate", "parsect_end_date").withColumnRenamed("parsect_endtime", "parsect_end_time").withColumnRenamed("denomid", "denom_id")\
        .withColumnRenamed("headercardid", "headercard_id").withColumnRenamed("inputmode", "input_mode").withColumnRenamed("singled", "headercard_singled")\
        .withColumnRenamed("lastmodify", "headercard_last_modify").withColumnRenamed("operationmode", "operation_mode").withColumnRenamed("sortingmode", "sorting_mode")\
        .withColumnRenamed("stackingmode", "stacking_mode").withColumnRenamed("parsect_startdate", "parsect_start_date").withColumnRenamed("parsect_starttime", "parsect_start_time")\
        .withColumnRenamed("numberau", "parsect_numberau").withColumnRenamed("expected", "parsect_expected")
    
    df = df.drop("szinfo1", "szinfo2", "szinfo3", "headercardno", "millisec", "issue", "stacker")
    
    df = df.withColumn("ingestion_date", f.current_date()).withColumn("fitness", f.when(df.quality == "Unfit", "Unfit").otherwise("Fit"))
    
    df = df.select("machine_serial_number","machine_variant","machine_version_info","shift_name","shift_number","shift_start_date","shift_start_time","shift_end_date","shift_end_time",
"headercard_id","headercard_singled","headercard_start_date","headercard_start_time","headercard_last_modify","headercard_total_amount", "rejects",
 "parsect_number", "parsect_container_id", "operation_mode", "input_mode", "stacking_mode", "sorting_mode", "parsect_expected", "parsect_denom_id", "parsect_numberau", "parsect_start_date", 
 "parsect_start_time", "parsect_end_date", "parsect_end_time","denom_id", "denom_name", "currency", "value", "quality", "fitness", "output", "number", "ingestion_date","site_name")
 
    return df

# COMMAND ----------

def standardized_data_types(df: DataFrame) -> DataFrame:
    
    for column in df.columns:
        
        if "DATE" in column.upper():
            df = df.withColumn(column, f.col(column).cast(t.DateType()))
            
        # elif ("TIME" in column.upper()) or ("LAST_MODIFY" in column.upper()): 
        #     df = df.withColumn(column , f.to_timestamp(column)) 
        
        elif column.upper() == "VALUE":
            df = df.withColumn(column, f.col(column).cast(t.DecimalType()))
            
        elif column.upper() in ["MACHINE_SERIAL_NUMBER", "SHIFT_NUMBER", "HEADERCARD_ID", "HEADERCARD_SINGLED", "HEADERCARD_TOTAL_AMOUNT", "PARSECT_NUMBER", "PARSECT_CONTAINER_ID", "PARSECT_DENOM_ID", "PARSECT_NUMBERAU", "DENOM_ID","NUMBER"]:
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

print('The raw balance path is '+raw_balance_path)

all_dirs =  dbutils.fs.ls(base_balance_path)

for _site in all_dirs:
  print(_site.name)
  if (_site.name != 'all_sites/'):
      site_id = _site.name.rstrip(_site.name[-1])
      print(site_id)
      site_list.append(site_id)

 
#iterate through each ShiftBalance file under current date
df, row_data = None, []
for file in all_files:

    print('Started processing the file '+file.name+' in the mode '+mode)

    site_atrribute = get_site(file.name,mode)
   
    
    shift_xml_root = get_xml_root(file)
 
    hcunit_list = shift_xml_root.find("MACHINE").find("SHIFT").findall("HCUNIT")
    
    if len(hcunit_list) == 0:
        print("\nShift file", file.name,"is empty, skipping it")
        continue
    
    
    print("\nProcessing shift file", file.name, "........... it has", len(hcunit_list), "HCUNIT tag(s)")

    
    machine_tag_attributes = get_machine_tag(shift_xml_root) 
    shift_tag_attributes = parse_shift_balance(shift_xml_root)
    
    print("\n Printing the hcunit_list \n")
    print(hcunit_list)

    for hcunit in hcunit_list:
        
        if not hcunit.findall("COUNTER"):
            continue

        hcunit_tag_attributes = get_header_card_data(hcunit, True)
        parsect_tag_attributes = parse_parsect_data(hcunit)
           
         
        #remove duplicates from counter rows before iterating/appending
        counter_tag_attributes = counter_remove_duplicates([element.attrib for element in hcunit.findall("COUNTER")])
        
         
        for counter in counter_tag_attributes:
    
            row_data.append({**machine_tag_attributes, **shift_tag_attributes, **hcunit_tag_attributes, **parsect_tag_attributes, **counter,**site_atrribute})
            # print(row_data)
            
        
        print("Processing shift file", file.name, "........... it has", len(counter_tag_attributes), "counter rows")
     

if not row_data:
    dbutils.notebook.exit("No counter rows to write")
    
df = spark.createDataFrame(data=row_data)
 

df = standardized_column_names(df)
df = standardized_data_types(df)

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Write to Lake
print(f"Writing data to {standardized_counters_path}")
df.write.format("parquet").mode(mode).save(standardized_counters_path) 

# COMMAND ----------

# DBTITLE 1,Write to UC
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
df = df.withColumn("value", col("value").cast(IntegerType()))
df.write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
