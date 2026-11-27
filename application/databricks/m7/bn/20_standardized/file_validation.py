# Databricks notebook source
from dbruntime.dbutils import FileInfo
import re

# COMMAND ----------

container = dbutils.widgets.text("container","", "container")
storage = dbutils.widgets.text("storage","", "storage")
date = dbutils.widgets.text("date","", "date")

container = dbutils.widgets.get("container")
storage = dbutils.widgets.get("storage")
date = dbutils.widgets.get("date")


site = dbutils.widgets.get("site")
mode = dbutils.widgets.get("mode")
 
if mode == 'append':
  date_raw = date+'/'+site
else:
  date_raw = date+'/all_sites' 

# COMMAND ----------

unconverted_files = []

# COMMAND ----------

def validate_file_count(converted_files: list):

    #get filename from each CSV file object
    for i in range(len(converted_files)):
        converted_files[i] = converted_files[i].name


    #get .bin and .xml pairs meant to be converted to csv
    source_files = dbutils.fs.ls(f"abfss://{container}@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/unconverted/{date_raw}")


    for file in source_files:
        
        #get each filename excluding file extension
        if ".bin" in file.name:
            file_name = file.name.split(".")[0]   
        elif ".xml" in file.name:
            file_name = file.name.split("_Conv.")[0]  



        if file_name + ".csv" in converted_files:
            print(file_name, "passed file count validation")

        else:

            #pass files out of notebook for warning
            print(file_name, ".bin and .xml pair was not converted to CSV")
            unconverted_files.append(file.name)

# COMMAND ----------

def validate_file_naming_convention(file: FileInfo, filename: str):


    regex_pattern = "^BNList2_[0-9]{7}_[0-9]{4}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_[0-9]{6}_([0-9]{1}|[0-9]{2}).csv$"
     
    regex_compiler = re.compile(regex_pattern)
    

    if not regex_compiler.match(filename):
        raise Exception(file.name, "has the wrong naming convention")


# COMMAND ----------

def validate_file_extension(file: FileInfo, file_extension: str):
    
    if file_extension !="csv":
        raise Exception(file.name, "has the wrong file extension")
    

# COMMAND ----------

all_files = dbutils.fs.ls(f"abfss://{container}@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/converted/{date_raw}")

# COMMAND ----------

validate_file_count(all_files[:])

# COMMAND ----------

for file in all_files:
    
    validate_file_extension(file, file.name.split(".")[1])
    validate_file_naming_convention(file, file.name)

    print(file.name, "has passed filename and file extension validation")

# COMMAND ----------

dbutils.notebook.exit(unconverted_files)
