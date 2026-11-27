# Databricks notebook source
import re
from dbruntime.dbutils import FileInfo

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


balance_path = "raw/internal/aoc_processing/m7/shift/balance/"+date_raw
hc_path = "raw/internal/aoc_processing/m7/shift/hc/"+date_raw

# COMMAND ----------

# MAGIC %run ./m7_functions

# COMMAND ----------


print("\n the balance path is "+"abfss://"+container+"@"+storage+".dfs.core.windows.net/"+balance_path)
balance_files = dbutils.fs.ls("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+balance_path)



# COMMAND ----------

try:
  hc_files = dbutils.fs.ls("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+hc_path)
except:
    hc_files = []

# COMMAND ----------

if len(hc_files) == 0 and len(balance_files) == 0:
    raise Exception("Both HC and Balance files have been ingested at a lesser count than expected.")
elif len(balance_files) == 0 and len(hc_files) > 0:
    raise Exception("HC files have been properly ingested. Balance files have been ingested at a lesser count than expected.")
elif len(hc_files) == 0 and len(balance_files) > 0:
    print("Warning, no headercards ingested. Banknotes weren't produced today or a test shift is being processed.")
else:
    print("HC and Balance files have passed total file count validation")

# COMMAND ----------

def validate_file_rows(file: FileInfo, missing_row_warnings: dict):
    
    xml_root = get_xml_root(file)
        
    if "Shift" in file.name:
        print('The Shift file to process'+file.name)
        row_count = [item for item in xml_root.find("MACHINE").find("SHIFT").findall("HCUNIT") if item.findall("COUNTER")]
            
    elif "HC" in file.name:
        print('The HC file to process'+file.name)
        row_count = [hcunit for hcunit in xml_root.iter('HCUNIT')]
        
     
    if len(row_count) == 0:
        missing_row_warnings["file_list"].append(file.name)
        print(file.name, "has warnings of missing rows")

# COMMAND ----------

hc_regex_pattern = "^[0-9]{7}_[0-9]{4}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_[0-9]{6}_HC_[0-9]{2}$"
balance_regex_pattern = "^[0-9]{7}_[0-9]{4}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])_[0-9]{6}_ShiftBalance$"

missing_row_warnings = { "file_list": []}

# COMMAND ----------




for file in balance_files + hc_files:
    
    print('The file to process is '+file.name)
    
    file_name_parsed = file.name.split(".")
    
    
    if file_name_parsed[1] != "xml":
        raise Exception(file.name, "has failed file extenstion validation.")
    
         
    if not re.compile(hc_regex_pattern).match(file_name_parsed[0]) and not re.compile(balance_regex_pattern).match(file_name_parsed[0]):
        raise Exception(file.name, "has failed file naming validation.")

        
    validate_file_rows(file, missing_row_warnings)
        

# COMMAND ----------

dbutils.notebook.exit(missing_row_warnings)
