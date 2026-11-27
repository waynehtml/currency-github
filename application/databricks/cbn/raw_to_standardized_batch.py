# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import time
from datetime import timedelta
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# Create parameter widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("env","")
dbutils.widgets.text("storage","")
dbutils.widgets.text("file_type","ssn")
dbutils.widgets.text("file_date","")
dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"])

# Get environment if not provided
env = dbutils.widgets.get("env")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
storage = dbutils.widgets.get("storage")
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Check file type if not provided
file_type = dbutils.widgets.get("file_type")
if (len(file_type.strip()) == 0):
    raise Exception("Parameter file_type is required.")
print(f"file_type: {file_type}")

# Check file date if not provided
file_date = dbutils.widgets.get("file_date")

# Get widget parameters
mode = dbutils.widgets.get("mode")
print(f"mode: {mode}")

# COMMAND ----------

def get_subset(sorted_array, head=None, end=None):
    """
    Get subset of sorted array from head to end inclusively.
    If head is None, start from beginning.
    If end is None, end at last element.
    """
    # If array is empty, return empty array
    if not sorted_array:
        return []
        
    start_idx = 0
    end_idx = len(sorted_array)
    
    # Find start index if head provided
    if head is not None:
        for i, s in enumerate(sorted_array):
            if s == head:
                start_idx = i
                break
    
    # Find end index if end provided
    if end is not None:
        for i, s in enumerate(sorted_array):
            if s == end:
                end_idx = i
                break
    
    return sorted_array[start_idx:end_idx+1]

def extract_exchange_range(range):
    # Split the range string by the tilde (~)
    head, tail = range.split('~')
    
    # Replace empty strings with None for omitted parts
    head = head if head else None
    tail = tail if tail else None
    
    return head, tail

def get_subdirs(base_path):
    subdirs = [f.name.rstrip('/') for f in dbutils.fs.ls(base_path) if f.isDir()]
    return subdirs

def duration(start_time):
    # Calculate the duration
    duration_seconds = time.time() - start_time
    return timedelta(seconds=duration_seconds)

def split_and_strip(str, separator=','):
    return [item.strip() for item in str.split(separator)]

# COMMAND ----------

# input file path in raw zone
raw_base_path = "raw/external/cbn"
raw_full_path = f"abfss://restricted@{storage}.dfs.core.windows.net/{raw_base_path}/{file_type}"
#raw_date_list = get_subdirs(raw_full_path)

# COMMAND ----------

# Exchanges
if len(file_date) == 0:
    # all exchanges
    dates = get_subdirs(raw_full_path)
elif "," in file_date:
    # comma separated list of exchanges
    dates = split_and_strip(file_date)
elif "~" in file_date:
    # range of dates
    (head, end) = extract_exchange_range(file_date)
    dates = get_subdirs(raw_full_path)
    dates = get_subset(dates,head,end)
else:
    # single exchange
    dates = [file_date]

print(f"dates: {dates}")

# COMMAND ----------

notebook = "raw_to_standardized"
parameter = {}
parameter['env'] = env
parameter['storage'] = storage
parameter['file_type'] = file_type
parameter['mode'] = mode
for date in dates:
    print(f"Processing date: {date}")
    parameter['file_date'] = date

    print(f"Running notebook: {notebook}, with parameters: {parameter}")
    dbutils.notebook.run(notebook, 0, parameter)
