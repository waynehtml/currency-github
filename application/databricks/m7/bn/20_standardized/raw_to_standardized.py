# Databricks notebook source
# DBTITLE 1,import statements
import json
from pyspark.sql.types import *
import pyspark.sql.dataframe as d
import pyspark.sql.functions as f
import pandas as pd
import re

# COMMAND ----------

# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,parameter
# Create widgets
env = dbutils.widgets.text("env", "")
storage = dbutils.widgets.text("storage", "")
land_storage = dbutils.widgets.text("land_storage", "")
date = dbutils.widgets.text("date", "")
mode = dbutils.widgets.text("mode", "")
site = dbutils.widgets.text("site", "")

# Get widget paramter
env = dbutils.widgets.get("env")
print(f"env: {env}")
storage = dbutils.widgets.get("storage")
print(f"storage: {storage}")
land_storage = dbutils.widgets.get("land_storage")
print(f"land_storage: {land_storage}")
date = dbutils.widgets.get("date")
print(f"date: {date}")
mode = dbutils.widgets.get("mode")
print(f"mode: {mode}")
site = dbutils.widgets.get("site")
print(f"site: {site}")

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

def path_exists(path):
    try:
        # try to list the contents of the path; if it exists, it will return a list
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        # if an exception is raised, the path does not exist
        return False

# COMMAND ----------

# DBTITLE 1,Process parameters
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Get land storage if not provided
if len(land_storage.strip()) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage: {land_storage}")

# COMMAND ----------

# Path to raw unconverted data
raw_u_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/unconverted"
print(f"raw_u_bnlist_path: {raw_u_bnlist_path}")

# Path to standardized data
std_u_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/standardized/internal/aoc_processing/m7/bn/list"
print(f"std_u_bnlist_path: {std_u_bnlist_path}")

# target table name
unity_catalog_name = get_unity_catalog_name(env)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_bnlist"
print(f"target_table_name: {target_table_name}")

# COMMAND ----------

# DBTITLE 1,find dates to be processed
if len(date) == 0:
    # all date
    date_list = get_subdirs(raw_u_bnlist_path)
elif "," in date:
    # comma separated list of date
    date_list = split_and_strip(date)
elif "~" in date:
    # range of date
    (head, end) = extract_exchange_range(date)
    date_list = get_subdirs(raw_u_bnlist_path)
    date_list = get_subset(date_list,head,end)
else:
    # single date
    date_list = [date]
print(f"date_list: {date_list}")

# COMMAND ----------

if len(site) == 0:
    # all site for all date
    site_list = []
elif "," in site:
    # comma separated list of site
    site_list = split_and_strip(site)
else:
    # single site
    site_list = [site]
print(f"site_list: {site_list}")

# COMMAND ----------

proc_notebook = "raw_to_standardized_proc"
proc_parameter = {}
proc_parameter['env'] = env
proc_parameter['storage'] = storage
proc_parameter['land_storage'] = land_storage

for date in date_list:
    print(f"Processing date: {date}")

    # unconverted path for date
    raw_u_bnlist_date_path = f"{raw_u_bnlist_path}/{date}"
    print(f"raw_u_bnlist_date_path: {raw_u_bnlist_date_path}")

    # skip if date does not exist
    if not path_exists(raw_u_bnlist_date_path):
        print(f"Skipping date: {date} as it does not exist in raw storage")
        continue

    if len(site_list) == 0:
        sites = get_subdirs(raw_u_bnlist_date_path)
        if 'all_sites' in sites:
            sites.remove('all_sites')
    else:
        sites = site_list
    print(f"sites for {date}: {sites}")

    # Remove existing data once for each date if overwrite is set
    if mode == 'overwrite':
        # lake
        std_u_bnlist_date_path = f"{std_u_bnlist_path}/{date}"
        if path_exists(std_u_bnlist_date_path):
            print(f"Deleting existing data in lake for date: {std_u_bnlist_date_path}")
            dbutils.fs.rm(std_u_bnlist_date_path, True)

        # UC
        print(f"Removing existing data in UC from {target_table_name}")
        sql = f""" DELETE FROM {target_table_name} WHERE file_date = '{date}' """
        spark.sql(sql)

    proc_parameter['date'] = date
    for site in sites:
        print(f"Processing site: {site}")

        # unconverted path for date
        raw_u_bnlist_date_site_path = f"{raw_u_bnlist_date_path}/{site}"
        print(f"raw_u_bnlist_date_site_path: {raw_u_bnlist_date_site_path}")
        # skip if site does not exist
        if not path_exists(raw_u_bnlist_date_site_path):
            print(f"Skipping {site} for {date} as it does not exist in raw storage")
            continue
        
        # process site for the date
        proc_parameter['site'] = site
        print(f"Running notebook: {proc_notebook}, with parameters: {proc_parameter}")
        dbutils.notebook.run(proc_notebook, 0, proc_parameter)
