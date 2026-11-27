# Databricks notebook source
import json
from pyspark.sql.types import *
import pyspark.sql.dataframe as d
import pyspark.sql.functions as f
import pandas as pd
import re

# COMMAND ----------

# MAGIC %run ../../../common/utils

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

def get_csv_files(path):
    try:
        # try to list the contents of the path; if it exists, it will return a list
        files = dbutils.fs.ls(path)
        file_names = [f.name for f in files if f.name.endswith(".csv")]
        return file_names
    except Exception as e:
        # if an exception is raised, the path does not exist
        return []

# COMMAND ----------

# environment
env = get_env()
print(f"env: {env}")

# lake storage
storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# path to raw unconverted data
raw_u_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/unconverted"
print(f"raw_u_bnlist_path: {raw_u_bnlist_path}")

# path to raw converted data
raw_c_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/converted"
print(f"raw_c_bnlist_path: {raw_c_bnlist_path}")

# COMMAND ----------

# DBTITLE 1,Find mismatched files
full_list = []
bad_list = []

# Get date list in unconverted diretory
date_list = get_subdirs(raw_u_bnlist_path)
#date_list = ["2023-10-30","2024-11-12"] # for testing
print(f"date_list: {date_list}")

# Interpret date list to find unmatched date
for date in date_list:
    print(f"Processing date: {date}")

    # Paths for date
    raw_u_bnlist_date_path = f"{raw_u_bnlist_path}/{date}"
    #print(f"raw_u_bnlist_date_path: {raw_u_bnlist_date_path}")
    raw_c_bnlist_date_path = f"{raw_c_bnlist_path}/{date}"
    #print(f"raw_c_bnlist_date_path: {raw_c_bnlist_date_path}")
    
    # Find sites
    site_list = get_subdirs(raw_u_bnlist_date_path)
    if 'all_sites' in site_list:
            site_list.remove('all_sites')
    
    # empty date
    if len(site_list) == 0:
        bad_list.append({'date': date, 'site': '', 'file':''})
        continue

    for site in site_list:
        print(f"Processing site: {site}")

        # path for site
        raw_u_bnlist_date_site_path = f"{raw_u_bnlist_date_path}/{site}"
        #print(f"raw_u_bnlist_date_site_path: {raw_u_bnlist_date_site_path}")
        raw_c_bnlist_date_site_path = f"{raw_c_bnlist_date_path}/{site}"
        #print(f"raw_c_bnlist_date_site_path: {raw_c_bnlist_date_site_path}")

        # list unconverted bin files
        u_files = dbutils.fs.ls(raw_u_bnlist_date_site_path)
        u_bin_files = [f.name for f in u_files if f.name.endswith(".bin")]
        #print(f"u_bin_files: {u_bin_files}")

        # empty site
        if len(u_bin_files) == 0:
            bad_list.append({'date': date, 'site': site, 'file':''})
            continue
        else:
            full_list.append({'date': date, 'site': site})

        # list converted csv files
        c_csv_files = get_csv_files(raw_c_bnlist_date_site_path)
        #print(f"c_csv_files: {c_csv_files}")
        
        for u_bin_file in u_bin_files:
            file_name = u_bin_file.replace(".bin", ".csv")
            if file_name not in c_csv_files:
                #print(f"File not found: {file_name}")
                bad_list.append({'date': date, 'site': site, 'file': u_bin_file})

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Fix mismatch between unconverted and converted

# COMMAND ----------

if len(bad_list) > 0:
    # Step 1: Show mismatched files
    bad_df = spark.createDataFrame(bad_list)
    bad_df.display()
    
    # Step 2: Delete dates in converted directory with a comma separated date list 
    unique_dates = list(set([item['date'] for item in bad_list]))
    unique_dates.sort()
    date_string = ','.join(unique_dates)
    print(f"Step 1: Delete following dates in converted directory: {date_string}")

    # Step 3: Rerun BN converter for following date and site
    print(f"Step 2: Rerun BN converter for the following dates and sites")
    for date in unique_dates:
        raw_u_bnlist_date_path = f"{raw_u_bnlist_path}/{date}"
        site_list = get_subdirs(raw_u_bnlist_date_path)
        site_to_remove = [s for s in site_list if s != "all_sites"]
        print(f"Date: {date}, sites: {site_to_remove}")
else:
    print("No mismatched files found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Fix mismatch between converted and standardized

# COMMAND ----------

# Convert full list to dataframe with date and site columns
raw_u_df = spark.createDataFrame(full_list)

# read standardize data with unqiue date & site
std_u_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/standardized/internal/aoc_processing/m7/bn/list/*"
print(f"std_u_bnlist_path: {std_u_bnlist_path}")
std_df = spark.read.parquet(std_u_bnlist_path).select(col('date').alias('std_date'), col('site_name').alias('std_site')).distinct()

# find mismatched date
mismatched_df = raw_u_df.join(std_df, (raw_u_df.date == std_df.std_date) & (raw_u_df.site == std_df.std_site), "left").filter(col("std_date").isNull()).sort("date", "site")
mismatched_df.display()

# compose date string for reprocessing
mismatched_date_df = mismatched_df.orderBy("date").select("date").distinct()
date_string = mismatched_date_df.select(f.concat_ws(",", f.collect_list("date"))).collect()[0][0]
print(f"Step 3: Rerun raw_to_standardized to process the following dates: {date_string}")

