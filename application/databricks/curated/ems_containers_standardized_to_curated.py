# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Connection Information & Packages

# COMMAND ----------

## import required libraries
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get input parameter
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

# Get environment if not provided
if len(environment) == 0:
    environment = get_env()
print(f"environment: {environment}")

# catalog name
unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

# define standardized table name
ems_std_table = f"{unity_catalog_name}.standardized_db.tbl_isa_emscontainersnapshot"
print(f"ems_std_table: {ems_std_table}")

# Define the curated table name
ems_curated_table = f"{unity_catalog_name}.curated_db.tbl_ems_containers_daily"
print(f"ems_curated_table: {ems_curated_table}")

# COMMAND ----------

# DBTITLE 1,Create extract dataframe
ems_extract_sql = f"""
    SELECT 
        DATE(snapshot_date) AS snapshot_date, 
        cash_centre, 
        container_type, 
        isa_location, 
        COUNT(isa_container_id) AS containers
    FROM 
        {ems_std_table}
    GROUP BY 
        DATE(snapshot_date), cash_centre, container_type, isa_location 
    ORDER BY 
        DATE(snapshot_date) DESC, cash_centre, container_type, isa_location
"""

# Create dataframe with full data
ems_extract_df = spark.sql(ems_extract_sql)

# filter in only the date for incremental load
if load_type == "incremental_load":
    print(f"filtering data for date: {date}")
    ems_extract_df = ems_extract_df.filter(f"date(snapshot_date) = '{date}'")

# if empty on incremental load, exit the notebook with no_date
ems_extract_count = ems_extract_df.count()
print(f"ems_extract_count: {ems_extract_count}")
if (load_type == "incremental_load" and ems_extract_count == 0):
    dbutils.notebook.exit("no_date")

# for testing
ems_extract_df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Write data to UC
if load_type == "initial_load":
    print(f"Overwriting the table with full data")

    # overwrite the table
    ems_extract_df.write.format("delta").mode("overwrite").saveAsTable(ems_curated_table)
else:
    print(f"Appending new data for {date} to the table")

    # Clean up the existing data to avoid duplication
    ems_extract_cleanup_sql = f"delete from {ems_curated_table} where snapshot_date = '{date}'"
    spark.sql(ems_extract_cleanup_sql)

    # append data to the table
    ems_extract_df.write.format("delta").mode("append").saveAsTable(ems_curated_table)

# COMMAND ----------

# Signal successful completion and return a value
dbutils.notebook.exit("pass")
