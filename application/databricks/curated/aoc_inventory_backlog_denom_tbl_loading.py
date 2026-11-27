# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Connection Information & Packages
# MAGIC

# COMMAND ----------

## import required libraries
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import pyspark.sql.types as t
from datetime import date as py_date
import pandas as pd
import numpy as np

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
internal_unity_catalog_name = get_interal_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Provide connection information
#connection information
land_storage = dbutils.widgets.text("land_storage","lakecacnpdstdevland01", "land_storage")
land_storage = dbutils.widgets.get("land_storage")
land_container = dbutils.widgets.text("land_container","config-currency", "land_container")
land_container = dbutils.widgets.get("land_container")


# COMMAND ----------


denom_type_mapping_df = spark.read.format("csv").load(f"abfss://{land_container}@{land_storage}.dfs.core.windows.net/inventory/mapping/Denom_type_inventory_mapping.csv", header = True)
denom_type_mapping_df.display()

# COMMAND ----------

denom_type_mapping_df.createOrReplaceTempView("temp_mapping")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Define the Unity Catalog table path
table_path = f"{unity_catalog_name}.curated_db.tbl_denom_type_inv_mapping"

# Define the SQL query to overwrite data in the curated table with data from the temp view
overwrite_query = f"""
INSERT OVERWRITE TABLE {table_path}
SELECT 
    inventory_type_code,
    inventory_type_category,
    inventory_type_subcategory,
    inventory_type_name
FROM 
    temp_mapping
"""

try:
    # Execute the overwrite query
    spark.sql(overwrite_query)
    print("Data from temp_mapping has successfully overwritten the existing data in the curated table.")
except AnalysisException as e:
    print(f"AnalysisException occurred: {e}")


