# Databricks notebook source
# DBTITLE 1,import module and function
from pyspark.sql.functions import col, DataFrame, to_date, to_timestamp, substring_index, trim, when, substring, lit, current_timestamp
from pyspark.sql.types import IntegerType, DecimalType
from datetime import date
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Get currency catalog name according to environment
def get_unity_catalog_name(environment:str):
    """
    Returns unity catalog name/prefix based on the environment parameter

    Args
    environment:environment name for which the catalog needs to be returned

    Returns
    unity catalog name for the prefix

    """

    catalog_name = environment + '_currency_cat'

    return catalog_name

# COMMAND ----------

# DBTITLE 1,Get boc_internal name according to environment
def get_interal_unity_catalog_name(environment:str):
    """
    Returns unity catalog name/prefix based on the environment parameter

    Args
    environment:environment name for which the catalog needs to be returned

    Returns
    unity catalog name for the prefix

    """

    if environment == 'dev' or environment == 'sbx':
        prefix = "dev"
    else:
        prefix = environment;
    
    catalog_name = prefix + '_boc_internal_src_cat';
    return catalog_name

# COMMAND ----------

def get_land_st_name(environment:str):
    if (environment == "dev") :
        land_st_name = "lakecacnpdstdevland01"
    if (environment == "stg") :
        land_st_name = "lakecacprdststgland01"
    if (environment == "prd") : 
        land_st_name = "lakecacprdstprdland01"

    return land_st_name

# COMMAND ----------

def get_lake_storage_account(environment):
    storage_map = {
        "dev": "lakecacnpdstdev01",
        "stg": "lakecacprdststg01",
        "prd": "lakecacprdstprd01"
    }
    return storage_map.get(environment, "lakecacnpdstdev01")

# COMMAND ----------

def get_env():
    # Get the resource group name, sample: lake-cac-npd-rg-manageddbwdevuc01
    rg = spark.sparkContext.getConf().get("spark.databricks.clusterUsageTags.managedResourceGroup")

    # Split by '-' and get the real name
    rg_name = rg.split("-")[-1]

    # Define the keywords to search for
    keywords = ["dev", "stg", "prd"]

    # Check for any keyword in the last part
    found_keyword = next((keyword for keyword in keywords if keyword in rg_name), None)

    return found_keyword

# COMMAND ----------

def replace_column_value(catalog, replacements, update = "false"):
    for config in replacements:
        index = config["index"]
        table = config["table"]
        column = config["column"]
        old = config["old"]
        new = config["new"]

        # Construct SQL statements
        # select sql
        select_sql = f"""
            SELECT COUNT(*) as row_count FROM {catalog}.{table} where {column} like '{old}'
        """
        #print(f"select_sql: {select_sql}")

        # update sql
        update_sql = f"""
        UPDATE {catalog}.{table}
        SET {column} = '{new}'
        WHERE {column} like '{old}'
        """
        #print(f"update_sql: {update_sql}")

        try:
            select_df = spark.sql(select_sql)
            select_count = select_df.collect()[0]["row_count"]
            print(f"Index {index}: {select_count} rows to be updated on '{old}' in {table}.{column}")

            if update == "true":
                spark.sql(update_sql)
                print("Updated.")
        except Exception as e:
            print(f"Failed to process index: {index}, Error: {e}")
