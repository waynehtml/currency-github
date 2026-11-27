# Databricks notebook source
from pyspark.sql import functions as f
import pyspark.sql.types as t
from datetime import date as py_date
from pyspark.sql.dataframe import DataFrame

# COMMAND ----------

container = dbutils.widgets.text("container","", "container")
storage = dbutils.widgets.text("storage","", "storage")
date = dbutils.widgets.text("date","", "date")

container = dbutils.widgets.get("container")
storage = dbutils.widgets.get("storage")
date = dbutils.widgets.get("date")

kv_scope = dbutils.widgets.text("kv_scope","", "kv_scope")
spn_id_secret = dbutils.widgets.text("spn_id_secret","", "spn_id_secret")
spn_key_secret = dbutils.widgets.text("spn_key_secret","", "spn_key_secret")
tenant_id_secret = dbutils.widgets.text("tenant_id_secret","", "tenant_id_secret")

kv_scope = dbutils.widgets.get("kv_scope")
spn_id_secret = dbutils.widgets.get("spn_id_secret")
spn_key_secret = dbutils.widgets.get("spn_key_secret")
tenant_id_secret = dbutils.widgets.get("tenant_id_secret")

standardized_rejection_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/standardized/internal/aoc_processing/m7/rejections/"+date
curated_rejection_path = "abfss://"+container+"@"+storage+".dfs.core.windows.net/curated/currency/m7/rejections/"

# COMMAND ----------

# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

tryEnableSpnAccess(kv_scope, storage, spn_id_secret, spn_key_secret, tenant_id_secret)

# COMMAND ----------

def get_earliest_hcunit(df: DataFrame) -> DataFrame:
    
    df2 = df.groupBy("HEADERCARD_ID").agg(f.min("HEADERCARD_START_TIME").alias("HEADERCARD_START_TIME"))
    
    df = df.join(df2, ["HEADERCARD_ID", "HEADERCARD_START_TIME"], how="inner")
    
    return df

# COMMAND ----------

df = spark.read.format("parquet").load(standardized_rejection_path)

df = get_earliest_hcunit(df)

# COMMAND ----------

try:      
  
  #cache file read or spark doesnt let us write to same path
  existingDf = spark.read.format("parquet").load(curated_rejection_path)
  existingDf.cache()  

  existingDf = existingDf.filter((existingDf.INGESTION_DATE != py_date.today()) & (existingDf.SHIFT_START_DATE != date))
  df = df.unionByName(existingDf)


except Exception as e: 
  print("Could not load and filter existing M7 data frame. Error here:", e)


df.write.format("parquet").mode("overwrite").save(curated_rejection_path) 

# COMMAND ----------


