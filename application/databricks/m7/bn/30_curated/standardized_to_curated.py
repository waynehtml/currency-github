# Databricks notebook source
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

# COMMAND ----------

# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

tryEnableSpnAccess(kv_scope, storage, spn_id_secret, spn_key_secret, tenant_id_secret)

# COMMAND ----------

df = spark.read.format("parquet").load(f"abfss://{container}@{storage}.dfs.core.windows.net/standardized/internal/aoc_processing/m7/shift/{date}/")

# COMMAND ----------

try:      
  #cache file read or spark doesnt let us write to same path
  existingDf = spark.read.format("parquet").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/curated/currency/m7/")
  existingDf.cache()  

  existingDf = existingDf.filter(existingDf.DATE != date)
  df = df.unionByName(existingDf)


except Exception as e: 
  print("Could not load and filter existing M7 data frame. Error here:", e)


df.write.format("parquet").mode("overwrite").save("abfss://"+container+"@"+storage+".dfs.core.windows.net/curated/currency/m7/") 

# COMMAND ----------


