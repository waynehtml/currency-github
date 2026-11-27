# Databricks notebook source
from pyspark.sql import functions as f
import pyspark.sql.types as t
import json

# COMMAND ----------

container = dbutils.widgets.text("container","restricted", "container")
kv_scope = dbutils.widgets.text("kv_scope","lake-kv-currency-scope", "kv_scope")
spn_id_secret = dbutils.widgets.text("spn_id_secret","spn-currencyservices-id", "spn_id_secret")
spn_key_secret = dbutils.widgets.text("spn_key_secret","spn-currencyservices-key", "spn_key_secret")
tenant_id_secret = dbutils.widgets.text("tenant_id_secret","lake-tenant-id", "tenant_id_secret")
env = dbutils.widgets.get("environment")

 


container = dbutils.widgets.get("container")
kv_scope = dbutils.widgets.get("kv_scope")
spn_id_secret = dbutils.widgets.get("spn_id_secret")
spn_key_secret = dbutils.widgets.get("spn_key_secret")
tenant_id_secret = dbutils.widgets.get("tenant_id_secret")

# COMMAND ----------

# MAGIC %run /currency/ISA/common/definition

# COMMAND ----------

# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

if env == 'dev':
    storage =  "lakecacnpdstdev01" 
elif env == 'stg':
    storage =  "lakecacprdststg01"
else:
    storage =  "lakecacprdstprd01"

ISA_Target_Location= "standardized/internal/inventory_supply_accounting/inventorysnapshot"
ISA_Target_Location_temp = "standardized/internal/inventory_supply_accounting/inventorysnapshot_temp"

# COMMAND ----------

tryEnableSpnAccess(kv_scope, storage, spn_id_secret, spn_key_secret, tenant_id_secret)

# COMMAND ----------


input_file_type = "inventorysnapshot"


dirs = dbutils.fs.ls(f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}")

for _iter in dirs:
  input_directory = _iter.name
  print(input_directory)

  inp_location = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{input_directory}"

  out_location = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location_temp}/{input_directory}"

  df_inputfile = spark.read.option("header",True). parquet(inp_location)

  col_list = df_inputfile.columns

  if 'snapshot_date' in col_list:
    print('Do Nothing')
    df_calculated = df_inputfile
  else:
    try:
      print('no snapshot date')
      df_calculated = df_inputfile.withColumn('snapshot_date',df_inputfile.ingestion_date-1)
    except Exception as e:
      print('Not handling')
  
  df_calculated.select('snapshot_date','cash_centre','denom_category','denom_count','denom_value','total_value','type_value','denom_value_source','denom_type','denom_type_en','denom_type_fr','invalid_media_type','special_stacker','ingestion_date').write.format("parquet").mode("overwrite").save(out_location)


 

# COMMAND ----------

dbutils.fs.mv(f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}",f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}_202411", recurse=True)

dbutils.fs.mv(f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location_temp}",f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}", recurse=True)
