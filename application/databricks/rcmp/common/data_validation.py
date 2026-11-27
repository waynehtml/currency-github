# Databricks notebook source
container = dbutils.widgets.text("container","restricted", "container")
land_container = dbutils.widgets.text("land_container","land-currency", "land_container")
storage = dbutils.widgets.text("storage","restricted", "storage")
land_storage = dbutils.widgets.text("land_storage","", "land_storage")
input_file = dbutils.widgets.text("file","4Ons", "file")
input_date = dbutils.widgets.text("date","2023-02-01", "date")


container = dbutils.widgets.get("container")
land_container = dbutils.widgets.get("land_container")
storage = dbutils.widgets.get("storage")
land_storage = dbutils.widgets.get("land_storage")
input_file = dbutils.widgets.get("file")
input_date = dbutils.widgets.get("date")

input_file_type = input_file.lower()


kv_scope = dbutils.widgets.text("kv_scope","lake-kv-currency-scope", "kv_scope")
spn_id_secret = dbutils.widgets.text("spn_id_secret","spn-currencyservices-id", "spn_id_secret")
spn_key_secret = dbutils.widgets.text("spn_key_secret","spn-currencyservices-key", "spn_key_secret")
tenant_id_secret = dbutils.widgets.text("tenant_id_secret","lake-tenant-id", "tenant_id_secret")

kv_scope = dbutils.widgets.get("kv_scope")
spn_id_secret = dbutils.widgets.get("spn_id_secret")
spn_key_secret = dbutils.widgets.get("spn_key_secret")
tenant_id_secret = dbutils.widgets.get("tenant_id_secret")

# COMMAND ----------

# MAGIC %run /currency/rcmp/common/definition

# COMMAND ----------

# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

tryEnableSpnAccess(kv_scope, storage, spn_id_secret, spn_key_secret, tenant_id_secret)

# COMMAND ----------

input_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{RCMP_Base_Location}/{input_file_type}/{input_date}/"


all_files = dbutils.fs.ls(f"abfss://{container}@{storage}.dfs.core.windows.net/{RCMP_Base_Location}/{input_file_type}/{input_date}/")


# COMMAND ----------

def validate_data_row(input_file,input_file_type):
    v_file_location=f"{input_directory}{input_file}"
    file_headers=ISA_File_Headers[input_file_type].fieldNames()
    df_inputfile = spark.read.option("header",True).schema(ISA_File_Headers[input_file_type]).csv(v_file_location)
    cols = len(df_inputfile.columns)
    for counter in range(cols):
        lst=ISA_File_Headers[input_file_type].fieldNames()
        if ISA_File_Headers[input_file_type][counter].metadata['format'] != 'N':
            print('The date validated for '+lst[counter])
            df_calculated=df_inputfile.withColumn("newts",from_unixtime(unix_timestamp(lst[counter],format=ISA_File_Headers[input_file_type][counter].metadata['format'])))
            df_calculated.show()
            if (df_calculated.count()) <= 0 :
                return 1
    
    return 0
 

# COMMAND ----------

 
for _file in all_files:
    print('The file name is '+_file.name)
    # if validate_data_row(_file.name,input_file_type) !=0:
    #     raise Exception("Mismatched datatype or No data records in the file "+_file.name)
    # else:
    #     print('All is good to process')



