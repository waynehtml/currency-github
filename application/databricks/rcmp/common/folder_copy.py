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
input_folder = dbutils.widgets.get("input_folder")
output_folder = dbutils.widgets.get("output_folder")
src_file  = dbutils.widgets.get("src_file")
out_file  = dbutils.widgets.get("out_file")


kv_scope = dbutils.widgets.text("kv_scope","lake-kv-currency-scope", "kv_scope")
spn_id_secret = dbutils.widgets.text("spn_id_secret","spn-currencyservices-id", "spn_id_secret")
spn_key_secret = dbutils.widgets.text("spn_key_secret","spn-currencyservices-key", "spn_key_secret")
tenant_id_secret = dbutils.widgets.text("tenant_id_secret","lake-tenant-id", "tenant_id_secret")

kv_scope = dbutils.widgets.get("kv_scope")
spn_id_secret = dbutils.widgets.get("spn_id_secret")
spn_key_secret = dbutils.widgets.get("spn_key_secret")
tenant_id_secret = dbutils.widgets.get("tenant_id_secret")

# COMMAND ----------

# MAGIC %run /currency/ISA/common/definition

# COMMAND ----------

# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

tryEnableSpnAccess(kv_scope, storage, spn_id_secret, spn_key_secret, tenant_id_secret)

# COMMAND ----------

input_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}/"
output_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{output_folder}/"


try:
        all_files = dbutils.fs.ls(f"{input_directory}")

        for _dir in all_files:
            print('The file name is '+_dir.name+'the size is  '+str(_dir.size))
            detail_files_dir = f"{input_directory}{_dir.name}"
            print('THe detail files are in the directory '+detail_files_dir)
            detail_files =  dbutils.fs.ls(f"{detail_files_dir}")
            for _file in  detail_files:
                print('The file name is '+_file.name+'the size is  '+str(_file.size))
                out_file_name = _file.name.replace(src_file,out_file)
                out_dir = f"{output_directory}{_dir.name}"
                input_full_path = f"{input_directory}{_dir.name}{_file.name}" 
                output_full_path = f"{out_dir}{out_file_name}"
                print("\n Input file"+input_full_path)
                print("\n output file"+output_full_path)

                dbutils.fs.cp(input_full_path,output_full_path)

except:
        print("There is an error in the source Read")
        dbutils.notebook.exit('zero')

# COMMAND ----------

input_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{input_folder}/"
output_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}/"


try:
        all_files = dbutils.fs.ls(f"{input_directory}")

        for _dir in all_files:
            print('The file name is '+_dir.name+'the size is  '+str(_dir.size))
            detail_files_dir = f"{input_directory}{_dir.name}"
            print('THe detail files are in the directory '+detail_files_dir)
            df = spark.read.format("parquet").load(detail_files_dir)

            out_dir = f"{output_directory}{_dir.name}"

            df.write.format("parquet").mode("overwrite").save(out_dir) 



except: 
        print("There is an error in the source Read")
        dbutils.notebook.exit('zero')
