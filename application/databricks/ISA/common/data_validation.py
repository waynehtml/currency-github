# Databricks notebook source
# DBTITLE 1,Parameters
# Create parameter widgets
dbutils.widgets.text("env","")
dbutils.widgets.text("container","restricted", "container")
dbutils.widgets.text("storage","", "storage")
dbutils.widgets.text("file","acdptrace", "file")
dbutils.widgets.text("date","", "date")

# Get widget parameters
env = dbutils.widgets.get("env")
print(f"env: {env}")
container = dbutils.widgets.get("container")
print(f"container: {container}")
storage = dbutils.widgets.get("storage")
print(f"storage: {storage}")
input_file = dbutils.widgets.get("file")
print(f"input_file: {input_file}")
input_date = dbutils.widgets.get("date")
print(f"input_date: {input_date}")

# COMMAND ----------

# DBTITLE 1,load utils
# MAGIC %run /currency/common/utils

# COMMAND ----------

# DBTITLE 1,load common definition
# MAGIC %run /currency/ISA/common/definition

# COMMAND ----------

# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Compose input and output file path
input_file_type = input_file.lower()
if input_file_type == 'maocreissnotes':
    t_input_file_type = 'extractreissnotes'
else:
    t_input_file_type = input_file_type
print(f"t_input_file_type: {t_input_file_type}")

# input file path in raw zone
input_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{t_input_file_type}/{input_date}/"
print(f"input_directory: {input_directory}")

# all_files path in raw zone
all_files = dbutils.fs.ls(f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{t_input_file_type}/{input_date}/")
print(f"all_files: {all_files}")

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
    if validate_data_row(_file.name,input_file_type) !=0:
        raise Exception("Mismatched datatype or No data records in the file "+_file.name)
    else:
        print('All is good to process')
