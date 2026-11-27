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

def validate_header_row(input_file,input_file_type):
    v_file_location=f"{input_directory}{input_file}"
    file_headers=ISA_File_Headers[input_file_type].fieldNames()
    df_inputfile = spark.read.option("header",True).csv(v_file_location)
    print(file_headers)
    print(df_inputfile.columns)

    file_headers_upper = [i.lower().strip() for i in file_headers]

    input_headers_upper = [i.lower().strip() for i in df_inputfile.columns]

    if file_headers_upper == input_headers_upper:
        return 0
    else: 
        return 1

def validate_mandatory_columns(input_file,input_file_type):
    v_file_location=f"{input_directory}{input_file}"
    file_headers=ISA_File_Headers[input_file_type].fieldNames()
    df_inputfile = spark.read.option("header",True).schema(ISA_File_Headers[input_file_type]).csv(v_file_location)
    cols = len(df_inputfile.columns)
    for counter in range(cols):
        if ISA_File_Headers[input_file_type][counter].metadata['NULL'] == 'N':
            lst=ISA_File_Headers[input_file_type].fieldNames()
            if (df_inputfile.where(col(lst[counter]).isNull()).count()) > 0 :
                print('The column '+lst[counter]+'is marked not null but value is null in the file'+input_file_type)
                return 1

            

            if (ISA_File_Headers[input_file_type][counter].metadata['format'] != 'N'):
                print('the date to be examined is for  '+lst[counter])
                 
                if ( df_inputfile.where(to_timestamp(from_unixtime(unix_timestamp(lst[counter],format=ISA_File_Headers[input_file_type][counter].metadata['format']))).isNull()).count()) > 0 :
                    print('The column '+lst[counter]+'is marked not null and the date format does not parse to the value expected '+input_file_type)
                    return 1

     

    return 0

# COMMAND ----------

for _file in all_files:
    print('The file name is '+_file.name+'the size is'+str(_file.size))

    if( _file.size != 0 ):
        print('All is good to process')
        dbutils.notebook.exit('nonzero')

        # if validate_header_row(_file.name,input_file_type) !=0:
        #     raise Exception("Header row not as per the spec "+_file.name)
        # elif validate_mandatory_columns(_file.name,input_file_type) !=0:
        #     raise Exception("Mandatory Columns are not populated as per the spec "+_file.name)
        # else:
        #     print('All is good to process')
        #     dbutils.notebook.exit('nonzero')
    
    else:
        print('Zero Size file')
        dbutils.notebook.exit('zero')
