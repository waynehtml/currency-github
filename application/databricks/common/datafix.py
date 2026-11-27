# Databricks notebook source
from pyspark.sql import functions as f
import pyspark.sql.types as t
import json

# COMMAND ----------

 
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
# input_folder = dbutils.widgets.get("input_folder")
# output_folder = dbutils.widgets.get("output_folder")



kv_scope = dbutils.widgets.text("kv_scope","lake-kv-currency-scope", "kv_scope")
spn_id_secret = dbutils.widgets.text("spn_id_secret","spn-currencyservices-id", "spn_id_secret")
spn_key_secret = dbutils.widgets.text("spn_key_secret","spn-currencyservices-key", "spn_key_secret")
tenant_id_secret = dbutils.widgets.text("tenant_id_secret","lake-tenant-id", "tenant_id_secret")

kv_scope = dbutils.widgets.get("kv_scope")
spn_id_secret = dbutils.widgets.get("spn_id_secret")
spn_key_secret = dbutils.widgets.get("spn_key_secret")
tenant_id_secret = dbutils.widgets.get("tenant_id_secret")

# COMMAND ----------

# container =  "restricted" 
# land_container =  "land-currency" 
# storage =  "lakecacnpdstdev01" 
# kv_scope =  "lake-kv-currency-scope" 
# spn_id_secret =  "spn-currencyservices-id" 
# spn_key_secret =  "spn-currencyservices-key" 
# tenant_id_secret =  "lake-tenant-id" 
# land_storage = "lakecacnpdstdevland01"

# ISA_Base_Location = "raw/internal/inventory_supply_accounting"

# ISA_Target_Location= "standardized/internal/inventory_supply_accounting"

# COMMAND ----------

# MAGIC %run /currency/ISA/common/definition

# COMMAND ----------

# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

tryEnableSpnAccess(kv_scope, storage, spn_id_secret, spn_key_secret, tenant_id_secret)

# COMMAND ----------

ISA_File_Hist = {
 "deposittohc" : StructType([  
    StructField("linkid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("nesid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("pcd",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'}), \
    StructField("nes_deposit_date",DateType(),False, metadata={'NULL':'Y','format':'YYYY-MM-DD'}), \
    StructField("nes_shipping_date",StringType(),False, metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'}), \
    StructField("aoc_received_date",StringType(),False, metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'}), \
    StructField("client_rdp",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_fi",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("cashcentre", StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("closed",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'})
  ]),
  "hctoclient" : StructType([ \
     StructField("linkid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("counting_link",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("pcd",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'}), \
    StructField("machine_date",StringType(),False, metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'}), \
    StructField("deposit_rule",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_rdp",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_fi", StringType(), False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("valuecounting", StringType(), False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("denom", StringType(), False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("denom_name", StringType(), False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbdenom",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbreject",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbkeyin",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("count_value",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("reject_value",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("prep_value",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("cashcentre", StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("closed",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'})
  ]),
  "4000perparcel" : StructType([ \
    StructField("linkid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("nesid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("parcelid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("deposit_rule",StringType(),False, metadata={'NULL':'N','format':'N'}), \
    StructField("nbdenom",IntegerType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_rdp",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_fi",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    # StructField("isa_process_date", StringType(), False,metadata={'NULL':'N','format':'yyyy-mm-dd HH:mm:ss'}),
    StructField("closed", StringType(), False,metadata={'NULL':'N','format':'yyyy-mm-dd HH:mm:ss'})
  ])
}

# COMMAND ----------

# Raw and break up of the per parcel
input_folder='4000perparcel'
output_folder='4000perparcel'
input_file_type='4000perparcel'



h_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/history/"
i_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}/"
o_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}/"
 
print(i_directory)

 
hist_file = h_directory+'/4pp_hist.csv'
hist_df = spark.read.option("header",True).option("encoding", "ISO-8859-1").schema(ISA_File_Hist[input_folder]).csv(hist_file)
 
hist_df = hist_df.withColumn('split_closed',f.substring(hist_df.closed,1,10))

display(hist_df)

list_dates = hist_df.select('split_closed').rdd.flatMap(lambda x:x).distinct().collect()

# # list_dates = ['2023-04-18']
 

for tup in range(len(list_dates)):
    print(list_dates[tup])
    i_directory_tar = i_directory+list_dates[tup] 
    i_directory_temp = i_directory+'temp'
    # print(i_directory_tar)
    df_calculatedy = hist_df.select('nesid','parcelid','deposit_rule','nbdenom','client_rdp','client_fi','closed').filter(col("split_closed") == list_dates[tup] )



    df_calculatedy.coalesce(1).write.format("csv").mode("overwrite").save(i_directory_temp,header=True) 

    mv_src = i_directory_temp+'/part*.csv'
    mv_tar = i_directory_tar+'/'+'4000perparcel_'+list_dates[tup]+'.csv'

    files = dbutils.fs.ls(i_directory_temp)

    for _file in files:
        if _file.name.endswith('.csv'):
            _name = _file.name
            dbutils.fs.mv(i_directory_temp+'/'+_name,mv_tar)
            dbutils.fs.rm(i_directory_temp,True)

    
    
     

# COMMAND ----------

# # Standardization of the per parcel

input_folder='4000perparcel'
output_folder='4000perparcel'
input_file_type='4000perparcel'

i_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}/"
o_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}/"

print(i_directory)

all_files = dbutils.fs.ls(f"{i_directory}")

for _dir in all_files:
 
    input_directory = i_directory+str(_dir.name)

    print(' The inner dir is '+input_directory)

    target_directory =  o_directory+str(_dir.name)

    print(' The target is '+target_directory)
    df_inputfile = spark.read.option("header",True).schema(ISA_File_Headers[input_file_type]).csv(input_directory)
    print(input_directory)

    df_calculated=df_inputfile.withColumn('ingestion_date',current_date() ) \
                            .withColumnRenamed('closed','raw_closed') \
                            .withColumnRenamed('client_fi','client_fi_raw') \
                            .withColumn("closed",f.to_timestamp(from_unixtime(unix_timestamp('raw_closed',format=ISA_File_Headers[input_file_type][6].metadata['format'])))) \
                            .withColumn("deposit_rule_en", f.split(f.col("deposit_rule"), " / ").getItem(0)).withColumn("deposit_rule_fr_calc", f.trim(f.split(f.col("deposit_rule"), " / ").getItem(1))) \
                            .withColumn("deposit_rule_fr",f.regexp_replace(f.col("deposit_rule_fr_calc"), 'M7', '')) \
                            .withColumn('client_fi',f.regexp_replace(f.col("client_fi_raw"), '-M7', ''))

    # display(df_calculated)
    
    df_calculated.select('ingestion_date','nesid','parcelid','deposit_rule_en','deposit_rule_fr','client_rdp','client_fi','nbdenom','closed').write.format("parquet").mode("overwrite").save(target_directory)  


# COMMAND ----------

#  Raw and break up of deposittohc

input_folder='deposittohc'
output_folder='deposittohc'
input_file_type='deposittohc'

h_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/history/"
i_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}/"
o_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}/"

o_directory_temp = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}_jan_2024_bkp/"

dbutils.fs.mv(i_directory,o_directory_temp,recurse=True)
 
print(i_directory)

 
hist_file = h_directory+'/dhc_hist.csv'
hist_df = spark.read.option("header",True).csv(hist_file)
hist_df = spark.read.option("header",True).schema(ISA_File_Hist[input_folder]).csv(hist_file)

hist_df = hist_df.withColumn('split_closed',f.substring(hist_df.closed,1,10))

hist_df=hist_df.withColumnRenamed('created_date','created_dates' ) \
                     .withColumn("created_date",f.to_timestamp(from_unixtime(unix_timestamp('created_dates',format=ISA_File_Hist[input_file_type][3].metadata['format'])))) \
                     .withColumn('new_created_date',f.date_format(col('created_date'),'MM/dd/yyyy hh:mm:ss aa')) \
                     .withColumnRenamed('nes_shipping_date','nes_shipping_dates' ) \
                     .withColumn("nes_shipping_date",f.to_timestamp(from_unixtime(unix_timestamp('nes_shipping_dates',format=ISA_File_Hist[input_file_type][5].metadata['format'])))) \
                     .withColumn('new_nes_shipping_date',f.date_format(col('nes_shipping_date'),'MM/dd/yyyy hh:mm:ss aa')) \
                     .withColumnRenamed('aoc_received_date','aoc_received_dates' ) \
                     .withColumn("aoc_received_date",f.to_timestamp(from_unixtime(unix_timestamp('aoc_received_dates',format=ISA_File_Hist[input_file_type][6].metadata['format'])))) \
                     .withColumn('new_aoc_received_date',f.date_format(col('aoc_received_date'),'MM/dd/yyyy hh:mm:ss aa')) \
                     .drop('created_dates','created_date','nes_shipping_dates','nes_shipping_date','aoc_received_date','aoc_received_dates')\
                     .withColumnRenamed('new_created_date','created_date') \
                     .withColumnRenamed('new_nes_shipping_date','nes_shipping_date') \
                     .withColumnRenamed('new_aoc_received_date','aoc_received_date') 
                     
                      

# display(hist_df)

list_dates = hist_df.select('split_closed').rdd.flatMap(lambda x:x).distinct().collect()

# list_dates = ['2023-04-18']
 

for tup in range(len(list_dates)):
    print(list_dates[tup])
    i_directory_tar = i_directory+list_dates[tup] 
    i_directory_temp = i_directory+'temp'
    # print(i_directory_tar)
    df_calculatedy = hist_df.select('nesid','pcd','created_date','nes_deposit_date','nes_shipping_date','aoc_received_date','client_rdp','client_fi','cashcentre','closed').filter(col("split_closed") == list_dates[tup] )



    df_calculatedy.coalesce(1).write.format("csv").mode("overwrite").save(i_directory_temp,header=True) 

    mv_src = i_directory_temp+'/part*.csv'
    mv_tar = i_directory_tar+'/'+'DepositToHc_'+list_dates[tup]+'.csv'

    files = dbutils.fs.ls(i_directory_temp)

    for _file in files:
        if _file.name.endswith('.csv'):
            _name = _file.name
            dbutils.fs.mv(i_directory_temp+'/'+_name,mv_tar)
            dbutils.fs.rm(i_directory_temp,True)

    
    
     

# COMMAND ----------

#  Standardization of deposittohc

input_folder='deposittohc'
output_folder='deposittohc'
input_file_type='deposittohc'

i_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}/"
o_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}/"
o_directory_temp = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}_jan_2024_bkp/"

dbutils.fs.mv(o_directory,o_directory_temp,recurse=True)

print(i_directory)

all_files = dbutils.fs.ls(f"{i_directory}")

for _dir in all_files:
 
    input_directory = i_directory+str(_dir.name)

    print(' The inner dir is '+input_directory)

    target_directory =  o_directory+str(_dir.name)

    print(' The target is '+target_directory)
  

    df_inputfile = spark.read.option("header",True).schema(ISA_File_Headers[input_file_type]).csv(input_directory)

    print(input_directory)

    df_calculated=df_inputfile.withColumn('ingestion_date',current_date() ) \
                                        .withColumnRenamed('created_date','created_dates' ) \
                                        .withColumnRenamed('nes_deposit_date','nes_deposit_dates' ) \
                                        .withColumnRenamed('nes_shipping_date','nes_shipping_dates' ) \
                                        .withColumnRenamed('aoc_received_date','aoc_received_dates' ) \
                                        .withColumnRenamed('cashcentre','cash_centre') \
                                        .withColumn("created_date",f.to_timestamp(from_unixtime(unix_timestamp('created_dates',format=ISA_File_Headers[input_file_type][2].metadata['format']))))\
                                        .withColumn("nes_deposit_date", to_date(col("nes_deposit_dates"), ISA_File_Headers[input_file_type][3].metadata['format']))   \
                                        .withColumn("nes_shipping_date",f.to_timestamp(from_unixtime(unix_timestamp('nes_shipping_dates',format=ISA_File_Headers[input_file_type][4].metadata['format']))) )\
                                        .withColumn("aoc_received_date",f.to_timestamp(from_unixtime(unix_timestamp('aoc_received_dates',format=ISA_File_Headers[input_file_type][5].metadata['format'])))) \
                                        .withColumnRenamed('closed','closed_date' ) \
                                        .withColumn("closed",f.to_timestamp(from_unixtime(unix_timestamp('closed_date',format=ISA_File_Headers[input_file_type][9].metadata['format']))))

    # display(df_calculated)
    
    df_calculated.select('ingestion_date','nesid','pcd','created_date','nes_deposit_date','nes_shipping_date','aoc_received_date','client_rdp','client_fi','cash_centre','closed').write.format("parquet").mode("overwrite").save(target_directory) 

# COMMAND ----------

#  Raw and break up of hctoclient

input_folder='hctoclient'
output_folder='hctoclient'
input_file_type='hctoclient'


h_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/history/"
i_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}/"
o_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{output_folder}/"

o_directory_temp = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}_jan_2024_bkp/"

dbutils.fs.mv(i_directory,o_directory_temp,recurse=True)
 
print(i_directory)

 
hist_file = h_directory+'/htc_hist.csv'
hist_df = spark.read.option("header",True).option("encoding", "ISO-8859-1").schema(ISA_File_Hist[input_folder]).csv(hist_file)
hist_df = hist_df.withColumn('split_closed',f.substring(hist_df.closed,1,10))

hist_df=hist_df.withColumnRenamed('created_date','created_dates' ) \
                     .withColumn("created_date",f.to_timestamp(from_unixtime(unix_timestamp('created_dates',format=ISA_File_Hist[input_file_type][3].metadata['format'])))) \
                     .withColumn('new_created_date',f.date_format(col('created_date'),'MM/dd/yyyy hh:mm:ss aa')) \
                     .withColumnRenamed('machine_date','machine_dates' ) \
                     .withColumn("machine_date",f.to_timestamp(from_unixtime(unix_timestamp('machine_dates',format=ISA_File_Hist[input_file_type][4].metadata['format'])))) \
                     .withColumn('new_machine_date',f.date_format(col('machine_date'),'MM/dd/yyyy hh:mm:ss aa')) \
                     .drop('created_dates','created_date','nes_shipping_dates','machine_date','machine_dates')\
                     .withColumnRenamed('new_created_date','created_date') \
                     .withColumnRenamed('new_machine_date','machine_date')\
                     .withColumnRenamed('valuecounting','valuecountings' ) \
                     .withColumn('valuecounting',col("valuecountings").cast("long") ) \
                     .drop('valuecountings')
                     
                      
# display(hist_df.select('denom_name').distinct())

list_dates = hist_df.select('split_closed').rdd.flatMap(lambda x:x).distinct().collect()

# list_dates = ['2023-04-18']
# # list_dates = [ ]

for tup in range(len(list_dates)):
    print(list_dates[tup])
    i_directory_tar = i_directory+list_dates[tup] 
    i_directory_temp = i_directory+'temp'
    # print(i_directory_tar)
    df_calculatedy = hist_df.select('counting_link','pcd','created_date','machine_date','deposit_rule','client_rdp','client_fi','valuecounting','denom','denom_name','nbdenom','nbreject','nbkeyin','count_value','reject_value','prep_value','cashcentre','closed').filter(col("split_closed") == list_dates[tup] )

    # display(df_calculatedy)


    df_calculatedy.coalesce(1).write.format("csv").mode("overwrite").save(i_directory_temp,header=True) 

    mv_src = i_directory_temp+'/part*.csv'
    mv_tar = i_directory_tar+'/'+'HcToClient_'+list_dates[tup]+'.csv'

    files = dbutils.fs.ls(i_directory_temp)

    for _file in files:
        if _file.name.endswith('.csv'):
            _name = _file.name
            dbutils.fs.mv(i_directory_temp+'/'+_name,mv_tar)
            dbutils.fs.rm(i_directory_temp,True)

    
    
     

# COMMAND ----------

#  Standardization of hctoclient

input_folder='hctoclient'
output_folder='hctoclient'
input_file_type='hctoclient'

i_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Base_Location}/{input_folder}/"
o_directory = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}/"

o_directory_temp = f"abfss://{container}@{storage}.dfs.core.windows.net/{ISA_Target_Location}/{output_folder}_jan_2024_bkpee/"

dbutils.fs.mv(o_directory,o_directory_temp,recurse=True) 

print(i_directory)


all_files = dbutils.fs.ls(f"{i_directory}")

for _dir in all_files:
 
    input_directory = i_directory+str(_dir.name)

    print(' The inner dir is '+input_directory)

    target_directory =  o_directory+str(_dir.name)

    print(' The target is '+target_directory)
  

    df_inputfile = spark.read.option("header",True).schema(ISA_File_Headers[input_file_type]).csv(input_directory)

    print(input_directory)
    
    df_calculated1=df_inputfile.withColumn('ingestion_date',current_date() ) \
                                .withColumn('denom_value',  f.when (col("denom").cast("int").isNull(),0 ) .otherwise(col("denom").cast("int"))) \
                                .withColumnRenamed('denom_name','denom_category' ) \
                                .withColumn('invalid_media_type',col("denom").cast("int").isNull()) \
                                .withColumn('denom_type', f.when ( col("denom").cast("int").isNotNull() , substring(substring_index('denom_category','-',-1),2,len('denom_category')) )  
                                            .otherwise(col("denom"))
                                            ) \
                                .withColumnRenamed('created_date','created_dates' ) \
                                .withColumnRenamed('machine_date','machine_dates' ) \
                                .withColumn("created_date",f.to_timestamp(from_unixtime(unix_timestamp('created_dates',format=ISA_File_Headers[input_file_type][2].metadata['format'])))) \
                                .withColumn("machine_date",f.to_timestamp(from_unixtime(unix_timestamp('machine_dates',format=ISA_File_Headers[input_file_type][3].metadata['format'])))) \
                                .withColumnRenamed('cashcentre','cash_centre' ) \
                                .withColumnRenamed('closed','closed_date' ) \
                                .withColumn("closed",f.to_timestamp(from_unixtime(unix_timestamp('closed_date',format=ISA_File_Headers[input_file_type][17].metadata['format']))))\
                                .withColumnRenamed('valuecounting','valuecountings' ) \
                                .withColumn('valuecounting',col("valuecountings").cast("long") ) \
                                .drop('valuecountings')

    df_calculated1 = df_calculated1.withColumn("deposit_rule_en", f.split(f.col("deposit_rule"), " / ").getItem(0)).withColumn("deposit_rule_fr", f.trim(f.split(f.col("deposit_rule"), " / ").getItem(1)))
    df_calculated1 = df_calculated1.withColumn("denom_type_en", f.split(f.col("denom_type"), "/").getItem(0)).withColumn("denom_type_fr", f.trim(f.split(f.col("denom_category"), "/").getItem(1)))
    df_calculated1 = df_calculated1.drop("deposit_rule", "denom_type")
    df_calculated1 = df_calculated1.withColumn('deposit_rule_fr', f.regexp_replace(f.col("deposit_rule_fr"), 'M7', ''))
    df_calculated1 = df_calculated1.withColumn('special_stacker', f.col('denom_category').contains('- SS -'))
    
    df_calculated1.select('ingestion_date','counting_link','pcd','created_date','machine_date','client_rdp','client_fi','valuecounting','denom_category','denom_value','nbdenom','nbreject','nbkeyin','count_value','reject_value','prep_value','cash_centre','denom_type_en', 'denom_type_fr', 'deposit_rule_en', 'deposit_rule_fr','special_stacker','invalid_media_type','closed').write.format("parquet").mode("overwrite").save(target_directory)  

