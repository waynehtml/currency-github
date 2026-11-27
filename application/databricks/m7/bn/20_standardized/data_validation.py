# Databricks notebook source
from pyspark.sql.dataframe import DataFrame
from dbruntime.dbutils import FileInfo
from pyspark.sql import functions as f
import time

# COMMAND ----------

container = dbutils.widgets.text("container","", "container")
storage = dbutils.widgets.text("storage","", "storage")
date = dbutils.widgets.text("date","", "date")

container = dbutils.widgets.get("container")
storage = dbutils.widgets.get("storage")
date = dbutils.widgets.get("date")


site = dbutils.widgets.get("site")
mode = dbutils.widgets.get("mode")

if mode == 'append':
  date_raw = date+'/'+site
else:
  date_raw = date+'/all_sites'

 

# COMMAND ----------


def validate_populated(file: FileInfo, df: DataFrame):


    if df.count() != df.filter(df.TIME.isNotNull()).count():
        raise Exception(file.name, "TIME column failed populated validation")
    
    if df.count() != df.filter(df.MSEC.isNotNull()).count():
        raise Exception(file.name, "MSEC column failed populated validation")

    if df.count() != df.filter(df.MSEC.cast('int').isNotNull()).count() or df.filter(df['MSEC'].contains('.')).count() > 0 :
        raise Exception(file.name, "MSEC column not interger failed populated validation")

    if df.count() == 0:
        raise Exception(file.name, "No Data")


# COMMAND ----------

def validate_date(file: FileInfo, df: DataFrame):

    #remove all date values null to begin with
    original_df = df.filter(df.DATE.isNotNull())


    #convert to date type
    converted_df = df.withColumn("DATE", f.to_date(original_df.DATE, "M/dd/yyyy"))


    #any nulls mean invalid data
    if original_df.count() != converted_df.count():
        raise Exception(file.name, "Date column failed data validation")

# COMMAND ----------

def validate_required_columns(file: FileInfo, df: DataFrame):
 
    if not {"DATE", "TIME", "MSEC","SERIAL_NUMBER"}.issubset(set(df.columns)):
        raise Exception(file.name, "failed required columns validation")
    

# COMMAND ----------

def bn_to_df(file: FileInfo) -> DataFrame:

    m7_csv_df = spark.read.options(delimiter="	", header=True).csv(file.path)
      
                                                                
    #strip encoding from column names
    for col in m7_csv_df.columns:
        m7_csv_df = m7_csv_df.withColumnRenamed(col, col.replace("\x00", "" ).replace(" ", "_" ).replace("��", "" ).replace("#", "NUMBER" ).upper())


    #drop any empty column headers
    m7_csv_df = m7_csv_df.drop("")


    #strip encoding from rows
    for col in m7_csv_df.columns:
        m7_csv_df = m7_csv_df.withColumn(col, f.regexp_replace(col, '\x00', ''))
        #replace empty with null
        m7_csv_df = m7_csv_df.withColumn(col, f.when(f.col(col)== "" , None).otherwise(f.col(col)))


    # m7_csv_df.show()

    return m7_csv_df

# COMMAND ----------

all_files = dbutils.fs.ls(f"abfss://{container}@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/converted/{date_raw}")

# COMMAND ----------

for file in all_files:
    
    m7_csv_df = bn_to_df(file)
    
    validate_required_columns(file, m7_csv_df)
    # Currently we are not validating the ID column as most of it comes by as NULL. The business team would come back to say when to make it mandatory
    validate_populated(file, m7_csv_df)
    validate_date(file, m7_csv_df)

    print(file.name, "passed required columns, date column validation, populated columns validation")
