# Databricks notebook source
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import col,from_unixtime,unix_timestamp, current_date, current_timestamp,substring,instr, lit, collect_list,substring_index,to_date,date_format,to_timestamp,split,trim,when
import datetime
import time

# COMMAND ----------

BRS_File_Headers = {
"qm11" : StructType([ \
    StructField("Notification",StringType(),False, metadata={'format' : 'N'}), \
    StructField("Required Start",DateType(),True, metadata={'format':'yyyyMMdd'}), \
    StructField("Required End",DateType(),True, metadata={'format' : 'yyyyMMdd'}), \
    StructField("Notification Status",StringType(),False, metadata={'format' : 'N'}), \
    StructField("Description",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Customer",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Created By",StringType(),False, metadata={'format' : 'N'}), \
    StructField("Changed By",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Created On",DateType(),False, metadata={'format' : 'yyyyMMdd'}), \
    StructField("Changed On",DateType(),True, metadata={'format' : 'yyyyMMdd'}), \
    StructField("Notification date",DateType(),True, metadata={'format' : 'yyyyMMdd'}), \
    StructField("Reference date",DateType(),False, metadata={'format' : 'yyyyMMdd'}), \
    StructField("Coding Code",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Claimed Value",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Return delivery qty",IntegerType(),True, metadata={'format' : 'N'}), \
    StructField("Date of Ret Delivery",DateType(),True, metadata={'format' : 'yyyyMMdd'}), \
    StructField("Reference number",DecimalType(15,2),True, metadata={'format' : 'N'}), \
    StructField("Substrate Type",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Coding code text",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Postal Code",StringType(),True, metadata={'format' : 'N'}), \
    StructField("City",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Country Key",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Region",StringType(),True, metadata={'format' : 'N'}), \
    StructField("FI/ACC/CU/POLICE", StringType(), False, metadata={'allowed_values': ['TRUE', 'FALSE']})
  ]),
"qm15" : StructType([ \
    StructField("Item number",StringType(),False, metadata={'format' : 'N'}), \
    StructField("Notification",StringType(),False, metadata={'format' : 'N'}), \
    StructField("Notification date",DateType(),False, metadata={'format':'yyyyMMdd'}), \
    StructField("Customer",StringType(),True, metadata={'format':'N'}), \
    StructField("Required Start",DateType(),True, metadata={'format':'yyyyMMdd'}), \
    StructField("Required End",DateType(),True, metadata={'format':'yyyyMMdd'}), \
    StructField("Defect class", StringType(),True, metadata={'format':'N'}), \
    StructField("Assembly",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Description of assembly",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Defective Qty external", DecimalType(15,3),False, metadata={'format' : 'N'}), \
    StructField("Unit of measure",StringType(),True, metadata={'format' : 'N'}), \
    StructField("Defective Qty internal", DecimalType(15,3), False,metadata={'format' : 'N'})
  ]),
}

from pyspark.sql.types import StructType, StructField, StringType

def flip_to_string_type(schema):
    new_fields = []
    for field in schema:
        # Create a new StructField with StringType for each field
        new_field = StructField(
            field.name,  # Field name remains the same
            StringType(),  # Change the data type to StringType
            field.nullable,  # Preserve the nullable attribute
            field.metadata  # Preserve the metadata
        )
        new_fields.append(new_field)
    
    # Return a new StructType with the updated fields
    return StructType(new_fields)

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
