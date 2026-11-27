# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, LongType
from pyspark.sql.functions import col,from_unixtime,unix_timestamp, current_date, current_timestamp,substring,instr, lit, collect_list,substring_index,to_date,date_format,to_timestamp,split,trim,when
import datetime
import time
from pyspark.sql import functions as f



# COMMAND ----------

RCMP_Base_Location = "raw/external/rcmp"

RCMP_Target_Location= "standardized/internal/inventory_supply_accounting/rcmp"

RCMP_CFT = {
"boc1" : StructType([ \
    StructField("c1",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':0,'end':6}), \
    StructField("volume",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':6,'end': 11}), \
    StructField("c2",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':11,'end': 18}), \
    StructField("bank_denom_am",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':18,'end': 22}), \
    StructField("series_yr",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':22,'end': 26}), \
    StructField("c3",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':26,'end': 27}), \
    StructField("class",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':27,'end': 41}), \
    StructField("c4",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':41,'end': 47}), \
    StructField("city",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':47,'end': 67}), \
    StructField("prov_state_ce",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':67,'end': 69}), \
    StructField("recv_dt",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':69,'end': 77}), \
    StructField("rec_type",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':77,'end': 78}), \
    StructField("recovery_mthd_type",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':78,'end': 79}), \
    StructField("serial_nb",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':79,'end': 95}), \
    StructField("last_fld",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':98,'end': 102})
     
  ]),
  "boc2" : StructType([ \
    StructField("c1",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':0,'end':6}), \
    StructField("volume",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':6,'end': 11}), \
    StructField("c2",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':11,'end': 18}), \
    StructField("bank_denom_am",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':18,'end': 22}), \
    StructField("series_yr",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':22,'end': 26}), \
    StructField("c3",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':26,'end': 27}), \
    StructField("class",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':27,'end': 41}), \
    StructField("c4",StringType(),False, metadata={'include_in_fact':'N','NULL':'N','start':41,'end': 47}), \
    StructField("city",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':47,'end': 67}), \
    StructField("prov_state_ce",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':67,'end': 69}), \
    StructField("recv_dt",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':69,'end': 77}), \
    StructField("rec_type",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':77,'end': 78}), \
    StructField("recovery_mthd_type",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':78,'end': 79}), \
    StructField("serial_nb",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':79,'end': 95}), \
    StructField("last_fld",StringType(),False, metadata={'include_in_fact':'Y','NULL':'N','start':98,'end': 102})
     
  ])
}

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
