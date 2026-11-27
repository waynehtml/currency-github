# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, LongType
from pyspark.sql.functions import col,from_unixtime,unix_timestamp, current_date, current_timestamp,substring,instr, lit, collect_list,substring_index,to_date,date_format,to_timestamp,split,trim,when
import datetime
import time
from delta.tables import *
from datetime import datetime, timedelta

# COMMAND ----------

ISA_Base_Location = "raw/internal/inventory_supply_accounting"

ISA_Target_Location= "standardized/internal/inventory_supply_accounting"



ISA_File_Headers = {
"4ons" : StructType([ \
    StructField("seal_id",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("denom",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("cash_centre", StringType(), metadata={'NULL':'N','format' : 'N'})
  ]),
"deposittohc" : StructType([ \
    StructField("nesid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("pcd",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("nes_deposit_date",DateType(),False, metadata={'NULL':'Y','format':'YYYY-MM-DD'}), \
    StructField("nes_shipping_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("aoc_received_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("client_rdp",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_fi",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("cashcentre", StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("closed", StringType(), False,metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'})
  ]),
"extracthstbtransfer" : StructType([ \
    StructField("denom",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbdenommachine",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbdenomtransfer",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbdenompcd",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbdenomreject",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbdenomsubtract",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nbcorrected",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("hstb_transfer",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("pcd_name", StringType(), False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("status",IntegerType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("rejnotes", IntegerType(), False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("hspdevname",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("creation", StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("lastupdate",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("luserupdate", StringType(), False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("seqnumber", IntegerType(), False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("lhstbtransfer", StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("cashcentre", StringType(), False, metadata={'NULL':'N','format' : 'N'})
  ]),
"hctoclient" : StructType([ \
    StructField("counting_link",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("pcd",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("machine_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
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
    StructField("closed", StringType(), False,metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'})
  ]),
"maocreissnotes" : StructType([ \
    StructField("bd_shipment_id",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("bd_rdp_id",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("bd_shipping_date",StringType(),False, metadata={'NULL':'N','format':'yyyy/MM/dd'}), \
    StructField("bd_denom",IntegerType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("bd_shipping_aoc",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("bd_4on_id",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("bd_num_notes", StringType(), False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("ivrname", StringType(), False, metadata={'NULL':'N','format' : 'N'})  
  ]),
"4000perparcel" : StructType([ \
    StructField("nesid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("parcelid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("deposit_rule",StringType(),False, metadata={'NULL':'N','format':'N'}), \
    StructField("nbdenom",IntegerType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_rdp",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("client_fi",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("closed", StringType(), False,metadata={'NULL':'N','format':'yyyy-MM-dd HH:mm:ss'})
  ]),
"inventorysnapshot" : StructType([ \
    StructField("snapshot_date",StringType(),False, metadata={'NULL':'N','format' : 'yyyy-MM-dd'}), \
    StructField("cash_centre_name",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("denom_name",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("denom_count",StringType(),False, metadata={'NULL':'N','format':'N'}), \
    StructField("denom_value",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("total_value",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("type_value",StringType(),False, metadata={'NULL':'N','format' : 'N'})
  ]),
 "preadvisementdata" : StructType([ \
    StructField("nesid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("nes_deposit_date",DateType(),False, metadata={'NULL':'Y','format':'yyyy-mm-dd'}), \
    StructField("nes_shipping_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("aoc_received_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("client_code",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("total_declared",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("denom",StringType(),False, metadata={'NULL':'Y','format':'N'}), \
    StructField("pieces",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("declared_denom_value",StringType(),False, metadata={'NULL':'Y','format' : 'N'}) 
  ]),
  "incompletedepositinfo" : StructType([ \
    StructField("snapshot_date",StringType(),False, metadata={'NULL':'N','format' : 'yyyy-MM-dd'}), \
    StructField("sealid",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("pcd",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("client_code",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("cashcentre",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("actual_counted_value",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("transaction_id",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("deposit_rule",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("complete_flag",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("nes_shipping_date",StringType(),False, metadata={'NULL':'Y','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format':'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("denom_name",StringType(),False, metadata={'NULL':'Y','format':'N'}), \
    StructField("denom_value",StringType(),False, metadata={'NULL':'Y','format':'N'}), \
    StructField("counted_denom_pieces",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("counted_denom_total",StringType(),False, metadata={'NULL':'Y','format' : 'N'})
  ]),
  "emscontainersnapshot" : StructType([ \
    StructField("snapshot_date",StringType(),False, metadata={'NULL':'N','format' : 'yyyy-MM-dd'}), \
    StructField("cash_centre",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_container_id",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_seal_id",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("isa_container_id",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("isa_seal_id",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("container_type",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("container_code",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("isa_location",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_positioncode",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_position",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_vaultlocation",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_invaultflag",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_roompositionid",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("ems_locationid",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}) 
  ]),
  "acdptrace" : StructType([ \
    StructField("created_date",StringType(),False, metadata={'NULL':'N','format' : 'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("current_scan",StringType(),False, metadata={'NULL':'N','format' : 'MM/dd/yyyy hh:mm:ss aa'}), \
    StructField("user",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("seqnumber",IntegerType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("headercard",StringType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("counting_link",StringType(),False, metadata={'NULL':'Y','format' : 'N'}), \
    StructField("counting_state",IntegerType(),False, metadata={'NULL':'N','format' : 'N'}), \
    StructField("machine_scan",StringType(),False, metadata={'NULL':'Y','format' : 'MM/dd/yyyy hh:mm:ss aa'})
  ])

}

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
