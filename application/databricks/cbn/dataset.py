# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, LongType, ArrayType
from pyspark.sql.functions import col,from_unixtime,unix_timestamp, current_date, current_timestamp,substring,instr, lit, collect_list,substring_index,to_date,date_format,to_timestamp,split,trim,when, explode_outer, explode
import datetime
import time
from delta.tables import *
from datetime import datetime, timedelta

# COMMAND ----------

# Dataset Schema Definition

# COMMAND ----------

dataset = {
    "ssn": {
        "format" : "xml",
        "schema": StructType([
                    StructField("processing_date", DateType(), True),
                    StructField("processing_job", LongType(), True),
                    StructField("source", StructType([
                        StructField("RDP", StringType(), True, metadata={"name": "source_rdp"}),
                        StructField("RDC", StringType(), True, metadata={"name": "source_rdc"})
                    ]), True),
                    StructField("batch", ArrayType(
                        StructType([
                            StructField("batch_id", StringType(), True),
                            StructField("batch_quantity", LongType(), True),
                            StructField("sku", StringType(), True),
                            StructField("year", IntegerType(), True),
                            StructField("serial_number_range_low", StringType(), True),
                            StructField("serial_number_range_high", StringType(), True)
                        ])
                    ), True)
                ]),
        "rootTag": "ns2:sheet_shipment",
        "rowTag": "ns2:sheet_shipment"
    },
    "opp": {
        "format" : "xml",
        "schema": StructType([
                    StructField("containerization_date", DateType(), True),
                    StructField("processing_job", LongType(), True), 
                    StructField("sku", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("container_id", IntegerType(), True), 
                    StructField("source", StructType([
                        StructField("RDP", StringType(), True, metadata={"name": "source_rdp"}),
                        StructField("RDC", StringType(), True, metadata={"name": "source_rdc"})
                    ]), True),
                    StructField("four_on", ArrayType(
                        StructType([
                            StructField("four_on_id", LongType(), True),
                            StructField("bps_id", IntegerType(), True),
                            StructField("bank_notes", ArrayType(
                                StructType([
                                    StructField("batch_id", StringType(), True),
                                    StructField("processing_date", DateType(), True),
                                    StructField("lot", StructType([
                                        StructField("lot_number", StringType(), True), 
                                        StructField("serial_number", ArrayType(StringType()), True)
                                    ]), True)
                                ])
                            ), True)
                        ])
                    ), True)
                ]),
        "rootTag": "ns2:opp",
        "rowTag": "ns2:opp"
    },
    "dailyprocessing": {
        "format" : "xml",
        "schema": StructType([
                    StructField("processing_date", DateType(), True),
                    StructField("processing_job", LongType(), True),
                    StructField("sku", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("source", StructType([
                        StructField("RDP", StringType(), True, metadata={"name": "source_rdp"}),
                        StructField("RDC", StringType(), True, metadata={"name": "source_rdc"})
                    ]), True),
                    StructField("batch", StructType([
                        StructField("batch_id", StringType(), True),
                        StructField("batch_details", ArrayType(StructType([
                            StructField("status", StringType(), True),
                            StructField("note_count", LongType(), True)
                        ])), True),
                        StructField("processed_notes", LongType(), True)
                    ]), True)
                ]),
        "rootTag": "ns2:daily_shift",
        "rowTag": "ns2:daily_shift"
    },
    "bpsreport": {
        "format" : "xml",
        "schema": StructType([
                    StructField("processing_job", LongType(), True),
                    StructField("sku", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("batch", StructType([
                        StructField("batch_id", StringType(), True),
                        StructField("processing_date", DateType(), True),
                        StructField("note_details", ArrayType(StructType([
                            StructField("status", StringType(), True),
                            StructField("count", LongType(), True)
                        ])), True)
                    ]), True)
                ]),
        "rootTag": "ns2:bps_report",
        "rowTag": "ns2:bps_report"
    },
    "productionstats": {
        "format" : "xml",
        "schema": StructType([
                    StructField("processing_job", LongType(), True),
                    StructField("sku", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("batch", StructType([
                        StructField("batch_id", StringType(), True),
                        StructField("error_zone", ArrayType(StructType([
                            StructField("error_zone_id", StringType(), True),
                            StructField("count", LongType(), True)
                        ])), True)
                    ]), True)
                ]),
        "rootTag": "ns2:production_stats",
        "rowTag": "ns2:production_stats"
    },
    "zonecategorization": {
        "format" : "xml",
        "schema": StructType([
                    StructField("processing_job", LongType(), True),
                    StructField("sku", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("categories_by_order", StructType([
                        StructField("category_zone", ArrayType(StructType([
                            StructField("category", StringType(), True),
                            StructField("error_zones", StructType([
                                StructField("error_zone_id", ArrayType(StringType(), True), True)
                            ]), True)
                        ])), True)
                    ]), True)
                ]),
        "rootTag": "ns2:categories",
        "rowTag": "ns2:categories"
    },
    "containeraoc": {
        "format" : "xml",
        "schema": StructType([
                    StructField("containerization_date", DateType(), True),
                    StructField("processing_job", LongType(), True),
                    StructField("sku", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("seal_number", IntegerType(), True),
                    StructField("container_id", IntegerType(), True),
                    StructField("quantity", LongType(), True),
                    StructField("source", StructType([
                        StructField("RDP", StringType(), True, metadata={"name": "source_rdp"}),
                        StructField("RDC", StringType(), True, metadata={"name": "source_rdc"})
                    ]), True),
                    StructField("target", StructType([
                        StructField("RDP", StringType(), True, metadata={"name": "target_rdp"}),
                        StructField("RDC", StringType(), True, metadata={"name": "target_rdc"})
                    ]), True),
                    # 'batch' is a repeating element, so it should be an ArrayType of StructType
                    StructField("batch", ArrayType(StructType([
                        StructField("batch_id", StringType(), True),
                        StructField("batch_quantity", LongType(), True)
                    ])), True)
                ]),
        "rootTag": "ns2:container",
        "rowTag": "ns2:container"
    },
    "containerqa": {
        "format" : "xml",
        "schema": StructType([
                    StructField("containerization_date", DateType(), True),
                    StructField("processing_date", DateType(), True),
                    StructField("processing_job", LongType(), True),
                    StructField("sku", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("source", StructType([
                        StructField("RDP", StringType(), True, metadata={"name": "source_rdp"}),
                        StructField("RDC", StringType(), True, metadata={"name": "source_rdc"})
                    ]), True),
                    StructField("target", StructType([
                        StructField("RDP", StringType(), True, metadata={"name": "target_rdp"}),
                        StructField("RDC", StringType(), True, metadata={"name": "target_rdc"})
                    ]), True),
                    StructField("parcel", StructType([
                        StructField("batch_id", StringType(), True),
                        StructField("bps_id", IntegerType(), True),
                        StructField("bag", ArrayType(StructType([
                            StructField("status", StringType(), True),
                            StructField("bag_quantity", LongType(), True),
                            StructField("serial_number", ArrayType(StringType(), True), True)
                        ])), True)
                    ]), True)
                ]),
        "rootTag": "ns2:container_qa",
        "rowTag": "ns2:container_qa"
    }
}

# COMMAND ----------

# Dataset Business Transformation
