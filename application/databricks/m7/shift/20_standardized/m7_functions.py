# Databricks notebook source
import json
from pyspark.sql import functions as f
from pyspark.sql.types import *
import datetime
import pyspark.sql.types as t
import zipfile
import io
import pandas as pd
import openpyxl
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
from dbruntime.dbutils import FileInfo
from pyspark.sql import Row
import re
from pyspark.sql import Window

# COMMAND ----------

def get_xml_root(file: FileInfo) -> Element:
    
    #get ShiftBalance file
    shift_df = spark.read.format("binaryFile").load(file.path)
    shift_df = io.BytesIO(shift_df.first().content)
    shift_xml = ET.parse(shift_df)
    shift_xml_root = shift_xml.getroot()
    
    return shift_xml_root

# COMMAND ----------

def parse_shift_balance(shift_xml: Element) -> dict:
    
    
    shift_tag_attributes = {"SHIFT_NUMBER": None, "SHIFT_START_DATE": None, "SHIFT_START_TIME": None, "SHIFT_END_DATE": None, "SHIFT_END_TIME": None, "SHIFT_NAME": None}
    

    shift_data = shift_xml.find("MACHINE").find("SHIFT").attrib
    
    
    shift_tag_attributes["SHIFT_NUMBER"] = shift_data["number"]
    shift_tag_attributes["SHIFT_START_DATE"] = shift_data["startdate"]
    shift_tag_attributes["SHIFT_START_TIME"] = shift_data["starttime"]
    shift_tag_attributes["SHIFT_END_DATE"] = shift_data["enddate"]
    shift_tag_attributes["SHIFT_END_TIME"] = shift_data["endtime"]
    shift_tag_attributes["SHIFT_NAME"] = shift_data["name"]
    
    
    
    return shift_tag_attributes    

# COMMAND ----------

def get_machine_tag(shift_xml_root: Element) -> dict:
    
    machine_tag_data = shift_xml_root.find("MACHINE").attrib
        
    machine_tag_data = {"MACHINE_SERIAL_NUMBER": machine_tag_data.pop("serialnumber"), "MACHINE_VERSION_INFO": machine_tag_data.pop("versioninfo"), "MACHINE_VARIANT":  machine_tag_data.pop("variant")}
    
    return machine_tag_data

# COMMAND ----------

def get_header_card_data(hcunit: Element, rejects: bool=None) -> Element:
    
    header_card_data = hcunit.attrib
    
    header_card_data.pop("headercardno")
    
    if not rejects:
        header_card_data.pop("rejects")
    
    header_card_data["HEADERCARD_START_DATE"] = header_card_data.pop('startdate')
    header_card_data["HEADERCARD_START_TIME"] = header_card_data.pop('starttime')
    header_card_data["HEADERCARD_LAST_MODIFY"] = header_card_data.pop('lastmodify')
    
 
    
    return header_card_data
