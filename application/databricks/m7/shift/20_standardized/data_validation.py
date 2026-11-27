# Databricks notebook source
import datetime
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


balance_path = "raw/internal/aoc_processing/m7/shift/balance/"+date_raw
hc_path = "raw/internal/aoc_processing/m7/shift/hc/"+date_raw

# COMMAND ----------

# MAGIC %run ./m7_functions

# COMMAND ----------

balance_files = dbutils.fs.ls("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+balance_path)

# COMMAND ----------

try:
    hc_files = dbutils.fs.ls("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+hc_path)
except:
    hc_files = []

# COMMAND ----------

def validate_data_type(xml_element: Element):
    
    #dict where {column_name: column_value}
    row_values = xml_element.attrib
    
    
    for key, value in row_values.items():
        
        
        if key == "startdate":
            try:
                datetime.date.fromisoformat(value)
                
            except ValueError:
                raise Exception(file.name, "input failed Date data type validation. It should be YYYY-MM-DD")
            
            
        elif key == "starttime":
            
            try:
                time.strptime(value, '%H:%M:%S')
                
            except ValueError:
                raise Exception(file.name, "input failed Time data type validation. It should be %H:%M:%S")

# COMMAND ----------

def required_columns_populated(xml_element: Element, tag_name: str):    
    
    #dict where {column_name: column_value}
    row_values = xml_element.attrib
    
    
    for key, value in row_values.items():
    
    
        #check if its a required column and null
        if tag_name == "hcunit" and  key in ["headercardid", "headercardno", "startdate", "starttime", "lastmodify", "denomid", "Exception", "value"] and value == None:
            raise Exception(file.name, "failed column validation.", key, "column must be populated.") 

        if tag_name == "machine" and  key in ['serialnumber','site','softwarerelease', 'versioninfo','type','variant','name', 'installationdate'] and value == None:
            raise Exception(file.name, "failed column validation.", key, "column must be populated.") 

        if tag_name == "shift" and  key in  ['number', 'startdate','starttime','enddate','endtime','name'] and value == None:
            raise Exception(file.name, "failed column validation.", key, "column must be populated.") 
        

# COMMAND ----------

def validate_numeric_range(xml_element: Element):
    
    row_values = xml_element.attrib
    
    for key, value in row_values.items():
         
        
        if key == "dTotalAmount" or key == "value":
            
            num = float(value)
            
            if num < 0:
                raise Exception(file.name, "failed validation in", key, "column. It cannot be negative.")

# COMMAND ----------

def validate_column_headers(xml_element: Element, tag_name: str):    
    
    
    column_names = list(xml_element.attrib.keys())

    print('The tag  '+tag_name)

    print(column_names)
    
    if tag_name == "hcunit" and column_names != ['headercardid', 'headercardno', 'singled', 'startdate', 'starttime', 'millisec', 'lastmodify', 'dTotalAmount', 'rejects']:
        raise Exception(file.name, "is missing HCUNIT columns")
    
        
    # elif tag_name == "counter" and not {'denomid', 'denomname', 'currency', 'value', 'quality', 'issue', 'output', 'number', 'stacker'}.issubset(set(column_names)):
    #     raise Exception(file.name, "is missing COUNTER columns")
            

    if tag_name == "machine" and column_names != ['serialnumber','site','softwarerelease', 'versioninfo','type','variant','name', 'installationdate']:
        raise Exception(file.name, "is missing Machine columns")
    
    if tag_name == "shift" and column_names != ['number', 'startdate','starttime','enddate','endtime','name']:
        raise Exception(file.name, "is missing Shift columns")



# COMMAND ----------

for file in hc_files + balance_files:
    
    
    xml_root = get_xml_root(file) 
 
             
    
    for machine in xml_root.iter('MACHINE'):
        validate_column_headers(machine, "machine")
        required_columns_populated(machine, "machine")
        validate_data_type(machine)
        validate_numeric_range(machine)

        for shift in machine.findall("SHIFT"):
            validate_column_headers(shift, "shift")
            required_columns_populated(shift, "shift")
            validate_data_type(shift)
            validate_numeric_range(shift)

            for hcunit in machine.find("SHIFT").findall('HCUNIT'):
                validate_column_headers(hcunit, "hcunit")
                required_columns_populated(hcunit, "hcunit")
                validate_data_type(hcunit)
                validate_numeric_range(hcunit)

    print(file.name, "passed data validation")
