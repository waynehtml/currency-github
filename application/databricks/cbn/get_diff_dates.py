# Databricks notebook source
import json
import re
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

def parse_dates(data_string):
    """
    Parse the pipe-separated JSON arrays and extract unique dates from filenames
    """
    # Split by pipe separator
    json_sections = data_string.split('|')
    
    all_dates = set()  # Use set to automatically handle uniqueness
    
    # Process each JSON section
    for section in json_sections:
        try:
            # Parse JSON array
            file_objects = json.loads(section.strip())
            
            # Extract dates from each filename
            for file_obj in file_objects:
                filename = file_obj['name']
                
                # Extract date using regex pattern: YYYYMMDD
                date_match = re.search(r'-(\d{8})\.', filename)
                if date_match:
                    date_str = date_match.group(1)
                    
                    # Convert YYYYMMDD to YYYY-MM-DD format
                    try:
                        date_obj = datetime.strptime(date_str, '%Y%m%d')
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                        all_dates.add(formatted_date)
                    except ValueError:
                        print(f"Invalid date format in filename: {filename}")
                        
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON section: {e}")
    
    # Convert set to sorted list
    sorted_dates = sorted(list(all_dates))
    
    return sorted_dates

# COMMAND ----------

def get_subdirs(base_path):
    try:
        subdirs = [f.name.rstrip('/') for f in dbutils.fs.ls(base_path) if f.isDir()]
    except Exception as e:
        subdirs = []
    return subdirs

# COMMAND ----------

# Create parameter widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("env","")
dbutils.widgets.text("storage","")
dbutils.widgets.text("file_type","ssn")
dbutils.widgets.text("file_names","")

# Get environment if not provided
env = dbutils.widgets.get("env")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
storage = dbutils.widgets.get("storage")
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Check file type if not provided
file_type = dbutils.widgets.get("file_type")
if (len(file_type.strip()) == 0):
    raise Exception("Parameter file_type is required.")
print(f"file_type: {file_type}")

# Get filenames
file_names = dbutils.widgets.get("file_names")
if (len(file_names.strip()) == 0):
    raise Exception("Parameter file_names is required.")
print(f"file_names: {file_names}")

# COMMAND ----------

# DBTITLE 1,Set test data
#file_names = "[{\"name\":\"4700008394-SSN-20220125.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220127.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220201.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220202.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220203.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220209.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220210.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220215.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220217.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220222.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220223.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220225.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220228.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220301.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220302.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220303.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220304.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220308.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220309.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220310.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220314.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220315.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220316.1.XML\",\"type\":\"File\"},{\"name\":\"4700008394-SSN-20220317.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221021.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221025.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221026.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221027.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221101.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221102.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221103.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221108.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221108.2.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221117.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221117.2.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221122.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221123.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221124.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221125.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221128.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221129.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221130.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221201.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221202.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221205.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221206.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221207.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221209.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221212.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221213.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221214.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221215.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221219.1.XML\",\"type\":\"File\"},{\"name\":\"4700008946-SSN-20221220.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220406.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220407.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220411.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220419.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220420.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220421.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220422.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220426.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220427.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220428.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220429.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220504.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220505.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220510.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220511.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220512.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220517.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220518.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220519.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220520.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220531.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220601.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220602.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220608.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220609.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220610.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220615.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220616.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220622.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220627.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220628.1.XML\",\"type\":\"File\"},{\"name\":\"4700008947-SSN-20220629.1.XML\",\"type\":\"File\"}]|[{\"name\":\"4700009433-SSN-20231023.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231025.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231031.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231101.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231102.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231107.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231108.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231110.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231114.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231115.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231116.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231117.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231120.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231121.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231122.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231123.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231124.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231127.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231128.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231129.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231130.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231204.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231205.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231206.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231207.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231212.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231212.2.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231213.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231214.1.XML\",\"type\":\"File\"},{\"name\":\"4700009433-SSN-20231219.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230328.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230329.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230404.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230404.2.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230405.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230414.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230418.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230420.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230421.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230424.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230425.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230426.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230427.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230428.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230501.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230502.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230503.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230504.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230505.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230510.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230524.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230525.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230530.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230531.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230601.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230606.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230607.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230608.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230613.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230614.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230620.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230710.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230710.2.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230718.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230719.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230721.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230724.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230725.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230726.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230727.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230728.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230801.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230802.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230803.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230804.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230808.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230809.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230814.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230815.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230816.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230817.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230822.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230823.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230824.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230829.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230901.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230905.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230906.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230907.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230913.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230915.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230919.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230920.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230922.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230926.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230928.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20230929.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231002.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231003.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231004.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231005.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231006.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231010.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231011.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231012.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231017.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231018.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231019.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231023.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231024.1.XML\",\"type\":\"File\"},{\"name\":\"4700009434-SSN-20231025.1.XML\",\"type\":\"File\"}]|[{\"name\":\"4700010672-SSN-20250507.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250509.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250512.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250514.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250515.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250516.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250520.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250521.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250522.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250523.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250527.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250528.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250529.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250602.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250603.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250610.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250611.1.XML\",\"type\":\"File\"},{\"name\":\"4700010672-SSN-20250612.1.XML\",\"type\":\"File\"}]"

# COMMAND ----------

# DBTITLE 1,Parse and Source Dates
src_date_list = parse_dates(file_names)
print(f"src_date_list: {src_date_list}")

# COMMAND ----------

# DBTITLE 1,Find Raw Dates
# input file path in raw zone
raw_base_path = "raw/external/cbn"
raw_full_path = f"abfss://restricted@{storage}.dfs.core.windows.net/{raw_base_path}/{file_type}"

raw_date_list = get_subdirs(raw_full_path)
print(f"raw_date_list: {raw_date_list}")

# COMMAND ----------

# DBTITLE 1,Source Dates - Raw Dates
diff_list = sorted(list(set(src_date_list) - set(raw_date_list)))
print(f"diff_list: {diff_list}")

# COMMAND ----------

# DBTITLE 1,Return diff Dates
dbutils.notebook.exit(json.dumps(diff_list))
