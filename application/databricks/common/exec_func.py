# Databricks notebook source
# DBTITLE 1,Widgets
# Create parameter widgets
dbutils.widgets.text("name","")
dbutils.widgets.text("args","")

# Get widget values
func_name = dbutils.widgets.get("name")
if len(func_name) == 0:
  raise Exception("name is required")
print(f"func_name: {func_name}")

func_args = dbutils.widgets.get("args")
if len(func_args) == 0:
  func_args = "{}"
print(f"func_args: {func_args}")

# COMMAND ----------

# MAGIC %md
# MAGIC # extract_date
# MAGIC Test case
# MAGIC > - name: extract_date
# MAGIC > - args: {"str": "2023-01-01 - $5 2023 Press Side Data 424064.xlsx","regex": "^(.{10})","iformat":"%Y-%m-%d","oformat": "%Y-%m-%d"}

# COMMAND ----------

# MAGIC %run ./func/extract_date

# COMMAND ----------

# MAGIC %md
# MAGIC # find_new_files
# MAGIC Test case
# MAGIC > - name: find_new_files
# MAGIC > - args: {"files": "2023-01-01 - $5 2023 Press Side Data 424064.xlsx|2022-01-01 - $100 2022 Press Side Data 418297.xlsx","raw_container": "restricted", "raw_path": "raw/external/cbn/press"}

# COMMAND ----------

# MAGIC %run ./func/find_new_files

# COMMAND ----------

# DBTITLE 1,Main
# check if function exists
if func_name not in globals():
        raise Exception(f"Function '{func_name}' not found")

# Convert to ojbect
params = json.loads(func_args)

# Call function with the remaining parameters
ret = globals()[func_name](**params)
#ret = function_map[func_name](**params)

# Return result as JSON string
dbutils.notebook.exit(json.dumps(ret))
