# Databricks notebook source
# MAGIC %md
# MAGIC # Folder operation utility
# MAGIC This notebook could delete, copy and move folders and sub-folders recursivelly.
# MAGIC
# MAGIC * Parameters:
# MAGIC   * env: "dev|stg|prd"
# MAGIC   * command: "copy|more|delete"
# MAGIC   * container: "raw|restricted"
# MAGIC   * storage: "lakecacnpdstdev01|lakecacprdststg01|lakecacprdstprd01"
# MAGIC   * source: folder path, for example raw/internal/rcmp
# MAGIC   * destination: folder path, for example raw/internal/rcmp_bak
# MAGIC

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ./utils

# COMMAND ----------

dbutils.widgets.removeAll()

# create databricks widgets
dbutils.widgets.dropdown("command", "copy", ["copy", "delete", "move"])
dbutils.widgets.text("env", "")
dbutils.widgets.text("src_container", "restricted")
dbutils.widgets.text("src_storage", "")
dbutils.widgets.text("src_path", "")
dbutils.widgets.text("des_container", "restricted")
dbutils.widgets.text("des_storage", "")
dbutils.widgets.text("des_path", "")

# environment
env = dbutils.widgets.get("env")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# get user input values from widgets/ADF base parameters
cmd = dbutils.widgets.get("command")
src_container = dbutils.widgets.get("src_container")

# source storage
src_storage = dbutils.widgets.get("src_storage")
if len(src_storage.strip()) == 0:
    src_storage = get_lake_storage_account(env)
print(f"src_storage: {src_storage}")

# source path
src_path = dbutils.widgets.get("src_path")

# destination container
des_container = dbutils.widgets.get("des_container")

# destination storage
des_storage = dbutils.widgets.get("des_storage")
if len(des_storage.strip()) == 0:
    des_storage = get_lake_storage_account(env)
print(f"des_storage: {des_storage}")

# destination path
des_path = dbutils.widgets.get("des_path")

# COMMAND ----------

def copy(src, des):
  print(f"Copying {src} to {des}")
  dbutils.fs.cp(src, des, recurse=True)

def delete(src):
  print(f"Deleting folder: {src}")
  dbutils.fs.rm(src, recurse=True)

def move(src, des):
  copy(src, des)
  delete(src)


# COMMAND ----------

# DBTITLE 1,backup the m7 bn data
src = f"abfss://{src_container}@{src_storage}.dfs.core.windows.net/{src_path}"
print(f"src: {src}")
des = f"abfss://{des_container}@{des_storage}.dfs.core.windows.net/{des_path}"
print(f"des: {des}")

if len(cmd) == 0:
  raise Exception("cmd is required")

if cmd.lower() == "copy":
  if len(src_path) == 0:
    raise Exception("Source path is required for copy")
  if len(des_path) == 0:
    raise Exception("Destination path is required for copy")
  copy(src, des)
elif cmd.lower() == "delete":
  if len(src_path) == 0:
    raise Exception("Source path is required for delete")
  delete(src)
elif cmd.lower() == "move":
  if len(src_path) == 0:
    raise Exception("Source path is required for move")
  if len(des_path) == 0:
    raise Exception("Destination path is required for move")
  move(src, des)
else:
  print(f"Unknown command: {cmd}")
