# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

def list_files_recursive(path, pattern=None):
    files = []
    try:
        for item in dbutils.fs.ls(path):
            if item.isDir():
                files.extend(list_files_recursive(item.path, pattern))
            else:
                if pattern is None or item.path.endswith(pattern):
                    files.append(item.name)
    except:
        pass
    return files

# COMMAND ----------

def find_new_files(files, raw_path, raw_container = 'restricted', storage = None):
    
    # Get lake storage if not provided
    if storage is None:
        storage = get_lake_storage_account(get_env())
    print(f"storage: {storage}")

    # split source files
    src_files = files.split("|")
    print(f"src_files: {src_files}")


    # input file path in raw zone
    raw_full_path = f"abfss://{raw_container}@{storage}.dfs.core.windows.net/{raw_path}"
    print(f"raw_full_path: {raw_full_path}")

    # find all files in raw zone
    raw_files = list_files_recursive(raw_full_path)
    print(f"raw_files: {raw_files}" )

    # find new files
    new_files = sorted(list(set(src_files) - set(raw_files)))
    print(f"new_files: {new_files}")

    return new_files
