# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType

# COMMAND ----------

# MAGIC %run ops/Utils/auth_util

# COMMAND ----------

dbutils.widgets.text("storage","", "storage")
storage_name = dbutils.widgets.get('storage')

container_name = "restricted"
stan_path = "standardized/internal/aoc_processing/"


CURRENY_SECRET_SCOPE = "lake-kv-currency-scope"
CURRENY_SPN_ID = "spn-currencyservices-id"
CURRENY_SPN_KEY = "spn-currencyservices-key"
LAKE_TENANT_ID = "lake-tenant-id"

# COMMAND ----------

tryEnableSpnAccess(CURRENY_SECRET_SCOPE, storage_name, CURRENY_SPN_ID, CURRENY_SPN_KEY, LAKE_TENANT_ID)

# COMMAND ----------

stand_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{stan_path}"

# COMMAND ----------

df = spark.createDataFrame( [("TEST_ROW_1",), ("TEST_ROW_2",), ("TEST_ROW_3",)], StructType([StructField("TEST_COLUMN_1", StringType(), True)]))

# COMMAND ----------

#test read access
dbutils.fs.ls(stand_path)

# COMMAND ----------

stand_test_path = f"{stand_path}/test"


# test write access
df.write.mode("overwrite").format("parquet").save(stand_test_path)


#if needed remove test folder - could be kept and overwritten every test 
dbutils.fs.rm(stand_test_path,True)
