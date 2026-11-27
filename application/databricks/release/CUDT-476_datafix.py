# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * Deploy CUD-476 to create the delta table
# MAGIC * Steps:
# MAGIC   * run this notebook with the parameter for the environment.
# MAGIC   *  
# MAGIC     * 
# MAGIC * Tasks: 
# MAGIC   * insert data from the parquet to the standardized table  tbl_isa_reissnotes   
# MAGIC   *  
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   *  
# MAGIC   *  
# MAGIC   *  
# MAGIC * [Other]
# MAGIC   * anything that needs to be mentioned to the release manager

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run /currency/common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")


# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
 

# COMMAND ----------

# DBTITLE 1,Create Curated Table
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_reissnotes"
 
print(table_name)

# COMMAND ----------

from delta.tables import *
from pyspark.sql import functions as F

dt = delta.DeltaTable.forName(spark,table_name)

dt.delete()

# COMMAND ----------

storage = get_lake_storage_account(environment)
path = f'abfss://restricted@{storage}.dfs.core.windows.net/standardized/internal/inventory_supply_accounting/extractreissnotes/*/'
print(path)

df = spark.read.format("parquet").load(path)

df.write.format('delta').mode('append').partitionBy('ingestion_date').saveAsTable(table_name)


# COMMAND ----------

display(spark.table(table_name).count())

