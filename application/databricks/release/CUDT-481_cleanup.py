# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates tables for isa and m7 standardized data **
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC * Tasks: 
# MAGIC   * create table tbl_isa_reissnotes
# MAGIC   *  
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   * replace - false or true, replace if tables exist

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")

# COMMAND ----------

# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

table_schema = f"{unity_catalog_name}.standardized_db"
print(f"table_schema: {table_schema}")
 

# COMMAND ----------


from pyspark.sql.utils import AnalysisException

def drop_table(table_schema: str, table_name: str) -> None:
    try:
        full_table_name = f"{table_schema}.{table_name}"
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        print(f"Successfully dropped table: {full_table_name}")
    except Exception as e:
        print(f"Failed to drop table: {full_table_name}")

# COMMAND ----------

# DBTITLE 1,isa
drop_table(table_schema, "tbl_isa_4000perparcel")
drop_table(table_schema, "tbl_isa_4ons")
drop_table(table_schema, "tbl_isa_acdptrace")
drop_table(table_schema, "tbl_isa_deposittohc")
drop_table(table_schema, "tbl_isa_emscontainersnapshot")
drop_table(table_schema, "tbl_isa_extracthstbtransfer")
drop_table(table_schema, "tbl_isa_extractreissnotes")
drop_table(table_schema, "tbl_isa_maocreissnotes") # renamed to tbl_isa_extractreissnotes
drop_table(table_schema, "tbl_isa_hctoclient")
drop_table(table_schema, "tbl_isa_incompletedepositinfo")
drop_table(table_schema, "tbl_isa_inventorysnapshot")
drop_table(table_schema, "tbl_isa_preadvisementdata")
drop_table(table_schema, "tbl_isa_reissnotes") # renamed to tbl_isa_extractreissnotes

# COMMAND ----------

# DBTITLE 1,m7
drop_table(table_schema, "tbl_m7_bnlist")
drop_table(table_schema, "tbl_m7_counters")
drop_table(table_schema, "tbl_m7_counts")
drop_table(table_schema, "tbl_m7_rejections")
drop_table(table_schema, "tbl_m7_rejects")
drop_table(table_schema, "tbl_m7_unfits")
