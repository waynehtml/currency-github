# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC This notebook will rename columns from convert to covert.
# MAGIC
# MAGIC ## Parameters
# MAGIC - environment: dev|stg|prd

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# DBTITLE 1,Utility
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get environment if not provided
env = dbutils.widgets.get("environment")
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")
internal_unity_catalog_name = get_interal_unity_catalog_name(env)
print(f"internal_unity_catalog_name: {internal_unity_catalog_name}")

# COMMAND ----------

# DBTITLE 1,Update in cur_bois_boisp_cft_bank_note_info_t
table = f"{internal_unity_catalog_name}.cur_bois_db.cur_bois_boisp_cft_bank_note_info_t"
print(f"table: {table}")


sql = f"""
UPDATE {table} SET serial_nb = RTRIM(serial_nb)
"""

print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

table = f"{internal_unity_catalog_name}.cur_bois_db.cur_bois_boisp_cft_bank_note_info_t"
print(f"table: {table}")


sql = f"""
UPDATE {table} SET serial_nb = RTRIM(REGEXP_REPLACE(serial_nb, '\u0000+$', ''));
"""

print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

table = f"{internal_unity_catalog_name}.cur_bois_db.cur_bois_boisp_cft_bank_note_info_t"
print(f"table: {table}")


sql = f"""
UPDATE {table} SET serial_nb = RTRIM(REGEXP_REPLACE(serial_nb, '\u0002+$', ''));
"""

print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Update in tbl_cft_rcmp
table = f"{unity_catalog_name}.curated_db.tbl_cft_rcmp"
print(f"table: {table}")

sql = f"""
UPDATE {table} SET serial_nb = RTRIM(serial_nb)
"""

print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

table = f"{unity_catalog_name}.curated_db.tbl_cft_rcmp"
print(f"table: {table}")

sql = f"""
UPDATE {table}
SET serial_nb = RTRIM(REGEXP_REPLACE(serial_nb, '\u0000+$', ''));
"""

print(f"sql: {sql}")
spark.sql(sql)

# COMMAND ----------

table = f"{unity_catalog_name}.curated_db.tbl_cft_rcmp"
print(f"table: {table}")

sql = f"""
UPDATE {table}
SET serial_nb = RTRIM(REGEXP_REPLACE(serial_nb, '\u0002+$', ''));
"""

print(f"sql: {sql}")
spark.sql(sql)
