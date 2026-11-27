# Databricks notebook source
# DBTITLE 1,Parameters
env = dbutils.widgets.text("env","", "env")
container = dbutils.widgets.text("container","", "container")
storage = dbutils.widgets.text("storage","", "storage")
date = dbutils.widgets.text("date","", "date")
site = dbutils.widgets.text("site","", "site")
mode = dbutils.widgets.text("mode","", "mode")

env = dbutils.widgets.get("env")
print(f"env: {env}")
container = dbutils.widgets.get("container")
print(f"container: {container}")
storage = dbutils.widgets.get("storage")
print(f"storage: {storage}")
date = dbutils.widgets.get("date")
print(f"date: {date}")
site = dbutils.widgets.get("site")
print(f"site: {site}")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

# MAGIC  %run ./m7_functions

# COMMAND ----------

# MAGIC %run ./raw_to_standardized_counters

# COMMAND ----------

# MAGIC %run ./raw_to_standardized_rejections

# COMMAND ----------

# MAGIC %run ./raw_to_standardized_unfit
