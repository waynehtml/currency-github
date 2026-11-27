# Databricks notebook source
def test_run(file_type, file_date, mode = "append"):
  dbutils.notebook.run("./raw_to_standardized", 0, {"file_type": file_type, "file_date": file_date, "mode": mode})

# COMMAND ----------

test_run("ssn","2025-06-03")

# COMMAND ----------

test_run("opp","2025-06-03")

# COMMAND ----------

test_run("dailyprocessing","2025-06-03")

# COMMAND ----------

test_run("bpsreport","2025-06-03")

# COMMAND ----------

test_run("productionstats","2025-06-03")

# COMMAND ----------

test_run("containeraoc","2025-06-03")

# COMMAND ----------

test_run("containerqa","2025-06-03")
