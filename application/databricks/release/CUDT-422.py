# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook provides both runable code for the deployment tasks and deployment instructions to the release manager.**
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * The pipeline needs to be deployed
# MAGIC
# MAGIC   * Steps:
# MAGIC
# MAGIC     Have Lukas copy the full load file to the Staging (or Prod) SFTP server
# MAGIC     run PL_Control_Main_ISA with parameters as follows:
# MAGIC     file: ACDPTrace
# MAGIC     Delta: Depends on the date on the file.  If it is today's date, then 0.  If it is prior to today, enter a number equivalent to the number of days in the past in the full load file name
# MAGIC   
# MAGIC   * Steps:
# MAGIC     Have Lukas copy an incremental file to the Staging (or Prod) SFTP server for DM Testing.  This step is only for Staging.
# MAGIC
