# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook will perform following deployment tasks for CUDT-409**
# MAGIC - Replace old French words with new words
# MAGIC
# MAGIC ## Parameters
# MAGIC - environment: dev|stg|prd
# MAGIC - update: false|true, only update when it is true, otherwise, only show the row count to be updated.

# COMMAND ----------

# DBTITLE 1,Common
dbutils.widgets.removeAll()

dbutils.widgets.text("environment", "")
dbutils.widgets.dropdown("update", "false", ["true", "false"])

env = dbutils.widgets.get("environment")
print(f"env: {env}")
update = dbutils.widgets.get("update")
print(f"update: {update}")

# COMMAND ----------

# DBTITLE 1,Utility
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

# DBTITLE 1,Replacement definitions
replacements = [
    {
        "index": "1",
        "table": "standardized_db.tbl_isa_hctoclient",
        "column": "denom_category",
        "new": "100$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",  #Make sure the data matches
        "old": "100$ - SHRED/DÉCHIQUETER  PAPER/PAPIER"
    },
    {
        "index": "2",
        "table": "standardized_db.tbl_isa_preadvisementdata",
        "column": "denom_category",
        "new": "Mutilated / Mutilés",
        "old": "Mutilated / Mutil�s"
    },
    {
        "index": "3",
        "table": "standardized_db.tbl_isa_4000perparcel",
        "column": "deposit_rule_fr",
        "new": "En bon état",
        "old": "En bon Ã©tat"
    },
    {
        "index": "4",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "Tray Creation/Création de Bac",
        "old": "Tray Creation/CrÃ©ation de Bac"
    },
    {
        "index": "5",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "1$ - SHRED/DÉCHIQUETER",
        "old": "1$ - SHRED/DÃ‰CHIQUETER"
    },
        {
        "index": "6",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "2$ - SHRED/DÉCHIQUETER",
        "old": "2$ - SHRED/DÃ‰CHIQUETER"
    },
    {
        "index": "7",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "5$ - POLYMER/POLYMÈRE",
        "old": "5$ - POLYMER/POLYMÃˆRE"
    },
    {
        "index": "8",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "5$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",
        "old": "5$ - SHRED/DÃ‰CHIQUETER â€“ PAPER/PAPIER"
    },
    {
        "index": "9",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "5$ - SS - POLYMER/POLYMÈRE",
        "old": "5$ - SS - POLYMER/POLYMÃˆRE"
    },
        {
        "index": "10",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "10$ - POLYMER/POLYMÈRE",
        "old": "10$ - POLYMER/POLYMÃˆRE"
    },
    {
        "index": "11",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "10$ - SHRED/DÉCHIQUETER",
        "old": "10$ - SHRED/DÃ‰CHIQUETER"
    },
    {
        "index": "12",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "10$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",
        "old": "10$ - SHRED/DÃ‰CHIQUETER â€“ PAPER/PAPIER"
    },
    {
        "index": "13",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "10$ - SS - POLYMER/POLYMÈRE",
        "old": "10$ - SS - POLYMER/POLYMÃˆRE"
    },
        {
        "index": "14",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "20$ - POLYMER/POLYMÈRE",
        "old": "20$ - POLYMER/POLYMÃˆRE"
    },
    {
        "index": "15",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "20$ - SHRED/DÉCHIQUETER",
        "old": "20$ - SHRED/DÃ‰CHIQUETER"
    },
    {
        "index": "16",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "20$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",
        "old": "20$ - SHRED/DÃ‰CHIQUETER â€“ PAPER/PAPIER"
    },
    {
        "index": "17",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "20$ - SS - POLYMER/POLYMÈRE",
        "old": "20$ - SS - POLYMER/POLYMÃˆRE"
    },
        {
        "index": "18",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "50$ - POLYMER/POLYMÈRE",
        "old": "50$ - POLYMER/POLYMÃˆRE"
    },
    {
        "index": "19",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "50$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",
        "old": "50$ - SHRED/DÃ‰CHIQUETER â€“ PAPER/PAPIER"
    },
    {
        "index": "20",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "50$ - SS - POLYMER/POLYMÈRE",
        "old": "50$ - SS - POLYMER/POLYMÃˆRE"
    },
    {
        "index": "21",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "100$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",
        "old": "100$ - SHRED/DÃ‰CHIQUETER â€“ PAPER/PAPIER"
    },
        {
        "index": "22",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "100$ - SS - POLYMER/POLYMÈRE",
        "old": "100$ - SS - POLYMER/POLYMÃˆRE"
    },
    {
        "index": "23",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "1000$ - SHRED/DÉCHIQUETER",
        "old": "1000$ - SHRED/DÃ‰CHIQUETER"
    }
]

# COMMAND ----------

replace_column_value(unity_catalog_name, replacements, update)

# COMMAND ----------

replacements2 = [
    {
        "index": "1",
        "table": "standardized_db.tbl_isa_hctoclient",
        "column": "denom_category",
        "new": "5$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",  #Make sure the data matches
        "old": "5$ - SHRED/DÉCHIQUETER  PAPER/PAPIER"
    },
    {
        "index": "2",
        "table": "standardized_db.tbl_isa_hctoclient",
        "column": "denom_category",
        "new": "20$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",
        "old": "20$ - SHRED/DÉCHIQUETER  PAPER/PAPIER"
    },
    {
        "index": "3",
        "table": "standardized_db.tbl_isa_hctoclient",
        "column": "denom_category",
        "new": "50$ - SHRED/DÉCHIQUETER – PAPER/PAPIER",
        "old": "50$ - SHRED/DÉCHIQUETER  PAPER/PAPIER"
    },
    {
        "index": "4",
        "table": "standardized_db.tbl_isa_hctoclient",
        "column": "denom_type_fr",
        "new": "DÉCHIQUETER – PAPER",
        "old": "DÉCHIQUETER  PAPER"
    },
    {
        "index": "5",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_category",
        "new": "100$ - POLYMER/POLYMÈRE",
        "old": "100$ - POLYMER/POLYMÃˆRE"
    },
    {
        "index": "6",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_type",
        "new": "SHRED/DÉCHIQUETER",
        "old": "SHRED/DÃ‰CHIQU"
    },
    {
        "index": "7",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_type",
        "new": "SHRED/DÉCHIQUETER",
        "old": "SHRED/DÉCHIQUE"
    },
    {
        "index": "8",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_type",
        "new": "POLYMER/POLYMÈRE",
        "old": "POLYMER/POLYMÈ"
    },
    {
        "index": "9",
        "table": "standardized_db.tbl_isa_extracthstbtransfer",
        "column": "denom_type",
        "new": "POLYMER/POLYMÈRE",
        "old": "POLYMER/POLYMÃ"
    }
]

# COMMAND ----------

replace_column_value(unity_catalog_name, replacements2, update)
