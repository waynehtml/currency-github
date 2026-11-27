# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook will perform following deployment tasks for CUDT-415**
# MAGIC - In M7 extracts standardize nomenclature for Shift Name, Operation Mode, Sorting Mode, and Stacking Mode.
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
        "index": "1101",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Polymer Strap All",
        "new": "Polymer StrapAll"
        
    },
    {
        "index": "1102",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Poly StrapAll",
        "new": "Polymer StrapAll"
        
    },
    {
        "index": "1103",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Paper Shred All",
        "new": "Paper ShredAll"
    },
    {
        "index": "1104",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Unfit deposit",
        "new": "Unfit Deposit"
    },
    {
        "index": "1105",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Poly ShredAll",
        "new": "Polymer ShredAll"
    },
    {
        "index": "1106",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "BNDS_Sampling",
        "new": "Surplus Deposit"
    },
    {
        "index": "1107",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Polymer strap all",
        "new": "Polymer StrapAll"
    },
    {
        "index": "1108",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Polymer Shred All",
        "new": "Polymer ShredAll"
    },
    {
        "index": "1109",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Paper Strap All",
        "new": "Paper StrapAll"
    },
    {
        "index": "1110",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Surplus",
        "new": "Surplus Deposit"
    },
    {
        "index": "1111",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Unfit",
        "new": "Unfit Deposit"
    },
    {
        "index": "1112",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Paper shred",
        "new": "Paper ShredAll"
    },
    {
        "index": "1113",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "shift_name",
        "old": "Paper shredAll",
        "new": "Paper ShredAll"
    },
    {
        "index": "1201",
        "table": "standardized_db.tbl_m7_bnlist",
        "column": "parametersection_sortmode",
        "old": "BNDS_Sampling",
        "new": "Surplus Deposit"
    },
    {
        "index": "2101",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Paper Shred All",
        "new": "Paper ShredAll"
    },
    {
        "index": "2102",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Poly ShredAll",
        "new": "Polymer ShredAll"
    },
    {
        "index": "2103",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Polymer Strap All",
        "new": "Polymer StrapAll"
    },
    {
        "index": "2104",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Poly StrapAll",
        "new": "Polymer StrapAll"
    },
    {
        "index": "2105",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Unfit deposit",
        "new": "Unfit Deposit"
    },
    {
        "index": "2106",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Polymer Shred All",
        "new": "Polymer ShredAll"
    },
    {
        "index": "2107",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Unfit",
        "new": "Unfit Deposit"
    },
    {
        "index": "2108",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Paper shred",
        "new": "Paper ShredAll"
    },
    {
        "index": "2109",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Paper Strap All",
        "new": "Paper StrapAll"
    },
    {
        "index": "2110",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "Paper shredAll",
        "new": "Paper ShredAll"
    },
    {
        "index": "2111",
        "table": "standardized_db.tbl_m7_counts",
        "column": "shift_name",
        "old": "BNDS_Sampling",
        "new": "Surplus Deposit"
    },
    {
        "index": "3101",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Paper Shred All",
        "new": "Paper ShredAll"
    },
    {
        "index": "3102",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Poly StrapAll",
        "new": "Polymer StrapAll"
    },
    {
        "index": "3103",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Unfit deposit",
        "new": "Unfit Deposit"
    },
    {
        "index": "3104",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "BNDS_Sampling",
        "new": "Surplus Deposit"
    },
    {
        "index": "3105",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Polymer Strap All",
        "new": "Polymer StrapAll"
    },
    {
        "index": "3106",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Paper Strap All",
        "new": "Paper StrapAll"
    },
    {
        "index": "3107",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Unfit",
        "new": "Unfit Deposit"
    },
    {
        "index": "3108",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Paper shred",
        "new": "Paper ShredAll"
    },
    {
        "index": "3109",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Poly ShredAll",
        "new": "Polymer ShredAll"
    },
    {
        "index": "3110",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Polymer Shred All",
        "new": "Polymer ShredAll"
    },
    {
        "index": "3111",
        "table": "standardized_db.tbl_m7_rejects",
        "column": "shift_name",
        "old": "Paper shredAll",
        "new": "Paper ShredAll"
    },
    {
        "index": "4101",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Paper Shred All",
        "new": "Paper ShredAll"
    },
    {
        "index": "4102",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Poly StrapAll",
        "new": "Polymer StrapAll"
    },
    {
        "index": "4103",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Unfit deposit",
        "new": "Unfit Deposit"
    },
    {
        "index": "4104",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Polymer Strap All",
        "new": "Polymer StrapAll"
    },
    {
        "index": "4105",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Paper Strap All",
        "new": "Paper StrapAll"
    },
    {
        "index": "4106",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Poly ShredAll",
        "new": "Polymer ShredAll"
    },
    {
        "index": "4107",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "BNDS_Sampling",
        "new": "Surplus Deposit"
    },
    {
        "index": "4108",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Polymer Shred All",
        "new": "Polymer ShredAll"
    },
    {
        "index": "4109",
        "table": "standardized_db.tbl_m7_unfits",
        "column": "shift_name",
        "old": "Unfit",
        "new": "Unfit Deposit"
    }
]

# COMMAND ----------

replace_column_value(unity_catalog_name, replacements, update)
