# Databricks notebook source
# DBTITLE 1,Import required libraries
## import required libraries
from pyspark.sql.functions import col, when, lit, max, min, trim, sum, count

# COMMAND ----------

# DBTITLE 1,Common - utils
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Parameters
# Define notebook specific widgets
dbutils.widgets.text("environment", "")
partition = dbutils.widgets.text("folder","2024-JAN", "folder")
hist_end_date_filter = dbutils.widgets.text("hist_end_date_filter","2023-12-01", "hist_end_date_filter")
dbutils.widgets.dropdown("load_type", "incremental_load", ["initial_load", "incremental_load"])
dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"])

## Get notebook specific parameters
env = dbutils.widgets.get("environment")
print(f"env: {env}")
partition = dbutils.widgets.get("folder")
print(f"partition: {partition}")
hist_end_date_filter = dbutils.widgets.get("hist_end_date_filter")
print(f"hist_end_date_filter: {hist_end_date_filter}")
load_type = dbutils.widgets.get("load_type")
print(f"load_type: {load_type}")
mode = dbutils.widgets.get("mode")
print(f"mode: {mode}")

# COMMAND ----------

# DBTITLE 1,Parameters
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")

# source tables and views
tbl_rcmp_cft_boc_monthly = f"{unity_catalog_name}.standardized_db.tbl_rcmp_cft_boc_monthly"
vw_bois_cft_bank_note_info = f"{unity_catalog_name}.view_db.vw_bois_cft_bank_note_info"
vw_bois_bank_note_dim = f"{unity_catalog_name}.view_db.vw_bois_bank_note_dim"
vw_bois_address_loc_info_dim = f"{unity_catalog_name}.view_db.vw_bois_address_loc_info_dim"

# target table
tbl_cft_rcmp = f"{unity_catalog_name}.curated_db.tbl_cft_rcmp"


# COMMAND ----------

# DBTITLE 1,Read and Transform Data
# ----------------------------------------------------
# 🚦 Load Type Handling
# ----------------------------------------------------
try:
    if load_type == "initial_load":
        print("🚀 Starting *Initial Load*...")

        # ----------------------------------------
        # Load historical + new raw data
        # ----------------------------------------
        cft_rcmp_new = (
            spark.read.format("delta")
            .table(tbl_rcmp_cft_boc_monthly)
            .filter(col("received_dt") > hist_end_date_filter)
        )
        cft_rcmp_hist = spark.read.format("delta").table(vw_bois_cft_bank_note_info)
        bn_hist = spark.read.format("delta").table(vw_bois_bank_note_dim).drop("SECURITY_MTHD_TYPE_CE", "INITL_ISSUE_DT")
        add_hist = spark.read.format("delta").table(vw_bois_address_loc_info_dim)

        # ----------------------------------------
        # Build historical dataset
        # ----------------------------------------
        cft_hist = (
            cft_rcmp_hist.filter(col("received_dt") <= hist_end_date_filter)
            .join(bn_hist, cft_rcmp_hist.BANK_NOTE_ID == bn_hist.BANK_NOTE_ID, "left")
            .join(add_hist, cft_rcmp_hist.ADDRESS_LOC_ID == add_hist.ADDRESS_LOC_ID, "left")
            .withColumn("SERIES_YR", col("SERIES_YR").cast("string"))
            .withColumn("PARTITION", lit("hist").cast("string"))
            .withColumn("CFT_CLASSIFICATION_CE", trim(col("CFT_CLASSIFICATION_CE")))
            .withColumn("bank_note_series_code", lit(None).cast("string"))
            .drop("BANK_NOTE_ID", "ADDRESS_LOC_ID", "LOAD_ID")
        )

        # Rename all columns to lowercase for consistency
        for col_name in cft_hist.columns:
            cft_hist = cft_hist.withColumnRenamed(col_name, col_name.lower())


    elif load_type == "incremental_load":
        print(f"📈 Starting *Incremental Load* for partition `{partition}`...")
        cft_rcmp_new = (
            spark.read.format("delta")
            .table(tbl_rcmp_cft_boc_monthly)
            .filter(col("partition") == partition)
        )

    else:
        raise ValueError(
            f"❌ Invalid load_type '{load_type}'. Must be 'initial_load' or 'incremental_load'."
        )

except Exception as e:
    print(f"💥 ERROR: {e}")
    raise SystemExit(1)

# ----------------------------------------------------
# 🔧 Transformations (common to both load types)
# ----------------------------------------------------
print("🔧 Transforming new RCMP data...")

cft_new = (
    cft_rcmp_new.filter(~col("cft_classification_ce").contains("GENUINE"))
    .withColumn(
        "cft_bank_note_value_am",
        col("cft_bank_note_ct") * col("bank_note_denom_am"),
    )
)

# Aggregate by key attributes
cft_new = (
    cft_new.groupBy(
        "received_dt",
        "cft_classification_ce",
        "cft_recovery_type_ce",
        "serial_nb",
        "series_yr",
        "bank_note_denom_am",
        "issuing_country_ce",
        "country_ce",
        "municipality_nm",
        "prov_or_state_ce",
        "partition",
        "bank_note_series_code"
    )
    .agg(
        sum("cft_bank_note_ct").alias("cft_bank_note_ct"),
        sum("cft_bank_note_value_am").alias("cft_bank_note_value_am"),
    )
)

# Reorder columns to match historical schema
cft_new = cft_new.select(
    "received_dt",
    "cft_classification_ce",
    "cft_recovery_type_ce",
    "serial_nb",
    "cft_bank_note_ct",
    "cft_bank_note_value_am",
    "series_yr",
    "bank_note_denom_am",
    "issuing_country_ce",
    "country_ce",
    "municipality_nm",
    "prov_or_state_ce",
    "partition",
    "bank_note_series_code"
)

# ----------------------------------------------------
# 📊 Row Counts + Final Union
# ----------------------------------------------------
if load_type == "initial_load":
    hist_rows = cft_hist.count()
    new_rows = cft_new.count()
    total_rows = hist_rows + new_rows

    print(f"📜 Historical rows: {hist_rows:,}")
    print(f"🆕 New rows: {new_rows:,}")
    print(f"📊 Total rows to write: {total_rows:,}")

    # Union historical + new
    cft_all = cft_hist.unionByName(cft_new)  # safer than union

elif load_type == "incremental_load":
    new_rows = cft_new.count()
    print(f"🆕 Rows for partition `{partition}`: {new_rows:,}")

# COMMAND ----------

# DBTITLE 1,Write to Curated
if load_type == "initial_load":
    cft_all.write.format("delta").mode("overwrite").saveAsTable(tbl_cft_rcmp)
    print(f"Saved {total_rows} rows to {tbl_cft_rcmp}, made up of {hist_rows} rows from bois historical and {new_rows} from rcmp")
else:
    num_rows = cft_new.count()

    try:    
        stmt = f"delete from {unity_catalog_name}.curated_db.tbl_cft_rcmp where partition = '{partition}'"
        print(stmt)
        spark.sql(stmt)
    except Exception as e:
        print('Do Nothing')

    cft_new.write.format("delta").mode("append").saveAsTable(tbl_cft_rcmp)
    print(f"Appended {num_rows} rows to {tbl_cft_rcmp}")
