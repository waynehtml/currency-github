# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Connection Information & Packages
# MAGIC

# COMMAND ----------

## import required libraries
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import pyspark.sql.types as t
from datetime import datetime

# Ensure 'date' is a properly formatted string
date = datetime.today().strftime('%Y-%m-%d')  
import pandas as pd
import numpy as np

# COMMAND ----------

# create databricks widgets
env = dbutils.widgets.text("env", "")
land_storage = dbutils.widgets.text("land_storage", "", "land_storage")
file_date = dbutils.widgets.text("date", "", "date")
load_type = dbutils.widgets.dropdown("load_type", "initial_load", ["initial_load", "incremental_load"])
debug = dbutils.widgets.dropdown("debug", "false", ["false", "true"])
mode = dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"])




# get user input values from widgets/ADF base parameters
env = dbutils.widgets.get("env")
print(f"env: {env}")
land_storage = dbutils.widgets.get("land_storage")
print(f"land_storage: {land_storage}")
file_date = dbutils.widgets.get("date")
print(f"file_date: {file_date}")
load_type = dbutils.widgets.get("load_type")
print(f"load_type: {load_type}")
debug = dbutils.widgets.get("debug")
print(f"debug: {debug}")
mode = dbutils.widgets.get("mode")
print(f"mode: {mode}")

# COMMAND ----------

# MAGIC %run /currency/common/utils

# COMMAND ----------

# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get land storage if not provided
if len(land_storage.strip()) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage: {land_storage}")

unity_catalog_name = get_unity_catalog_name(env)
print(f"unity_catalog_name: {unity_catalog_name}")
internal_unity_catalog_name = get_interal_unity_catalog_name(env)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Inventory Snapshot, Preadvisement Data, Incomplete deposit info data From Standardized

# COMMAND ----------

# Inventory snapshot table
inventory_snapshot_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_inventorysnapshot"


# Construct the SQL query
query = f"""
SELECT * 
FROM {unity_catalog_name}.standardized_db.tbl_isa_inventorysnapshot
WHERE file_date = DATE('{file_date}')
"""
# Read data into a DataFrame
inv_snapshot_prod_df = spark.sql(query)

# if no data for the given date, exit the notebook
if inv_snapshot_prod_df.count() == 0:
        dbutils.notebook.exit('no_date')

# display for debugging
inv_snapshot_prod_df.limit(100).display()


# COMMAND ----------

# preadvisement snapshot table
preadvisement_snapshot_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_preadvisementdata"


# Construct the SQL query
query_1 = f"""
SELECT * 
FROM {unity_catalog_name}.standardized_db.tbl_isa_preadvisementdata
"""
# Read data into a DataFrame
preadvisement_prod_df = spark.sql(query_1)

# if no data for the given date, exit the notebook
if preadvisement_prod_df.count() == 0:
        dbutils.notebook.exit('no_date')

# display for debugging
preadvisement_prod_df.limit(100).display()

# COMMAND ----------

# Incompletedepositinfo snapshot table
incompletedepositinfo_snapshot_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_incompletedepositinfo"


# Construct the SQL query
query_2 = f"""
SELECT * 
FROM {unity_catalog_name}.standardized_db.tbl_isa_incompletedepositinfo
WHERE file_date = DATE('{file_date}')
"""

# Read data into a DataFrame
incomplete_deposit_prod_df = spark.sql(query_2)

# if no data for the given date, exit the notebook
if incomplete_deposit_prod_df.count() == 0:
        dbutils.notebook.exit('no_date')

# display for debugging
incomplete_deposit_prod_df.limit(100).display()

# COMMAND ----------

# Incompletedepositinfo datecalc table
incompletedepositinfo_snapshot_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_incompletedepositinfo"


# Construct the SQL query
query_3 = f"""
SELECT * 
FROM {unity_catalog_name}.standardized_db.tbl_isa_incompletedepositinfo
"""

# Read data into a DataFrame
incomplete_deposit_datecalc_df = spark.sql(query_3)

# if no data for the given date, exit the notebook
if incomplete_deposit_datecalc_df.count() == 0:
        dbutils.notebook.exit('no_date')

# COMMAND ----------

# ISA deposittohc datecalc table
isadeposittohc_table_name = f"{unity_catalog_name}.standardized_db.tbl_isa_deposittohc"


# Construct the SQL query
query_4 = f"""
SELECT * 
FROM {unity_catalog_name}.standardized_db.tbl_isa_deposittohc
"""

# Read data into a DataFrame
isa_deposittohc_datecalc_df = spark.sql(query_4)

# if no data for the given date, exit the notebook
if isa_deposittohc_datecalc_df.count() == 0:
        dbutils.notebook.exit('no_date')

# COMMAND ----------

# denom type mapping data
denom_type_data_path = f"abfss://config-currency@{land_storage}.dfs.core.windows.net/inventory/mapping/Denom_type_inventory_mapping.csv"
print(f"denom_type_data_path: {denom_type_data_path}")

# COMMAND ----------


denom_type_mapping_df = (
    spark.read.format("csv")
    .option("header", "true")  # Ensures the first row is treated as headers
    .option("encoding", "UTF-8")  # Specifies UTF-8 encoding
    .load(denom_type_data_path)
)

denom_type_mapping_df.display()


# COMMAND ----------

denom_type_mapping_df.createOrReplaceTempView("temp_mapping")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Temp Views

# COMMAND ----------

# Drop the "pcd" column from incomplete_deposit_prod_df
incomplete_deposit_prod_df = incomplete_deposit_prod_df.drop("pcd")

# Remove duplicate rows and create a temporary view for Incomplete Deposit
incomplete_deposit_prod_distinct_df = incomplete_deposit_prod_df.distinct()
incomplete_deposit_prod_distinct_df.createOrReplaceTempView("Incomplete_Deposit_extract_distinct")

# Remove duplicate rows and create a temporary view for Preadvisement Data
preadvisement_prod_distinct_df = preadvisement_prod_df.distinct()
preadvisement_prod_distinct_df.createOrReplaceTempView("Preadvisement_Data_extract_distinct")

# Create a temporary view for Inv Snapshot without any changes
inv_snapshot_prod_df.createOrReplaceTempView("Inv_Snapshot_extract")

#Create temporary views for date calculations
preadvisement_prod_df.createOrReplaceTempView("Preadvisement_Data_extract_datecalc")
incomplete_deposit_datecalc_df.createOrReplaceTempView("Incomplete_Deposit_extract_datecalc")
isa_deposittohc_datecalc_df.createOrReplaceTempView("ISA_Deposittohc_extract_datecalc")

print("Temporary views created successfully.")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     (SELECT COUNT(DISTINCT sealid) FROM incomplete_deposit_extract_distinct) AS total_sealids,
# MAGIC     (SELECT COUNT(DISTINCT sealid) FROM incomplete_deposit_extract_distinct ide WHERE EXISTS (
# MAGIC         SELECT 1 FROM preadvisement_data_extract_distinct pde WHERE pde.nesid = ide.sealid
# MAGIC     )) AS matched_sealids,
# MAGIC     CASE
# MAGIC         WHEN 
# MAGIC             (SELECT COUNT(DISTINCT sealid) FROM incomplete_deposit_extract_distinct) =
# MAGIC             (SELECT COUNT(DISTINCT sealid) FROM incomplete_deposit_extract_distinct ide WHERE EXISTS (
# MAGIC                 SELECT 1 FROM preadvisement_data_extract_distinct pde WHERE pde.nesid = ide.sealid
# MAGIC             ))
# MAGIC         THEN 'True'
# MAGIC         ELSE 'False'
# MAGIC     END AS sealids_match
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incomplete deposit numbers for UNFIT and FIT 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   snapshot_date,
# MAGIC   ingestion_date,
# MAGIC   cash_centre,
# MAGIC   CASE
# MAGIC     WHEN deposit_rule_en = 'Unfit' THEN 0
# MAGIC     ELSE denom_value
# MAGIC   END AS denom_value,
# MAGIC   SUM(counted_denom_total) AS counted_total
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       DISTINCT snapshot_date,
# MAGIC       ingestion_date,
# MAGIC       sealid,
# MAGIC       cash_centre,
# MAGIC       denom_value,
# MAGIC       client_code,
# MAGIC       denom_category,
# MAGIC       counted_denom_total,
# MAGIC       transaction_id,
# MAGIC       deposit_rule_en
# MAGIC     FROM
# MAGIC       incomplete_deposit_extract_distinct
# MAGIC     WHERE
# MAGIC       deposit_rule_en IN ('Fit', 'Unfit')
# MAGIC       AND (
# MAGIC         deposit_rule_en <> 'Fit'
# MAGIC         OR denom_value <> 0
# MAGIC       )
# MAGIC   ) AS subquery
# MAGIC GROUP BY
# MAGIC   snapshot_date,
# MAGIC   ingestion_date,
# MAGIC   cash_centre,
# MAGIC   CASE
# MAGIC     WHEN deposit_rule_en = 'Unfit' THEN 0
# MAGIC     ELSE denom_value
# MAGIC   END
# MAGIC ORDER BY
# MAGIC   snapshot_date,
# MAGIC   ingestion_date,
# MAGIC   cash_centre,
# MAGIC   denom_value;

# COMMAND ----------

incomplete_df = _sqldf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preadvisement numbers for MAOC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   'MAOC' as cash_centre,
# MAGIC   denom_value,
# MAGIC   case
# MAGIC     when denom_value = 0 then sum(total_declared)
# MAGIC     else 0
# MAGIC   end as unfit_value,
# MAGIC   case
# MAGIC     when sum(declared_denom_value) is null then 0
# MAGIC     else sum(declared_denom_value)
# MAGIC   end as surplus_value,
# MAGIC   unfit_value + surplus_value as declared_value,
# MAGIC   min(nes_deposit_date) as oldest_nes_deposit_date,
# MAGIC   min(aoc_received_date) as oldest_aoc_received_date,
# MAGIC   case
# MAGIC   when denom_value = 0 then '\$1 STC UNFIT Process'
# MAGIC   else CONCAT(denom_category, ' Process') end as denom_category
# MAGIC
# MAGIC from
# MAGIC   (
# MAGIC     SELECT
# MAGIC       distinct nesid,
# MAGIC       denom_value,
# MAGIC       (total_declared),
# MAGIC       (pieces),
# MAGIC       (declared_denom_value),
# MAGIC       (nes_deposit_date),
# MAGIC       date(aoc_received_date),
# MAGIC       p.denom_category
# MAGIC     FROM
# MAGIC       preadvisement_data_extract_distinct p
# MAGIC     where
# MAGIC       p.nesid in (
# MAGIC         SELECT
# MAGIC           distinct sealid
# MAGIC         FROM
# MAGIC           incomplete_deposit_extract_distinct i
# MAGIC         where
# MAGIC           i.cash_centre = 'MAOC'
# MAGIC       )
# MAGIC   )
# MAGIC group by denom_value,denom_category

# COMMAND ----------

maoc_process_data_df = _sqldf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preadvisement numbers for TAOC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   'TAOC' as cash_centre,
# MAGIC   denom_value,
# MAGIC   case
# MAGIC     when denom_value = 0 then sum(total_declared)
# MAGIC     else 0
# MAGIC   end as unfit_value,
# MAGIC   case
# MAGIC     when sum(declared_denom_value) is null then 0
# MAGIC     else sum(declared_denom_value)
# MAGIC   end as surplus_value,
# MAGIC   unfit_value + surplus_value as declared_value,
# MAGIC   min(nes_deposit_date) as oldest_nes_deposit_date,
# MAGIC   min(aoc_received_date) as oldest_aoc_received_date,
# MAGIC   case
# MAGIC   when denom_value = 0 then '\$1 STC UNFIT Process'
# MAGIC   else CONCAT(denom_category, ' Process') end as denom_category
# MAGIC
# MAGIC from
# MAGIC   (
# MAGIC     SELECT
# MAGIC       distinct nesid,
# MAGIC       denom_value,
# MAGIC       (total_declared),
# MAGIC       (pieces),
# MAGIC       (declared_denom_value),
# MAGIC       (nes_deposit_date),
# MAGIC       date(aoc_received_date),
# MAGIC       p.denom_category
# MAGIC     FROM
# MAGIC       Preadvisement_data_extract_distinct p
# MAGIC     where
# MAGIC       p.nesid in (
# MAGIC         SELECT
# MAGIC           distinct sealid
# MAGIC         FROM
# MAGIC           incomplete_deposit_extract_distinct i
# MAGIC         where
# MAGIC           i.cash_centre = 'TAOC'
# MAGIC       )
# MAGIC   )
# MAGIC group by denom_value,denom_category

# COMMAND ----------

taoc_process_data_df = _sqldf

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join Unfit and Fit Inventory data to load into INV SNAPSHOT

# COMMAND ----------

from pyspark.sql.functions import max as max_, lit, when, col
from pyspark.sql.window import Window
# Step 1: Union the maoc_df and taoc_df first
result_df = maoc_process_data_df.union(taoc_process_data_df)

# Perform a outer join on 'cash_centre' and 'denom_value'
result_df = result_df.join(incomplete_df, on=['cash_centre', 'denom_value'], how='outer')

# Apply the CASE-like logic for denom_category when it's NULL
result_df = result_df.withColumn(
    'denom_category',
    when(
        col('denom_category').isNull(), 
        when(col('denom_value') == 0, lit('$1 STC UNFIT Process'))  # If denom_value = 0, set to "$1 STC UNFIT Process"
        .otherwise(concat(col('denom_value').cast('int').cast('string'), lit('$ - SURPLUS Process')))  # Cast denom_value to int to remove decimal, then concatenate
    ).otherwise(col('denom_category'))  # Keep the existing denom_category if it's not NULL
)


# Step 2: Add the additional columns to final_df
final_df = result_df.withColumn('type_value', lit('Open')) \
                   .withColumn('denom_value_from_source', result_df['denom_value']) \
                   .withColumn('denom_type', regexp_replace(result_df['denom_category'], r'^.*-\s*', '')) \
                   .withColumn('denom_type_en', regexp_replace(result_df['denom_category'], r'^.*-\s*', '')) \
                   .withColumn('denom_type_fr', lit(None)) \
                   .withColumn('invalid_media_type', lit(None)) \
                   .withColumn('special_stacker', lit(None))


# Step 3A: Create a window partitioned by cash_centre and denom_value
window_spec = Window.partitionBy('cash_centre', 'denom_value')

# Step 3B: Create flags indicating whether both types exist in the group
final_df = final_df.withColumn(
    'has_surplus',
    max_(when(col('denom_category').contains('SURPLUS Process'), lit(1)).otherwise(lit(0))).over(window_spec)
).withColumn(
    'has_sam_ech',
    max_(when(col('denom_category').contains('SAM/ECH'), lit(1)).otherwise(lit(0))).over(window_spec)
)

# Step 3C: Override counted_total to 0 only for SAM/ECH where both exist
final_df = final_df.withColumn(
    'counted_total',
    when(
        (col('denom_category').contains('SAM/ECH')) &
        (col('has_surplus') == 1) &
        (col('has_sam_ech') == 1),
        lit(0)
    ).otherwise(col('counted_total'))
)

# Step 3D (optional): Drop helper flags
final_df = final_df.drop('has_surplus', 'has_sam_ech')

# Show the final result
final_df.display()


# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

# Select the specified columns and convert NULL values to 0 using coalesce and lit
selected_columns_df = final_df.select(
    'snapshot_date',
    'cash_centre',
    'denom_category',
    # Add denom_count: if denom_value = 0, denom_count = 0; else denom_count = total_value / denom_value
    when(final_df['denom_value'] == 0, lit(0))
    .otherwise((coalesce(final_df['declared_value'], lit(0)) - coalesce(final_df['counted_total'], lit(0))) / final_df['denom_value']).alias('denom_count'),
    'denom_value',
    (coalesce(final_df['declared_value'], lit(0)) - coalesce(final_df['counted_total'], lit(0))).alias('total_value'),  # Calculating total_value as declared_value - counted_total and converting NULL to 0
    'type_value',
    coalesce(final_df['denom_value_from_source'], lit(0)).alias('denom_value_source'),  # Convert NULL to 0 for denom_value_from_source
    'denom_type',
    'denom_type_en',
    'denom_type_fr',
    'invalid_media_type',
    'special_stacker',
    'ingestion_date',
    'oldest_nes_deposit_date',
    'oldest_aoc_received_date'
)


# Display the result
selected_columns_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge inventory snapshot and calculated data

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from inv_snapshot_extract
# MAGIC

# COMMAND ----------

inv_data_df = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC select AOC, denom_value, case when denom_value = 0 then sum(total_declared) else null end as unfit_unopened_value, sum(pieces), sum(declared_denom_value), min(nes_deposit_date) as oldest_nes_deposit_date, min(aoc_received_date) as oldest_aoc_received_date from
# MAGIC (SELECT distinct nesid, denom_value, (total_declared), (pieces), (declared_denom_value), (nes_deposit_date), date(aoc_received_date), case when client_rdp in ('01','02','04','05','07','08') OR denom_type_en = ' SAM' then 'MAOC' else 'TAOC' end as AOC
# MAGIC FROM Preadvisement_Data_extract_datecalc
# MAGIC where total_declared <> 0 and date(created_date) > date('2025-01-16') and Preadvisement_Data_extract_datecalc.nesid NOT IN ('0200297181', '0200297182', '0200319386', '0100332473', '0100332370', '0100333059', '0100331833') and Preadvisement_Data_extract_datecalc.nesid not in (SELECT distinct sealid FROM Incomplete_Deposit_extract_datecalc)  
# MAGIC and Preadvisement_Data_extract_datecalc.nesid not in (SELECT distinct nesid FROM ISA_Deposittohc_extract_datecalc))
# MAGIC group by AOC, denom_value sort by AOC, denom_value;

# COMMAND ----------

pending_dates_df =_sqldf

# COMMAND ----------

from pyspark.sql import functions as F

# Rename columns in pending_dates_df to avoid ambiguity after the join
pending_dates_df_renamed = pending_dates_df.withColumnRenamed(
    'oldest_nes_deposit_date', 'pending_oldest_nes_deposit_date'
).withColumnRenamed(
    'oldest_aoc_received_date', 'pending_oldest_aoc_received_date'
)

# Perform a left join on 'denom_value' and 'cash_centre = AOC' to ensure all 113 rows from inv_data_df are retained
joined_df = inv_data_df.alias('inv').join(
    pending_dates_df_renamed.alias('pending'),
    (inv_data_df['denom_value'] == pending_dates_df_renamed['denom_value']) &
    (inv_data_df['cash_centre'] == pending_dates_df_renamed['AOC']),
    how='left'
)

# Define the condition to apply the new columns only for 'SURPLUS' or '$1 STC Unfit'
replace_condition = F.col('inv.denom_type_en').isin('SURPLUS', '$1 STC Unfit', 'SAM')

# Add the new columns with appropriate values based on the condition
inv_result_df = joined_df.withColumn(
    'oldest_nes_deposit_date',
    F.when(
        replace_condition,
        F.col('pending.pending_oldest_nes_deposit_date')
    ).otherwise(F.lit(None))
).withColumn(
    'oldest_aoc_received_date',
    F.when(
        replace_condition,
        F.col('pending.pending_oldest_aoc_received_date')
    ).otherwise(F.lit(None))
).select(
    'inv.snapshot_date', 'inv.cash_centre', 'inv.denom_category', 'inv.denom_count',
    'inv.denom_value', 'inv.total_value', 'inv.type_value', 'inv.denom_value_source',
    'inv.denom_type', 'inv.denom_type_en', 'inv.denom_type_fr', 'inv.invalid_media_type',
    'inv.special_stacker', 'inv.ingestion_date',
    'oldest_nes_deposit_date', 'oldest_aoc_received_date'
)

# Ensure unique rows and display the final DataFrame with all rows
inv_result_df.distinct().display()


# COMMAND ----------

merged_final_df = selected_columns_df.union(inv_result_df)
merged_final_df.display()

# COMMAND ----------


# Example: Save DataFrame as a table in Unity Catalog
# Assuming df_temp is your DataFrame
#merged_final_df.createOrReplaceTempView("temp_view_table")


# Perform the join with additional conditions for specific denom_categories
merged_final_df = merged_final_df.join(
    denom_type_mapping_df,
    (
        (merged_final_df['denom_type'] == denom_type_mapping_df['inventory_type_code']) |  # Standard join on matching types
        (
            (
                (merged_final_df['denom_category'].contains("SURPLUS Process")) &  # Check if denom_category contains 'SURPLUS Process'
                (denom_type_mapping_df['inventory_type_code'].contains("SURPLUS Process"))  # Check if inventory_type_code contains 'SURPLUS Process'
            ) |
            (
                (merged_final_df['denom_category'].contains("SAM/ECH Process")) &  # Check if denom_category contains 'SAM/ECH Process'
                (denom_type_mapping_df['inventory_type_code'].contains("SAM/ECH Process"))  # Check if inventory_type_code contains 'SAM/ECH Process'
            ) |
            (
                (merged_final_df['denom_category'].contains("Mutilated")) &  # Check if denom_category contains 'Mutilated'
                (denom_type_mapping_df['inventory_type_code'].contains("Mutilated"))  # Check if inventory_type_code contains 'Mutilated'
            ) |
            (
                (merged_final_df['denom_category'].contains("Tray Creation")) &  # Check if denom_category contains 'Tray Creation'
                (denom_type_mapping_df['inventory_type_code'].contains("Tray Creation"))  # Check if inventory_type_code contains 'Tray Creation'
            ) |
            (
                (merged_final_df['denom_category'].contains("POLYMER")) &  # Check if denom_category contains 'POLYMER'
                (denom_type_mapping_df['inventory_type_code'].contains("POLYMER"))  # Check if inventory_type_code contains 'POLYMER'
            ) |
            (
                (merged_final_df['denom_category'].contains("Foreign Currency")) &  # Check if denom_category contains 'Foreign Currency'
                (denom_type_mapping_df['inventory_type_code'].contains("Foreign Currency"))  # Check if inventory_type_code contains 'Foreign Currency'
            )
        )
    ),
    how='inner'  # Adjust this join type as needed (inner, left, right, outer)
)


# Display the result
merged_final_df.display()



# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit

# Step 1: Get the unique non-null values for snapshot_date and ingestion_date
non_null_snapshot_date = merged_final_df.select("snapshot_date").where(col("snapshot_date").isNotNull()).first()["snapshot_date"]
non_null_ingestion_date = merged_final_df.select("ingestion_date").where(col("ingestion_date").isNotNull()).first()["ingestion_date"]

# Step 2: Replace NULLs in snapshot_date and ingestion_date with their unique non-null values using coalesce
if non_null_snapshot_date:
    merged_final_df = merged_final_df.withColumn(
        "snapshot_date",
        coalesce(col("snapshot_date"), lit(non_null_snapshot_date))
    )

if non_null_ingestion_date:
    merged_final_df = merged_final_df.withColumn(
        "ingestion_date",
        coalesce(col("ingestion_date"), lit(non_null_ingestion_date))
    )

# Display the result to verify
merged_final_df.display()


# COMMAND ----------

merged_final_df.createOrReplaceTempView("temp_view")

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from temp_view

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Assume these are set at the top of your notebook
# unity_catalog_name = "your_catalog_name"
# file_date = dbutils.widgets.get("file_date")
# load_type = dbutils.widgets.get("load_type")

table_path = f"{unity_catalog_name}.curated_db.tbl_inventory_daily"

try:
    if load_type == "initial_load":
        # Step 1: Delete existing data for the specific file_date
        delete_query = f"""
        DELETE FROM {table_path}
        WHERE INVENT_SNAPSHOT_DAY_DT = DATE('{file_date}')
        """
        spark.sql(delete_query)

        # Step 2: Insert new data for that file_date from temp_view
        insert_query = f"""
        INSERT INTO {table_path}
        SELECT
            snapshot_date AS INVENT_SNAPSHOT_DAY_DT,
            denom_type AS INVENT_TYPE_CE,
            denom_value AS BANK_NOTE_DENOM_AM,
            cash_centre AS BOC_LOC_ID,
            'ISA' AS SOURCE_CE,
            CAST(total_value AS DECIMAL(38, 2)) AS INVENT_NOTE_VALUE_AM,
            CAST(denom_count AS DECIMAL(38, 2)) AS INVENT_NOTE_QT,
            oldest_nes_deposit_date,
            oldest_aoc_received_date,
            inventory_type_code,
            inventory_type_category,
            inventory_type_subcategory,
            inventory_type_name
        FROM temp_view
        WHERE snapshot_date = DATE('{file_date}')
        """
        spark.sql(insert_query)

        print(f"✅ Data for file_date = {file_date} has been refreshed in {table_path}.")

    else:
        # Incremental load — Append logic
        temp_df = spark.table("temp_view")
        target_schema = spark.table(table_path).schema

        temp_df_aligned = temp_df.select(
            temp_df["snapshot_date"].cast(target_schema["INVENT_SNAPSHOT_DAY_DT"].dataType).alias("INVENT_SNAPSHOT_DAY_DT"),
            temp_df["denom_type"].cast(target_schema["INVENT_TYPE_CE"].dataType).alias("INVENT_TYPE_CE"),
            temp_df["denom_value"].cast(target_schema["BANK_NOTE_DENOM_AM"].dataType).alias("BANK_NOTE_DENOM_AM"),
            temp_df["cash_centre"].cast(target_schema["BOC_LOC_ID"].dataType).alias("BOC_LOC_ID"),
            F.lit("ISA").cast(target_schema["SOURCE_CE"].dataType).alias("SOURCE_CE"),
            temp_df["total_value"].cast(target_schema["INVENT_NOTE_VALUE_AM"].dataType).alias("INVENT_NOTE_VALUE_AM"),
            temp_df["denom_count"].cast(target_schema["INVENT_NOTE_QT"].dataType).alias("INVENT_NOTE_QT"),
            temp_df["oldest_nes_deposit_date"].cast(target_schema["oldest_nes_deposit_date"].dataType).alias("oldest_nes_deposit_date"),
            temp_df["oldest_aoc_received_date"].cast(target_schema["oldest_aoc_received_date"].dataType).alias("oldest_aoc_received_date"),
            temp_df["inventory_type_code"].cast(target_schema["inventory_type_code"].dataType).alias("inventory_type_code"),
            temp_df["inventory_type_category"].cast(target_schema["inventory_type_category"].dataType).alias("inventory_type_category"),
            temp_df["inventory_type_subcategory"].cast(target_schema["inventory_type_subcategory"].dataType).alias("inventory_type_subcategory"),
            temp_df["inventory_type_name"].cast(target_schema["inventory_type_name"].dataType).alias("inventory_type_name")
        )

        temp_df_aligned.write.format("delta").mode("append").saveAsTable(table_path)
        print("✅ Data has been appended for the incremental load.")

except AnalysisException as e:
    print(f"❌ AnalysisException occurred: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")


# COMMAND ----------

# Write your SQL query using the variable
query = f"SELECT * FROM {unity_catalog_name}.curated_db.tbl_inventory_daily"

# Run the query
spark.sql(query).display()

# COMMAND ----------

# Define the return value
result_value = "pass"

# Signal successful completion and return a value
dbutils.notebook.exit(result_value)
