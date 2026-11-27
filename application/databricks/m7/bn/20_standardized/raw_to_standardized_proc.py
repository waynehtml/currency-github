# Databricks notebook source
# DBTITLE 1,import statements
import json
from pyspark.sql.types import *
import pyspark.sql.dataframe as d
import pyspark.sql.functions as f
import pandas as pd
import re

# COMMAND ----------

# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,parameter
# Create widgets
env = dbutils.widgets.text("env", "", "env")
storage = dbutils.widgets.text("storage", "", "storage")
land_storage = dbutils.widgets.text("land_storage", "", "land_storage")
date = dbutils.widgets.text("date", "", "date")
site = dbutils.widgets.text("site", "", "site")

# Get widget paramter
env = dbutils.widgets.get("env")
print(f"env: {env}")
storage = dbutils.widgets.get("storage")
print(f"storage: {storage}")
land_storage = dbutils.widgets.get("land_storage")
print(f"land_storage: {land_storage}")
date = dbutils.widgets.get("date")
print(f"date: {date}")
site = dbutils.widgets.get("site")
print(f"site: {site}")

# COMMAND ----------

# DBTITLE 1,Process parameters
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
if len(storage.strip()) == 0:
    storage = get_lake_storage_account(env)
print(f"storage: {storage}")

# Get land storage if not provided
if len(land_storage.strip()) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage: {land_storage}")

# Hardcoded file name
bn_schema_filename = "bn_schema.json"
bn_schema_path = f"abfss://config-currency@{land_storage}.dfs.core.windows.net/m7/bn/{bn_schema_filename}"
print(f"bn_schema_path: {bn_schema_path}")

## Input path
# path to raw unconverted data
raw_u_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/unconverted"
print(f"raw_u_bnlist_path: {raw_u_bnlist_path}")

# path to raw converted data
raw_c_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/internal/aoc_processing/m7/bn/converted"
print(f"raw_c_bnlist_path: {raw_c_bnlist_path}")

## Output path
# path to standardized data 
sdz_bnlist_path = f"abfss://restricted@{storage}.dfs.core.windows.net/standardized/internal/aoc_processing/m7/bn/list"
print(f"sdz_bnlist_path: {sdz_bnlist_path}")

# target table name
unity_catalog_name = get_unity_catalog_name(env)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_bnlist"

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PROCESSING

# COMMAND ----------

# DBTITLE 1,standardization functions
# remove extra/non-printable characters from column header
def clean_column_header(col_name):
    new_name = ''.join(ch for ch in col_name if ch.isprintable()).replace(" ", "_").replace("��", "").replace("#", "number").strip().lower()
    return f"`{col_name}` AS `{new_name}`"

# ensure the column list matches the schema exactly
def apply_schema_column_filter(csv_df: d.DataFrame, bn_schema_no_xml: dict) -> d.DataFrame:
    # retrieve field names from the bn schema
    schema_columns = bn_schema_no_xml.keys()
    
    columns_to_select = []

    for col in schema_columns:
        if col not in csv_df.columns:
            # if schema contains columns that the dataframe does not, add a new column with null values
            csv_df = csv_df.withColumn(col, f.lit(None))
    
        # always append the column to the list to select later
        columns_to_select.append(col)

    if columns_to_select:
        return csv_df.selectExpr(*columns_to_select)
   
    # if no columns match, return the original DataFrame
    return csv_df

# cast each column to the expected type according to the schema
def apply_bn_schema_datatypes(csv_df: d.DataFrame, bn_schema_no_xml: dict) -> d.DataFrame:
    # list of expressions for each column
    exprs = []

    for col in csv_df.columns:
        # get the column's expected type from the schema
        column_type = bn_schema_no_xml.get(col)

        # handle INTEGER casting
        if column_type == "int":
            # attempt to cast the column to Integer and replace non-digits using regex but allowing a negative sign only if it appears at the beginning
            exprs.append(
                f"CASE WHEN {col} IS NOT NULL THEN CAST(TRIM(REGEXP_REPLACE({col}, '[^0-9\\-]', '')) AS INT) ELSE NULL END AS {col}"
            )
       
        # handle STRING casting
        elif column_type == "string" or column_type is None:
            exprs.append(f"CAST({col} AS STRING) AS {col}")

        # handle DATE casting with multiple formats
        elif column_type == "date":
            # try multiple formats using to_timestamp, then cast to DATE
            exprs.append(f"""
                CASE
                    WHEN to_timestamp({col}, 'M/d/yyyy') IS NOT NULL THEN CAST(to_timestamp({col}, 'M/d/yyyy') AS DATE)
                    WHEN to_timestamp({col}, 'MM/d/yyyy') IS NOT NULL THEN CAST(to_timestamp({col}, 'MM/d/yyyy') AS DATE)
                    WHEN to_timestamp({col}, 'M/dd/yyyy') IS NOT NULL THEN CAST(to_timestamp({col}, 'M/dd/yyyy') AS DATE)
                    ELSE CAST(to_timestamp({col}, 'MM/dd/yyyy') AS DATE)
                END AS {col}
            """)

    # apply all transformations in a single selectExpr call
    return csv_df.selectExpr(*exprs)

# COMMAND ----------

# DBTITLE 1,Load bn list schema defined in json config
# Read json config into a python dictionary
bn_schema_df = spark.read.option("multiline","true").json(bn_schema_path)
bn_schema_string = bn_schema_df.select(f.to_json(bn_schema_df['bn_schema']).alias("json_column")).collect()[0]["json_column"]
bn_schema_dict = json.loads(bn_schema_string)
print(f"Columns in bn schema: {len(bn_schema_dict)}")

# COMMAND ----------

# DBTITLE 1,define xml schemas for bnlist xml data
# define xml schema structure
xml_schema = StructType([
    StructField("_Created", StringType()),
    StructField("Machine", ArrayType(StructType([
            StructField("_SerialNumber", StringType(), True),
            StructField("_Site", StringType(), True),
            StructField("_SoftwareRelease", StringType(), True),
            StructField("_VersionInfo", StringType(), True),
            StructField("_Type", StringType(), True),
            StructField("_Variant", StringType(), True),
            StructField("_Name", StringType(), True),
            StructField("_InstallationDate", TimestampType(), True),
            StructField("ParameterSection", ArrayType(StructType([
                    StructField("_Number", StringType(), True),
                    StructField("_Sortmode", StringType(), True),
                    StructField("_Stackingmode", StringType(), True),
                    StructField("_StartTime", TimestampType(), True),
                    StructField("_EndTime", TimestampType(), True),
                    StructField("Operator", StructType([
                        StructField("_Name", StringType(), True),
                        StructField("_UserId", StringType(), True)
                    ]), True),
                    StructField("Section", ArrayType(StructType([
                            StructField("Shift", StructType([
                                StructField("_Number", StringType(), True),
                                StructField("_Name", StringType(), True),
                                StructField("_StartTime", TimestampType(), True)
                            ]))
                        ])
                    ), True)
                ])
            ), True)
        ])
    ), True)
])

# COMMAND ----------

proc_day = date

# COMMAND ----------

# DBTITLE 1,Read unconverted XML data
# Read unconverted XML data
u_xml_path = f"{raw_u_bnlist_path}/{proc_day}/{site}/*.xml"
print(f"reading unconverted xml data from: {u_xml_path}")
xml_df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag","BPS") \
        .schema(xml_schema) \
        .load(u_xml_path)

# flatten the dataframe to include relevant nested attributes - using hardcoded indexes isn't ideal
xml_df = xml_df.select(
    f.col("_Created").alias("bps_created"),  # root attribute
    f.col("Machine._SerialNumber")[0].alias("machine_serialnumber"),
    f.col("Machine._Site")[0].alias("machine_site"),
    f.col("Machine._SoftwareRelease")[0].alias("machine_softwarerelease"),
    f.col("Machine._VersionInfo")[0].alias("machine_versioninfo"),
    f.col("Machine._Type")[0].alias("machine_type"),
    f.col("Machine._Variant")[0].alias("machine_variant"),
    f.col("Machine._Name")[0].alias("machine_name"),
    f.col("Machine._InstallationDate")[0].alias("machine_installationdate"),
    f.col("Machine.ParameterSection._Number")[0][0].alias("parametersection_number"),
    f.col("Machine.ParameterSection._Sortmode")[0][0].alias("parametersection_sortmode"),
    f.col("Machine.ParameterSection._Stackingmode")[0][0].alias("parametersection_stackingmode"),
    f.col("Machine.ParameterSection._StartTime")[0][0].alias("parametersection_starttime"),
    f.col("Machine.ParameterSection._EndTime")[0][0].alias("parametersection_endtime"),
    f.col("Machine.ParameterSection.Operator._Name")[0][0].alias("parametersection_operatorname"),
    f.col("Machine.ParameterSection.Operator._UserId")[0][0].alias("parametersection_userid"),
    f.col("Machine.ParameterSection.Section.Shift._Number")[0][0][0].alias("shift_number"),
    f.col("Machine.ParameterSection.Section.Shift._Name")[0][0][0].alias("shift_name"),
    f.col("Machine.ParameterSection.Section.Shift._StartTime")[0][0][0].alias("shift_starttime")
)
print(f"Columns in unconverted XML: {len(xml_df.columns)}")

xml_df.limit(10).display()


# COMMAND ----------

# DBTITLE 1,Read converted CSV data
# READ converted CSV data
csv_df = spark.read.options(header="True") \
    .options(delimiter="\t") \
    .csv(f"{raw_c_bnlist_path}/{proc_day}/{site}/*.csv")

# remove extra/non-printable characters from column headers
csv_df = csv_df.selectExpr([clean_column_header(col) for col in csv_df.columns])

# rename reserved13 (if exist) to reserved14 since the new converter creates reserved13 instead of reserved14
if "reserved13" in csv_df.columns:
    csv_df = csv_df.withColumnRenamed("reserved13", "reserved14")

# match dataframe columns to schema columns
# csv_df contains cols: reserved14, reserved148, reserved149
csv_df = apply_schema_column_filter(csv_df, bn_schema_dict)

# remove extra/non-printable characters and set empty values as null in row values
csv_df = csv_df.selectExpr(*[f"regexp_replace({col}, '\\\\x00', '') AS {col}" for col in csv_df.columns])
csv_df = csv_df.selectExpr(*[f"CASE WHEN {col} IS NULL OR TRIM({col}) = '' THEN NULL ELSE {col} END AS {col}" for col in csv_df.columns])

# cast columns to their datatypes according to the schema
csv_df = apply_bn_schema_datatypes(csv_df, bn_schema_dict)

# for testing
print(f"Columns in csv files: {len(csv_df.columns)}")

# COMMAND ----------

# DBTITLE 1,# JOIN CSV AND XML DATAFRAMES
# Create joint cols in both csv and xml
# Extract filename without .csv
csv_df = csv_df.withColumn("filename_csv", f.input_file_name().substr(-37, 33)) 
# Extract filename without _Conv.xml
xml_df = xml_df.withColumn("filename_xml", f.input_file_name().substr(-42, 33)) 

# Join in
csv_xml_df = csv_df.join(xml_df, csv_df["filename_csv"] == xml_df["filename_xml"], "inner")

# drop join keys
csv_xml_df = csv_xml_df.drop("filename_xml")
csv_xml_df = csv_xml_df.drop("filename_csv")

# append site name and ingest date
csv_xml_df = csv_xml_df \
            .withColumn('site_name',f.lit(site).cast(StringType())) \
            .withColumn("ingestion_date",f.current_date().cast(DateType()))

# for testing
print(f"Columns after joining csv and xml: {len(csv_xml_df.columns)}")
#csv_xml_df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Output to lake
bn_lake_df = csv_xml_df

# standardized file path for the date
sdz_output_path = f"{sdz_bnlist_path}/{proc_day}"
print(f"Writing to {sdz_output_path}")

# write to lake
bn_lake_df.write.format("parquet").mode("append").save(sdz_output_path)

# for testing
print(f"Columns written in lake: {len(bn_lake_df.columns)}")
#bn_lake_df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Output to UC
bn_uc_df = csv_xml_df

## Add extra cols for UC
# file_date
bn_uc_df = bn_uc_df.withColumn("file_date", to_date(lit(proc_day), "yyyy-MM-dd"))
# block_seq_num: merge reserved14, reserved148, reserved149 to block_seq_num
bn_uc_df = bn_uc_df.withColumn(
        "block_seq_num", 
        f.coalesce(bn_uc_df["reserved14"], bn_uc_df["reserved148"], bn_uc_df["reserved149"])).drop("reserved14", "reserved148", "reserved149")

# Write to table
print(f"Writing to {target_table_name}")
bn_uc_df.write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)

# for testing
print(f"Columns written in UC: {len(bn_uc_df.columns)}")
#bn_uc_df.limit(10).display()
