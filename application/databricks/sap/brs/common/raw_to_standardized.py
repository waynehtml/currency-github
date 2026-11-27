# Databricks notebook source
storage = dbutils.widgets.text("storage","lakecacnpdstdev01")
brs_filetype = dbutils.widgets.text("brs_filetype","qm11")
file_date = dbutils.widgets.text("file_date","")
env = dbutils.widgets.text("env","dev")

storage = dbutils.widgets.get("storage")
brs_filetype = dbutils.widgets.get("brs_filetype")
file_date = dbutils.widgets.get("file_date")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %run ./definition

# COMMAND ----------

# MAGIC %run ../../../common/utils

# COMMAND ----------

from pyspark.sql import functions as f
import re

# COMMAND ----------

# Function to convert date, integer, and decimal columns based on the schema
# It also normalizes the column name which will switch it from the source schema to delta allowed schema
# by making all letters lower can and swapping special characters to _
def convert_columns(df, schema):
    for field in schema.fields:
        format_info = field.metadata.get('format', '')
        normalized_col = re.sub(r'[.,\;{}\(\)\n\t= ]', '_', field.name.lower())

        # Convert DateType fields (initially loaded as StringType) using 'yyyyMMdd'
        if isinstance(field.dataType, DateType):
            df = df.withColumn(
                normalized_col,
                f.when(f.col(f"`{normalized_col}`") == "00000000", None)
                .otherwise(f.to_date(f.col(f"`{normalized_col}`"), format_info))
            )
        
        # Convert IntegerType fields (initially loaded as StringType)
        elif isinstance(field.dataType, IntegerType):
            df = df.withColumn(
                normalized_col,
                f.when(f.col(f"`{normalized_col}`") == "", None)
                .otherwise(f.col(f"`{normalized_col}`").cast("int"))
            )
        
        # Convert DecimalType fields based on precision and scale in metadata (initially loaded as StringType)
        elif isinstance(field.dataType, DecimalType):
            precision = field.dataType.precision  # Get the precision
            scale = field.dataType.scale          # Get the scale
            # Apply the transformation:
            # 1. Remove non-numeric characters except periods using regexp_replace
            # 2. Cast to DecimalType with the specified precision and scale
            df = df.withColumn(
                normalized_col,
                f.when(f.col(f"`{normalized_col}`") == "", None)  # Replace empty strings with None (NULL)
                .otherwise(
                    f.regexp_replace(f.col(f"`{normalized_col}`").cast("string"), "[^0-9.]", "")  # Remove non-numeric characters
                    .cast(DecimalType(precision, scale))  # Cast to DecimalType with precision and scale
                )
            )

    return df

def normalize_column_names(df):
    for col in df.columns:
        normalized_col = re.sub(r'[.,\;{}\(\)\n\t= ]', '_', col.lower())
        print(normalized_col)  # Print the normalized column name for verification
        df = df.withColumnRenamed(col, normalized_col)
    return df


# COMMAND ----------

# Get Catalog Name
unity_catalog_name = get_unity_catalog_name(env)  

source_file = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/internal/sap/brs/{brs_filetype}/{file_date}/*"
print(f"Source file: {source_file}")
target_table = f"{unity_catalog_name}.standardized_db.tbl_sap_brs_{brs_filetype}"
print(f"Target Table: {target_table}")

# COMMAND ----------

#
original_schema = BRS_File_Headers[brs_filetype]

# Apply the new schema where all types are StringType and nullable
new_schema = flip_to_string_type(original_schema)

# Read in the CSV file
df_inputfile = spark.read.option("header",True).schema(new_schema).csv(source_file)

# Change the original column names to valid delta table names (and lowercase)
df_renamed = normalize_column_names(df_inputfile)

# Switch the schema from all string to target schema.  This should always work as the file data has been validated first
df_outputfile = convert_columns(df_renamed, original_schema)
df_outputfile.display()

# Clean up records on the same file_date
delete_sql = f"DELETE FROM {target_table} WHERE file_date = '{file_date}'"
print(f"delete_sql: {delete_sql}")
spark.sql(delete_sql)

#Append the CSV data to the delta table#
df_outputfile = df_outputfile.withColumn("file_date", lit(file_date))
df_outputfile.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)

