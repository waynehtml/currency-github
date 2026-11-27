# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BinaryType, TimestampType, ArrayType
import uuid

# COMMAND ----------

environment = dbutils.widgets.text("environment","")
storage = dbutils.widgets.text("storage","")
run_date = dbutils.widgets.text("run_date","") # eg. 2025-02-26

environment = dbutils.widgets.get("environment").strip()
print(f"environment: {environment}")
storage = dbutils.widgets.get("storage").strip()
print(f"storage: {storage}")
run_date = dbutils.widgets.get("run_date").strip()
print(f"run_date: {run_date}")


# COMMAND ----------

# MAGIC %run ../../common/utils 

# COMMAND ----------

# Get environment if not provided
if len(environment) == 0:
    environment = get_env()
print(f"environment: {environment}")

# Get lake storage if not provided
if len(storage) == 0:
    storage = get_lake_storage_account(environment)
print(f"storage: {storage}")

# run_date is required
if len(run_date) == 0:
    raise Exception("run_date is required")


# source file
file_name = f"abfss://restricted@{storage}.dfs.core.windows.net/raw/external/cision/media/{run_date}/*.xml"
print(f"file_name: {file_name}")

# Target tables
unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")
tbl_bank_note_media_info = f"{unity_catalog_name}.standardized_db.tbl_cision_media_bank_note_media_info"
print(f"tbl_bank_note_media_info: {tbl_bank_note_media_info}")
tbl_tag_association = f"{unity_catalog_name}.standardized_db.tbl_cision_media_tag_association"
print(f"tbl_tag_association: {tbl_tag_association}")

# COMMAND ----------

# DBTITLE 1,Read source file
# Define the schema for the XML structure
schema = StructType([
    StructField("_NoDoc", StringType(), True),
    StructField("mstns:title", StringType(), True),
    StructField("mstns:DateInserted", TimestampType(), True),
    StructField("mstns:DatePublication", TimestampType(), True),
    StructField("mstns:SourceName", StringType(), True),
    StructField("mstns:Media", StringType(), True),
    StructField("mstns:SubMedia", StringType(), True),
    StructField("mstns:Continent", StringType(), True),
    StructField("mstns:ProvState", StringType(), True),
    StructField("mstns:Country", StringType(), True),
    StructField("mstns:Town", StringType(), True),
    StructField("mstns:Reach", IntegerType(), True),
    StructField("mstns:Tone", StringType(), True),
    StructField("mstns:TagAssociation", ArrayType(StructType([  # Updated to ArrayType
        StructField("_Group", StringType(), True),
        StructField("_Label", StringType(), True)
    ])), True)
])

# Read the XML file
xml_df = spark.read.format("com.databricks.spark.xml") \
    .option("rootTag", "mstns:Export") \
    .option("rowTag", "mstns:Document") \
    .option("attributePrefix", "_") \
    .option("namespaceAware", "true") \
    .option("valueTag", "_VALUE") \
    .schema(schema) \
    .load(file_name)

# Calculate MEDIA_INFO_ID = hash of DocNo + DatePublication + SourceName
xml_df = xml_df.withColumn("media_info_id", F.sha1(F.concat_ws("", col("_NoDoc"), col("mstns:DatePublication"), col("mstns:SourceName"))))

# for testing
xml_df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Standardized column names
# Create the BANK_NOTE_MEDIA_INFO DataFrame
media_info_df = xml_df.select(
        "media_info_id",
        F.col("_NoDoc").alias("media_docno"),
        F.col("mstns:title").alias("media_title"),
        F.col("mstns:DatePublication").alias("media_publication_dt"),
        F.col("mstns:SourceName").alias("media_source_nm"),
        F.col("mstns:Media").alias("media_type"),
        F.col("mstns:SubMedia").alias("media_sub_type"),
        F.col("mstns:Continent").alias("continent"),
        F.col("mstns:ProvState").alias("prov_state"),
        F.col("mstns:Country").alias("country"),
        F.col("mstns:Town").alias("town"),
        F.col("mstns:Reach").cast(IntegerType()).alias("read_area_nb"),
        F.col("mstns:Tone").alias("tone")
    ).withColumn("ingestion_date", F.current_timestamp())

# for testing
print("Count of media:", media_info_df.count())
media_info_df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Create TAG_ASSOCIATION
# Create the TAG_ASSOCIATION DataFrame by exploding the TagAssociation array
tag_association_df = xml_df.withColumn("TagAssociation", F.explode("mstns:TagAssociation")) \
    .select(
        "media_info_id",
        F.col("TagAssociation._Group").alias("group"),
        F.col("TagAssociation._Label").alias("label")
    ).withColumn("ingestion_date", F.current_timestamp())

# for testing
print("Count of tag:", tag_association_df.count())
tag_association_df.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Write to UC
# Write bank note media info
print(f"Writing to table: {tbl_bank_note_media_info}")
media_info_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_bank_note_media_info)

# Write TAG_ASSOCIATION
print(f"Writing to table: {tbl_tag_association}")
tag_association_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl_tag_association)
