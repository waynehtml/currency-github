# Databricks notebook source
storage = dbutils.widgets.text("storage","lakecacnpdstdev01", "storage")
brs_filetype = dbutils.widgets.text("brs_filetype","qm11", "brs_filetype")
file_date = dbutils.widgets.text("file_date","","file_date")

storage = dbutils.widgets.get("storage")
brs_filetype = dbutils.widgets.get("brs_filetype")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run ./definition

# COMMAND ----------

folder_path =  f"raw/internal/sap/brs/{brs_filetype}/{file_date}"
input_directory = f"abfss://restricted@{storage}.dfs.core.windows.net/{folder_path}"
print(f"Input directory: {input_directory}")

# COMMAND ----------

def validate_csv_headers(df, schema):
    """
    This function checks if the CSV headers match the schema's column names.
    
    Parameters:
    df (DataFrame): The input dataframe from the CSV file
    schema (StructType): The original schema to validate against
    
    Returns:
    bool: True if the headers match the schema, False otherwise
    """
    
    # Get the column names from the CSV file (df)
    csv_columns = df.columns
    
    # Get the column names from the schema
    schema_columns = [field.name for field in schema.fields]
    
    # Check if the column names match
    if csv_columns != schema_columns:
        print(f"Error: CSV headers do not match the schema column names.")
        print(f"CSV Headers: {csv_columns}")
        print(f"Schema Columns: {schema_columns}")
        return False
    return True

def validate_schema(df, original_schema):
    """
    This function validates the columns of a DataFrame against the original schema.
    
    Parameters:
    df (DataFrame): The input dataframe with columns as StringType and nullable
    original_schema (StructType): The original schema containing the original data types and nullability
    
    Returns:
    bool: True if all validations pass, False if there are any validation issues
    """
    no_validation_error = True

    # Check if headers match the schema
    if not validate_csv_headers(df, original_schema):
        return False  # Exit early if the headers do not match
    
    for field in original_schema.fields:
        # Check for NULLs only allowed in original nullable columns
        if not field.nullable:
            null_count = df.filter(F.col(f"`{field.name}`").isNull()).count()
            if null_count > 0:
                print(f"Error: Column '{field.name}' contains NULL values but is not nullable in the original schema.")
                no_validation_error = False
        
        # Check if column can be converted back to the original type without data loss
        if not isinstance(field.dataType, StringType):
            # non-string type: Date, Decimal, Integer
            if isinstance(field.dataType, DateType):
                # DateType
                if field.nullable:
                    df = df.withColumn(
                        field.name, 
                        F.when(F.col(f"`{field.name}`") == "00000000", None))
                df_invalid = df.filter(F.to_date(F.col(f"`{field.name}`"), "yyyyMMdd").isNull() & F.col(f"`{field.name}`").isNotNull())                
                invalid_count = df_invalid.count()
                if invalid_count > 0:
                    print(f"Error: Column '{field.name}' contains values that cannot be cast to DateType: {invalid_count}.")
                    df_invalid.display()
                    no_validation_error = False
            elif isinstance(field.dataType, DecimalType):
                # DecimalType
                precision = field.dataType.precision  # Get the precision
                scale = field.dataType.scale          # Get the scale
                casted_df = df.withColumn("temp_cast", F.col(field.name).cast(DecimalType(precision, scale)))
                df_invalid = casted_df.filter(F.col(field.name).isNotNull() & F.col("temp_cast").isNull())
                invalid_count = df_invalid.count()
                df.drop("temp_cast")
                if invalid_count > 0:
                    print(invalid_count)
                    print(f"Error: Column '{field.name}' contains values that cannot be cast to DecimalType with ({precision},{scale}): {invalid_count}.")
                    df_invalid.display()
                    no_validation_error = False
            elif isinstance(field.dataType, IntegerType):
                # IntegerType
                casted_df = df.withColumn("temp_cast", F.col(field.name).cast(IntegerType()))
                df_invalid = casted_df.filter(F.col(field.name).isNotNull() & F.col("temp_cast").isNull())
                invalid_count = df_invalid.count()
                df.drop("temp_cast")
                if invalid_count > 0:
                    print(f"Error: Column '{field.name}' contains values that cannot be cast to IntegerType: {df_invalid}.")
                    df_invalid.display()
                    no_validation_error = False
            else:
                print(f"Error: Validation for type {type(field.dataType)} is not implemented.")
                no_validation_error = False
        else:
            # StringType
            allowed_values = field.metadata.get("allowed_values", None)
            if allowed_values is not None:
                df_invalid = df.filter(~F.col(f"`{field.name}`").isin(allowed_values))
                invalid_count = df_invalid.count()
                if invalid_count > 0:
                    print(f"Error:  Column '{field.name}' contains values that is not in the allowed values: {invalid_count}")
                    df_invalid.display()
                    no_validation_error = False

    return no_validation_error


# COMMAND ----------


original_schema = BRS_File_Headers[brs_filetype]

# Apply the new schema where all types are StringType and nullable
new_schema = flip_to_string_type(original_schema)

#Load the csv data
file_path = f"{input_directory}/*"
print(f"file_path:{file_path}")
df = spark.read.csv(file_path, header=True, schema=new_schema)

for field in original_schema.fields:
    # only keep number and decimal points in decimal columns, especially for 'Reference number'
    if isinstance(field.dataType, DecimalType):
        df = df.withColumn(
            field.name, 
            F.regexp_replace(F.col(f"`{field.name}`"), "[^0-9.]", '')
        )
    
    # find nullable DateType columns and replace "00000000" with NULL
    if isinstance(field.dataType, DateType) and field.nullable:
        df = df.withColumn(
            field.name, 
            F.when(F.col(f"`{field.name}`") == "00000000", None).otherwise(F.col(f"`{field.name}`"))
        )

# Validate the schema
result = validate_schema(df, original_schema)
if result:
    print("Schema validation passed.")
    #dbutils.notebook.exit("Schema validation passed.")
else:
    print("Schema validation failed.")
    #dbutils.notebook.exit("Schema validation failed.")
    raise Exception("Schema validation failed.")


