# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, LongType, ArrayType
from pyspark.sql.functions import col,from_unixtime,unix_timestamp, current_date, current_timestamp,substring,instr, lit, collect_list,substring_index,to_date,date_format,to_timestamp,split,trim,when, explode_outer, explode, input_file_name, from_utc_timestamp, struct
import datetime
import time
from delta.tables import *
from datetime import datetime, timedelta

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.session.timeZone", "America/Toronto")

# COMMAND ----------

def read_xml_files(full_path, schema, root_tag, row_tag):
    """
    Reads XML files into a Spark DataFrame using the provided schema, root tag, and row tag.

    Args:
        full_path (str): The full path to the XML files.
        schema (StructType): The schema to apply to the XML data.
        root_tag (str): The root tag of the XML files.
        row_tag (str): The row tag of the XML files.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the XML files.
    """
    
    df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", root_tag) \
    .option("rowTag", row_tag) \
    .option("excludeAttribute", "true") \
    .schema(schema) \
    .load(full_path)

    return df

# COMMAND ----------

def rename_columns(df: DataFrame, schema: StructType = None) -> DataFrame:
    """
    Rename DataFrame columns based on 'name' metadata in the schema.
    
    Args:
        df: PySpark DataFrame to rename columns
        schema: Optional StructType schema. If None, uses df.schema
    
    Returns:
        DataFrame with renamed columns based on metadata
    """
    if schema is None:
        schema = df.schema
    
    def build_select_expression(struct_type: StructType, parent_path: str = "", parent_alias: str = "") -> list:
        """
        Recursively build select expressions for renaming nested columns
        """
        select_fields = []
        
        for field in struct_type.fields:
            current_path = f"{parent_path}.{field.name}" if parent_path else field.name
            
            # Determine the alias name (use metadata 'name' if available, otherwise original name)
            if field.metadata and 'name' in field.metadata:
                alias_name = field.metadata['name']
            else:
                alias_name = field.name
            
            # If field is a struct, recursively process it
            if isinstance(field.dataType, StructType):
                nested_fields = build_select_expression(field.dataType, current_path, alias_name)
                if nested_fields:  # Only create struct if there are fields
                    struct_expr = struct(*nested_fields).alias(alias_name)
                    select_fields.append(struct_expr)
            else:
                # Simple field - select with alias
                select_fields.append(col(current_path).alias(alias_name))
        
        return select_fields
    
    def build_final_struct(struct_type: StructType, df: DataFrame) -> DataFrame:
        """
        Build the final DataFrame with proper nested structure
        """
        select_expressions = []
        
        for field in struct_type.fields:
            field_name = field.name
            
            # Determine the target name
            if field.metadata and 'name' in field.metadata:
                target_name = field.metadata['name']
            else:
                target_name = field_name
            
            if isinstance(field.dataType, StructType):
                # Handle nested struct
                nested_exprs = build_nested_struct_expression(field.dataType, field_name)
                if nested_exprs:
                    struct_expr = struct(*nested_exprs).alias(target_name)
                    select_expressions.append(struct_expr)
            else:
                # Simple field
                select_expressions.append(col(field_name).alias(target_name))
        
        return df.select(*select_expressions)
    
    def build_nested_struct_expression(struct_type: StructType, parent_path: str) -> list:
        """
        Build expressions for nested struct fields
        """
        nested_fields = []
        
        for field in struct_type.fields:
            current_path = f"{parent_path}.{field.name}"
            
            # Determine target name
            if field.metadata and 'name' in field.metadata:
                target_name = field.metadata['name']
            else:
                target_name = field.name
            
            if isinstance(field.dataType, StructType):
                # Recursively handle nested structs
                deeper_nested = build_nested_struct_expression(field.dataType, current_path)
                if deeper_nested:
                    nested_fields.append(struct(*deeper_nested).alias(target_name))
            else:
                # Simple nested field
                nested_fields.append(col(current_path).alias(target_name))
        
        return nested_fields
    
    # Check if any renaming is needed
    def has_metadata_renames(struct_type: StructType) -> bool:
        """Check if any field in the schema has rename metadata"""
        for field in struct_type.fields:
            if field.metadata and 'name' in field.metadata:
                return True
            if isinstance(field.dataType, StructType):
                if has_metadata_renames(field.dataType):
                    return True
        return False
    
    if not has_metadata_renames(schema):
        print("No fields with 'name' metadata found.")
        return df
    
    # Print what will be renamed
    def print_renames(struct_type: StructType, parent_path: str = ""):
        """Print the rename operations that will be performed"""
        for field in struct_type.fields:
            current_path = f"{parent_path}.{field.name}" if parent_path else field.name
            
            if field.metadata and 'name' in field.metadata:
                new_name = field.metadata['name']
                new_path = f"{parent_path}.{new_name}" if parent_path else new_name
                print(f"  {current_path} -> {new_path}")
            
            if isinstance(field.dataType, StructType):
                next_parent = f"{parent_path}.{field.metadata.get('name', field.name)}" if parent_path else field.metadata.get('name', field.name)
                print_renames(field.dataType, next_parent)
    
    print("Renaming columns based on schema metadata:")
    print_renames(schema)
    
    # Apply the transformation
    return build_final_struct(schema, df)

# COMMAND ----------

def get_array_columns(schema, parent_path=""):
    """
    Recursively find all array columns in a schema
    Returns a list of tuples: (column_path, array_element_schema)
    """
    array_columns = []
    
    for field in schema.fields:
        current_path = f"{parent_path}.{field.name}" if parent_path else field.name
        
        if isinstance(field.dataType, ArrayType):
            array_columns.append((current_path, field.dataType.elementType))
            
            # If array contains structs, recursively check for nested arrays
            if isinstance(field.dataType.elementType, StructType):
                nested_arrays = get_array_columns(field.dataType.elementType, f"{current_path}_element")
                array_columns.extend(nested_arrays)
                
        elif isinstance(field.dataType, StructType):
            # Recursively check struct fields for arrays
            nested_arrays = get_array_columns(field.dataType, current_path)
            array_columns.extend(nested_arrays)
    
    return array_columns

# COMMAND ----------

def get_struct_columns(schema, parent_path=""):
    """
    Recursively find all struct columns in a schema
    Returns a list of column paths
    """
    struct_columns = []
    
    for field in schema.fields:
        current_path = f"{parent_path}.{field.name}" if parent_path else field.name
        
        if isinstance(field.dataType, StructType):
            struct_columns.append(current_path)
            # Recursively check nested structs
            nested_structs = get_struct_columns(field.dataType, current_path)
            struct_columns.extend(nested_structs)
    
    return struct_columns

# COMMAND ----------

def flatten_struct_columns(df, struct_columns):
    """
    Flatten struct columns by selecting their nested fields while preserving arrays.
    Supports multiple levels of nesting.
    """
    select_cols = []
    struct_col_names = [col_path for col_path in struct_columns if "." not in col_path]
    
    def get_nested_fields(field_type, parent_path=""):
        """
        Recursively extract all nested field paths from a struct type
        Returns list of tuples: (full_path, leaf_field_name)
        """
        nested_fields = []
        if isinstance(field_type, StructType):
            for nested_field in field_type.fields:
                current_path = f"{parent_path}.{nested_field.name}" if parent_path else nested_field.name
                if isinstance(nested_field.dataType, StructType):
                    # Recursively get nested fields
                    nested_fields.extend(get_nested_fields(nested_field.dataType, current_path))
                else:
                    # Leaf field - add to list
                    nested_fields.append((current_path, nested_field.name))
        return nested_fields
    
    # Get all top-level columns that are not structs (including arrays!)
    for field in df.schema.fields:
        if not isinstance(field.dataType, StructType):
            # Keep non-struct columns (including arrays)
            select_cols.append(col(field.name))
        elif field.name not in struct_col_names:
            # Keep struct columns that are not in our flatten list
            select_cols.append(col(field.name))
    
    # Flatten struct columns
    for struct_col in struct_col_names:
        struct_field = next((f for f in df.schema.fields if f.name == struct_col), None)
        if struct_field and isinstance(struct_field.dataType, StructType):
            # Get all nested fields recursively
            nested_fields = get_nested_fields(struct_field.dataType, struct_col)
            
            for full_path, leaf_name in nested_fields:
                # Use only the leaf field name as alias (remove all parent prefixes)
                select_cols.append(col(full_path).alias(leaf_name))
    
    return df.select(*select_cols)

# COMMAND ----------

def dynamic_explode_arrays(df, explode_outer_join=True):
    """
    Dynamically explode all array columns in a DataFrame
    
    Args:
        df: Input DataFrame
        explode_outer_join: If True, use explode_outer to preserve rows with empty arrays
    
    Returns:
        DataFrame with arrays exploded
    """
    current_df = df
    array_columns = get_array_columns(current_df.schema)
    
    print(f"Found {len(array_columns)} array columns to explode:")
    for col_path, element_type in array_columns:
        print(f"  - {col_path} (element type: {element_type})")
    
    # Process each array column
    for col_path, element_type in array_columns:
        if "." not in col_path:  # Only process top-level arrays for now
            
            # Get all columns except the array column
            other_cols = [col(f.name) for f in current_df.schema.fields if f.name != col_path]
            
            # Explode the array column
            if explode_outer_join:
                exploded_col = explode_outer(col(col_path)).alias(f"{col_path}")
            else:
                exploded_col = explode(col(col_path)).alias(f"{col_path}")
            
            # Select other columns plus exploded array
            current_df = current_df.select(*other_cols, exploded_col)
            
            # If the exploded array contains structs, flatten them
            if isinstance(element_type, StructType):
                struct_cols = []
                for nested_field in element_type.fields:
                    # Use just the nested field name without parent prefix
                    alias_name = nested_field.name
                    struct_cols.append(col(f"{col_path}.{nested_field.name}").alias(alias_name))
                
                # Replace the exploded struct with its flattened fields
                other_cols = [col(f.name) for f in current_df.schema.fields if f.name != f"{col_path}"]
                current_df = current_df.select(*other_cols, *struct_cols)
    
    return current_df

# COMMAND ----------

def flatten_nested_columns(df):
    """
    Flatten a DataFrame's columns by exploding arrays and flattening structs.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with flattened columns
    """

    # Step 1: Flatten struct columns
    struct_columns = get_struct_columns(df.schema)
    if struct_columns:
        print(f"\nStep 1: Flattening {len(struct_columns)} struct columns:")
        for struct_col in struct_columns:
            print(f"  - {struct_col}")
        df_flattened_structs = flatten_struct_columns(df, struct_columns)
    else:
        print("\nStep 1: No struct columns found to flatten")
        df_flattened_structs = df
    df_flattened_structs.printSchema()

    # Step 2: Explode array columns
    print(f"\nStep 2: Exploding array columns...")
    df_final = dynamic_explode_arrays(df_flattened_structs)

    print(f"\nFinal Flattened DataFrame Schema:")
    df_final.printSchema()

    return df_final

# COMMAND ----------

def has_struct_or_array(schema):
    return any(isinstance(field.dataType, (StructType, ArrayType)) for field in schema.fields)

# COMMAND ----------

def perform_standard_transform(df, file_type, file_date):

    while has_struct_or_array(df.schema):
        df = flatten_nested_columns(df)
    
    # Flatten the nested columns in the dataframe
    #df = flatten_nested_columns(df)
    #df = flatten_nested_columns(df)
    #df = flatten_nested_columns(df)
    #df.limit(10).display()

    # Add file_date column
    df = df.withColumn('file_date', to_date(lit(file_date), "yyyy-MM-dd"));

    # Add file_name column
    df = df.withColumn("file_name", substring_index(input_file_name(), "/", -1))

    return df

# COMMAND ----------

def perform_dataset_transform(df, file_type, file_date):

    # add denom form sku column
    if "sku" in df.columns:
        df = df.withColumn("denom", split(col("sku"), "-").getItem(0).cast("integer"))

    # Try to get and call the function
    try:
        func = globals().get(file_type)
        if func and callable(func):
            print(f"Applying specific logic for {file_type}")
            df = func(df, file_type, file_date)
        else:
            print(f"No specific logic found for {file_type}")
    except Exception as e:
        print(f"Error processing {file_type}: {e}")
    
    return df

# COMMAND ----------

def write_to_unity_catalog(df, full_table_name, file_date, 
                          partition_column="file_date", mode="append"):
    """
    Write DataFrame to Unity Catalog table with partitioning and reload capability
    
    Args:
        df: DataFrame to write
        full_table_name: Table name
        file_date: Date for partitioning (string in YYYY-MM-DD format)
        partition_column: Column name for partitioning (default: 'file_date')
        mode: Write mode ('append', 'overwrite')
    """

    print(f"Writing to Unity Catalog table: {full_table_name}");
    print(f"File date: {file_date}");
    print(f"Partition column: {partition_column}");
    print(f"Write mode: {mode}");
    
    # add update_time column
    df = df.withColumn("update_time", current_timestamp())

    if mode == "append":
        # For reload capability, delete existing data for this file_date first
        print(f"Removing existing data for {partition_column} = '{file_date}'...");
        
        # Delete existing partition data
        delete_sql = f"""
        DELETE FROM {full_table_name} 
        WHERE {partition_column} = '{file_date}'
        """;
        spark.sql(delete_sql);
        print(f"Deleted existing records for {partition_column} = '{file_date}'");
        
        # Write new data
        df.write \
          .format("delta") \
          .mode("append") \
          .option("partitionOverwriteMode", "dynamic") \
          .partitionBy(partition_column) \
          .saveAsTable(full_table_name);

    elif mode == "overwrite":
        # Overwrite entire table
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .option("partitionOverwriteMode", "dynamic") \
          .partitionBy(partition_column) \
          .saveAsTable(full_table_name);
          
    print(f"Successfully wrote {df.count()} records to {full_table_name}");
