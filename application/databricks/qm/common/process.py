# Databricks notebook source
from pyspark.sql import DataFrame
import re
from pyspark.sql import functions as F
from pyspark.sql.functions import round, col, lit
from pyspark.sql.types import *
from typing import Any

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") # support this format June 10, 2025
#spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.session.timeZone", "America/Toronto")

# COMMAND ----------

def header(text, level=1, add_newline=True, color="black"):
    """
    Print formatted header in Databricks using HTML header tags.
    
    Parameters:
    -----------
    text : str
        The header text to display
    level : int, default=1
        Header level (1-6, where 1 is largest)
    add_newline : bool, default=True
        Whether to add a line break before the header
    color : str, default="black"
        Color of the header text
        
    Example:
    --------
    print_header("Main Title", level=1)
    print_header("Subtitle", level=2, color="blue")
    print_header(f"After add lit columns: {len(df_litho.columns)} columns", level=3)
    """
    newline = "<br>" if add_newline else ""
    level = max(1, min(6, level))  # Ensure level is between 1-6
    displayHTML(f"{newline}<h{level} style='color:{color};'>{text}</h{level}>")


# COMMAND ----------

def list_files(folder_path: str, file_extension: str = ".csv") -> list:
    """
    Lists all files in a folder with a given extension using dbutils.
    
    Args:
        folder_path (str): The DBFS folder path (e.g., '/mnt/data/my_folder/')
        file_extension (str): File extension to filter (default is '.csv')
    
    Returns:
        List[str]: List of file paths with the specified extension.
    """
    files = dbutils.fs.ls(folder_path)
    return [f.name for f in files if f.path.endswith(file_extension)]


# COMMAND ----------

def standardize_keys(dictionary):
    """Return a new dictionary with whitespace trimmed from keys"""
    return {key.strip().replace(" ", "").lower(): value for key, value in dictionary.items()}

# COMMAND ----------

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Alternative implementation using toDF() method.
    
    Args:
        df (DataFrame): Input Spark DataFrame
        
    Returns:
        DataFrame: DataFrame with cleaned column names
    """
    # Clean all column names at once
    cleaned_columns = [re.sub(r'[\n\r\s]+', '', col).lower() for col in df.columns]
    return df.toDF(*cleaned_columns)

# COMMAND ----------

def find_column_name(col_name, schema, match='rough'):
    """
    Find matching schema element for a column name
    
    Args:
        col_name: Column name to match
        schema: List of schema definitions with comma-separated tags
        matchtype: "exact" or "like" matching mode
    
    Returns:
        Matching schema element or None if no match found
    """
    
    for col_def in schema:
        # Split the tag by comma and strip whitespace
        tags = [tag.strip() for tag in col_def["tag"].split(",")]
        #print(f"col_name: {col_name}, tags: {tags}")
        
        if match == "exact":
            # Only check if col_name exactly matches any of the tags
            if col_name in tags:
                return col_def
        elif match == "rough":
            # First check if col_name exactly matches any of the tags
            if col_name in tags:
                return col_def
            # Then check if col_name contains any of the tags
            for tag in tags:
                if tag in col_name:
                    return col_def
    
    # No match found
    return None

# COMMAND ----------

def get_cast_expression(column_name, col_type):
    if isinstance(col_type, DateType):
        return F.coalesce(
            F.to_date(F.col(column_name), "dd/MM/yyyy"),      # 20/11/2023
            F.to_date(F.col(column_name), "MMM dd, yyyy"),    # Jun 10, 2023
            F.to_date(F.col(column_name), "MMMM dd, yyyy"),   # June 10, 2023   
            F.to_date(F.col(column_name), "dd-MMM-yy"),       # 04-Jun-25
            F.to_date(F.col(column_name), "dd-MMM-yyyy"),     # in case you have 04-Jun-2025
            F.to_date(F.col(column_name), "MM/dd/yyyy"),      # fallback for US format
            F.to_date(F.col(column_name), "yyyy-MM-dd")       # fallback for ISO format
        )
    else:
        return F.col(column_name).cast(col_type)

# COMMAND ----------

def get_cast_expression(column_name, col_type):
    if isinstance(col_type, DateType):
        return F.coalesce(
            # Handle ISO datetime strings (most common)
            F.to_date(F.col(column_name)),  # This handles "yyyy-MM-dd HH:mm:ss" automatically
            # Handle other formats
            F.to_date(F.col(column_name), "MMM dd, yyyy"),
            F.to_date(F.col(column_name), "MM/dd/yyyy"),
            F.to_date(F.col(column_name), "dd/MM/yyyy")
        )
    else:
        return F.col(column_name).cast(col_type)

# COMMAND ----------

def match_names_by_tags(tags, names, operator="exact"):
    """
    Match names based on tags using exact or rough matching.
    
    Parameters:
    tags (list): List of keywords/tags to match against
    names (list): List of names to search through
    operator (str): "exact" for exact matching, "rough" for substring matching
    
    Returns:
    str: First matched name, or None if no match found
    """
    if operator not in ["exact", "rough"]:
        raise ValueError("Operator must be 'exact' or 'rough'")
    
    for tag in tags:
        tag_lower = tag.lower()  # Case-insensitive matching
        
        for name in names:
            name_lower = name.lower()
            
            if operator == "exact":
                # Exact matching: tag equals name
                if tag_lower == name_lower:
                    return name
            elif operator == "rough":
                # Rough matching: name contains tag
                if tag_lower in name_lower:
                    return name
    
    return None

# COMMAND ----------

def schema_convert_by_name(df, schema, match='exact'):
    """
    Process a single DataFrame according to schema definition
    
    Args:
        df: Spark DataFrame to process
        schema: List of column definitions with tag, name, and type
    
    Returns:
        Processed DataFrame
        
    Raises:
        Exception: If processed DataFrame has fewer columns than schema definition
    """

    # Show column numbers in dataframe and schema
    print(f"Processing DataFrame with {len(df.columns)} columns")
    print(f"Schema defines {len(schema)} columns")
     
    # Match columns in the DataFrame with schema definitions
    # if find column name in schema, rename it to schema name and cast to schema type
    # if not find column name in schema, drop it
    for col_name in df.columns:
        print(f"Processing column: {col_name}")

        col_def = find_column_name(col_name, schema, match)
        print(f"  Found schema definition: {col_def}")
        if col_def:
            # Found matching schema definition
            #col_def = tag_to_schema[col_name]
            new_name = col_def["name"]
            col_type = col_def["type"]
            
            # Rename column to schema name
            if new_name != col_name:
                df = df.withColumnRenamed(col_name, new_name)
                print(f"  Renamed column: {col_name} -> {new_name}")
            
            # Cast column to specified type
            df = df.withColumn(
                new_name, 
                get_cast_expression(new_name, col_type)
            )
            print(f"  Cast column {new_name} to {col_type}")
        else:
            # Column not found in schema - drop it
            df = df.drop(col_name)
            print(f"Dropped column: {col_name}")
    
    # Now, df only has columns that is defined in schema
    # Let us check if any columns missing in schema that is nullable, make them null
    for col_def in schema:
        if (col_def["name"] not in df.columns) and ('null' in col_def and col_def['null']):
            print(f"Adding null column: {col_def['name']}")
            df = df.withColumn(col_def["name"], F.lit(None).cast(col_def["type"]))

    # Compare column count in dataframe and schema, and make sure they are the same
    # If they are different, raise exception
    if len(df.columns) != len(schema):
        raise Exception(f"Column count mismatch: DataFrame has {len(df.columns)} columns but schema defines {len(schema)} columns")
    
    return df

# COMMAND ----------

# def schema_convert_by_index(df, schema):
#     """
#     Process a DataFrame by matching columns to schema definitions by index position
#     
#     Args:
#         df: Spark DataFrame to process
#         schema: List of column definitions with tag, name, and type (ordered)
#     
#     Returns:
#         Processed DataFrame
#         
#     Raises:
#         Exception: If DataFrame has different number of columns than schema definition
#     """
#     print(f"Processing DataFrame with {len(df.columns)} columns")
#     print(f"Schema defines {len(schema)} columns")
#     
#     # Check if DataFrame has fewer columns than schema
#     if len(df.columns) < len(schema):
#         raise Exception(f"DataFrame has fewer columns ({len(df.columns)}) than schema definition ({len(schema)})")
#     
#     # Start with the original DataFrame
#     processed_df = df
#     
#     # Get the columns to process (limited by schema length)
#     columns_to_process = df.columns[:len(schema)]
#     columns_to_drop = df.columns[len(schema):] if len(df.columns) > len(schema) else []
#     
#     if columns_to_drop:
#         print(f"Will drop {len(columns_to_drop)} extra columns: {columns_to_drop}")
#     
#     # Process each column by index position (only up to schema length)
#     for i, (col_name, col_def) in enumerate(zip(columns_to_process, schema)):
#         print(f"Processing column {i}: {col_name}")
#         
#         new_name = col_def["name"]
#         col_type = col_def["type"]
#         
#         # Rename column to schema name
#         if new_name != col_name:
#             processed_df = processed_df.withColumnRenamed(col_name, new_name)
#             print(f"  Renamed column: {col_name} -> {new_name}")
#         
#         # Cast column to specified type
#         processed_df = processed_df.withColumn(
#             new_name, 
#             get_cast_expression(new_name, col_type)
#         )
#         print(f"  Cast column {new_name} to {col_type}")
#     
#     # Drop extra columns that are not covered by schema
#     for col_name in columns_to_drop:
#         processed_df = processed_df.drop(col_name)
#         print(f"Dropped extra column: {col_name}")
#     
#     return processed_df

# COMMAND ----------

# def schema_convert(df, schema, selector="index", match="rough"):
#     """
#     Convert a Spark DataFrame to match a given schema using either index or name matching.
#
#     Args:
#         df: Spark DataFrame to process
#         schema: List of schema definitions with 'tag', 'name', and 'type'
#         selector: 'index' to match by index, otherwise match by name
#
#     Returns:
#         Processed DataFrame
#     """
#     if selector == "index":
#         return schema_convert_by_index(df, schema)
#     else:
#         return schema_convert_by_name(df, schema, match)

# COMMAND ----------

def standardize_sheet(pdf, schema, mode = "index"):
    """
    Standardize a pandas DataFrame to match a given schema and return a Spark DataFrame.

    Args:
        pdf (pd.DataFrame): Input pandas DataFrame to standardize.
        schema (list): List of schema definitions, each with 'tag', 'name', and 'type' keys.

    Returns:
        DataFrame: Spark DataFrame with standardized column names and types.
    """

    # Convert pands to spark
    header("Convert Panda to Spark")
    sdf = spark.createDataFrame(pdf)
    print(f"Original schema")
    sdf.printSchema()

    # standardize column names
    header("Standardize Column Names")
    sdf = clean_column_names(sdf)
    sdf.printSchema()

    # schema conversion
    header("Schema Conversion")
    if (mode == "index"):
        sdf = schema_convert_by_index(sdf, schema)
    else:
        sdf = schema_convert_by_tag(sdf, schema)
    sdf.printSchema()
    return sdf

# COMMAND ----------

def add_lit_columns(df: DataFrame, **kwargs: Any) -> DataFrame:
    """
    Add new columns to a PySpark DataFrame with specified values.
    
    Parameters:
    -----------
    df : DataFrame
        The input PySpark DataFrame
    **kwargs : Any
        Keyword arguments where keys are column names and values are the values to assign
        
    Returns:
    --------
    DataFrame
        New DataFrame with additional columns
    """
    
    # Start with the original DataFrame
    result_df = df
    
    # Add each new column with its corresponding value
    for col_name, col_value in kwargs.items():
        result_df = result_df.withColumn(col_name, lit(col_value))
    
    return result_df

# COMMAND ----------

def read_csv_cells(binary_file, cells):
    """
    Read multiple specific cell data from pandas DataFrame
    
    Args:
        pandas_df (pd.DataFrame): Pandas DataFrame to read from
        row_col_pairs (dict): Dictionary with keys as identifiers and values as "row,col" strings
                             Example: {"key": "1,B", "key1": "2,C"}
    
    Returns:
        dict: Dictionary with same keys and cell values as strings
              Example: {"key": "value", "key1": "value1"}
    """
    
    try:
        # Initialize result dictionary
        result = {}

        pandas_df = pd.read_csv(BytesIO(binary_file.select("content").first()[0]), header=None, dtype=str)
        
        # Process each key-value pair
        for key, row_col_str in cells.items():
            try:
                # Parse the "row,col" string
                parts = row_col_str.split(',')
                if len(parts) != 2:
                    print(f"Warning: Invalid format for key '{key}': '{row_col_str}'. Expected 'row,col'")
                    result[key] = None
                    continue
                
                row_str, col_str = parts
                row = int(row_str.strip())
                col = col_str.strip().upper()
                
                # Convert column letter to number (A=0, B=1, C=2, etc.)
                col_num = ord(col) - ord('A')
                
                # Convert to 0-based row index
                row_idx = row - 1
                
                # Check bounds
                if row_idx < 0 or row_idx >= pandas_df.shape[0]:
                    print(f"Warning: Row {row} for key '{key}' is out of bounds (DataFrame has {pandas_df.shape[0]} rows)")
                    result[key] = None
                    continue
                    
                if col_num < 0 or col_num >= pandas_df.shape[1]:
                    print(f"Warning: Column {col} for key '{key}' is out of bounds (DataFrame has {pandas_df.shape[1]} columns)")
                    result[key] = None
                    continue
                
                # Extract cell value
                cell_value = pandas_df.iloc[row_idx, col_num]
                
                # Convert to string, handle NaN/None values
                if pd.isna(cell_value):
                    result[key] = None
                else:
                    result[key] = str(cell_value).strip()
                    
            except ValueError as ve:
                print(f"Error parsing row/col for key '{key}': {str(ve)}")
                result[key] = None
            except Exception as e:
                print(f"Error reading cell for key '{key}': {str(e)}")
                result[key] = None
        
        return result
    except Exception as e:
        print(f"Error reading DataFrame: {str(e)}")
        print(f"Error type: {type(e)}")
        return {}

# COMMAND ----------

def read_csv_table(binary_file, skiprows=8, header=0):
    """
    Read CSV file into Spark DataFrame using binary file reader and pandas
    
    Args:
        binary_file: Binary file DataFrame containing the CSV content
        skiprows (int): Number of rows to skip from the beginning
        header (int): Row number to use as column headers (0-based after skipping rows)
    
    Returns:
        pyspark.sql.DataFrame: Spark DataFrame with the CSV data
    """
        
    # Read with pandas, skipping rows and setting header
    df_pandas = pd.read_csv(BytesIO(binary_file.select("content").first()[0]), skiprows=skiprows, header=header, dtype=str)
    
    # Clean up column names (remove extra spaces and special characters)
    #df_pandas.columns = df_pandas.columns.str.strip().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    
    # Remove completely empty rows
    df_pandas = df_pandas.dropna(how='all')
    
    # Reset index
    df_pandas = df_pandas.reset_index(drop=True)
    
    # Convert pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df_pandas)
    
    return spark_df

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
