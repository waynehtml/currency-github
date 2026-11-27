# Databricks notebook source
# MAGIC %md
# MAGIC # NES File Validation Notebook
# MAGIC
# MAGIC Validates raw NES files **before** ingestion by `02_raw_to_standardized_NES`.
# MAGIC
# MAGIC ## Checks performed (per file)
# MAGIC | # | Check | Failure behaviour |
# MAGIC |---|---|---|
# MAGIC | 1 | **File size** — zero-byte file | Raises exception |
# MAGIC | 2 | **Empty data** — header-only file (0 data rows) | Raises exception |
# MAGIC | 3 | **File naming convention** — must match `nes_<type>_yyyy_MM_dd_HH_mm.csv` | Raises exception |
# MAGIC | 4 | **Header content** — same set of columns (missing / unexpected) | Raises exception listing missing / unexpected columns |
# MAGIC | 5 | **Mandatory columns** — no null or empty-string values | Raises exception listing offending columns and counts |
# MAGIC | 6 | **Timestamp format** — all non-null values parse with the expected format | Raises exception listing offending columns and counts |
# MAGIC | 7 | **Numeric castability** — decimal / integer / bigint columns contain only parseable values | Raises exception listing offending columns and counts |
# MAGIC | 8 | **Boolean castability** — boolean columns contain only `true`/`false`/`1`/`0`/null | Raises exception listing offending columns and counts |
# MAGIC | 9 | **Duplicate rows** — no rows with the exact same values | Raises exception |
# MAGIC | 10 | **Multiple files per type** — warns when more than one file found for same type / date | Warning in summary |
# MAGIC
# MAGIC ## Exit codes
# MAGIC - `valid` — all files passed all checks (duplicates warn only).
# MAGIC - `zero_size` — one or more files were zero bytes.
# MAGIC - `no_files` — no matching files found for the requested file type and date.
# MAGIC - Raises `Exception` on any hard failure (checks 2–8).

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("env", "", "env")
dbutils.widgets.text("pull_date", "", "pull_date")  # yyyy-MM-dd

dbutils.widgets.dropdown(
    "file_key",
    "--please-select--",
    [
        "--please-select--",
        "shortage",
        "deposits",
        "overcap",
        "rdc_profile",
        "rdc_profile_details",
        "counterfeits",
        "withdrawal",
        "shipments",
        "inventory_snapshot",
        "non_conformances"
    ]
)

env = dbutils.widgets.get("env")
pull_date = dbutils.widgets.get("pull_date")
file_key = dbutils.widgets.get("file_key").lower()
delimiter = ","

print(f"env: {env}")
print(f"pull_date: {pull_date}")
print(f"file_key: {file_key}")
print(f"delimiter: {delimiter}")

# COMMAND ----------

# DBTITLE 1,Load utilities
# MAGIC %run currency/common/utils

# COMMAND ----------

# DBTITLE 1,Imports
import re
from pyspark.sql.functions import col, to_timestamp, trim, coalesce

# COMMAND ----------

# DBTITLE 1,Resolve environment and storage
if len(env.strip()) == 0:
    env = get_env()
if len(pull_date.strip()) == 0:
    raise Exception("pull_date is required (format: yyyy-MM-dd)")

print(f"resolved env: {env}")
print(f"pull_date: {pull_date}")

# COMMAND ----------

# DBTITLE 1,File configurations
# ---------------------------------------------------------------------------
# FILE_CONFIGS mirrors 02_raw_to_standardized_NES.
#
# expected_columns: raw source headers (uppercase). Used for missing/unexpected checks.
# mandatory_nonzero_columns: columns that must never be null / empty in the raw file.
# mandatory_columns: columns that must exist but can be 0 or null in the raw file.
# timestamp_columns: {raw_col_name: spark_format_string}
# decimal_columns: {raw_col_name: decimal_type_string}
# integer_columns: [raw_col_name, ...]
# bigint_columns: [raw_col_name, ...]
# boolean_columns: [raw_col_name, ...]
# ---------------------------------------------------------------------------

NES_TIMESTAMP_FORMATS = [
    "M/d/yyyy h:mm:ss a",
    "dd/MM/yy H:mm"
]

FILE_CONFIGS = {
    "shortage": {
        "file_name_regex": r"^nes_shortage_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "NES_FUNCTION", "ISA_LINK_ID", "TRANSACTION_ID", "DENOMINATION",
            "NOTE_TYPE", "WITHDRAWAL_ENTRY_DATE", "SHORTAGE_DATE",
            "REGION_NAME", "REGION_ID", "FI_NAME", "FI_ID", "RDC_ID",
            "WITHDRAWAL_NOTE_COUNT", "SHORTAGE_NOTE_COUNT", "SHORTAGE_NOTE_VALUE"
        ],
        "mandatory_nonzero_columns": [
            "NES_FUNCTION", "ISA_LINK_ID", "TRANSACTION_ID", "DENOMINATION",
            "NOTE_TYPE", "WITHDRAWAL_ENTRY_DATE", "SHORTAGE_DATE",
            "REGION_NAME", "REGION_ID", "FI_NAME", "FI_ID", "RDC_ID",
            "WITHDRAWAL_NOTE_COUNT", "SHORTAGE_NOTE_COUNT", "SHORTAGE_NOTE_VALUE"
        ],
        "mandatory_columns": [],
        "timestamp_columns": {
            "WITHDRAWAL_ENTRY_DATE": NES_TIMESTAMP_FORMATS,
            "SHORTAGE_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "WITHDRAWAL_NOTE_COUNT": "decimal(10,0)",
            "SHORTAGE_NOTE_COUNT": "decimal(38,0)"
        },
        "integer_columns": ["SHORTAGE_NOTE_VALUE"],
        "bigint_columns": [],
        "boolean_columns": []
    },
    "deposits": {
        "file_name_regex": r"^nes_deposits_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "NES_FUNCTION", "DEPOSIT_ISA_LINK_ID", "TRANSACTION_ID",
            "CREATE_DATE", "LAST_UPDATE_DATE", "STATUS", "CREATE_USER",
            "RDC_NAME", "RDC_CODE", "NOTE_TYPE", "TOTAL_DEPOSIT_VALUE",
            "DENOMINATION", "NOTE_COUNT", "NOTE_VALUE", "SEAL_ID",
            "PARCEL_ID", "SECONDARY_SEAL_ID", "PARCEL_TOTAL_VALUE",
            "PARCEL_TOTAL_COUNT", "VAULT_MAX_CAP", "VAULT_CURRENT_CAP",
            "VAULT_AVAIL_CAP", "READY_TO_RELEASE_VALUE", "PROJECTED_POSITION"
        ],
        "mandatory_nonzero_columns": [
            "NES_FUNCTION", "DEPOSIT_ISA_LINK_ID", "TRANSACTION_ID", "CREATE_DATE",
            "STATUS", "CREATE_USER", "RDC_NAME", "RDC_CODE", "NOTE_TYPE",
            "TOTAL_DEPOSIT_VALUE", "DENOMINATION", "VAULT_MAX_CAP", "VAULT_CURRENT_CAP",
            "PROJECTED_POSITION", "NOTE_COUNT", "NOTE_VALUE", "VAULT_AVAIL_CAP"
        ],
        "mandatory_columns": [],
        "timestamp_columns": {
            "CREATE_DATE": NES_TIMESTAMP_FORMATS,
            "LAST_UPDATE_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "STATUS": "decimal(10,0)",
            "TOTAL_DEPOSIT_VALUE": "decimal(18,6)",
            "NOTE_COUNT": "decimal(10,0)",
            "NOTE_VALUE": "decimal(18,6)",
            "PARCEL_TOTAL_VALUE": "decimal(18,6)",
            "PARCEL_TOTAL_COUNT": "decimal(18,6)",
            "VAULT_MAX_CAP": "decimal(18,6)",
            "VAULT_CURRENT_CAP": "decimal(18,6)",
            "VAULT_AVAIL_CAP": "decimal(18,6)",
            "READY_TO_RELEASE_VALUE": "decimal(18,6)",
            "PROJECTED_POSITION": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "boolean_columns": []
    },
    "overcap": {
        "file_name_regex": r"^nes_overcap_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "NES_FUNCTION", "DEPOSIT_OVERCAP_BILLING_EVENT_ISA_LINK_ID",
            "DEPOSIT_REQUEST_DATE", "OVERCAP_DATE", "RDP", "RDC_ID",
            "REGION_NAME", "REGION_ID", "FI_NAME", "FI_ID",
            "DEPOSIT_DECLARED_AMOUNT", "OVERCAP_AMOUNT", "TRANSACTION_ID",
            "RDC_CAPACITY_AMOUNT", "DEPOSIT_TYPE"
        ],
        "mandatory_nonzero_columns": [
            "NES_FUNCTION", "DEPOSIT_OVERCAP_BILLING_EVENT_ISA_LINK_ID",
            "DEPOSIT_REQUEST_DATE", "OVERCAP_DATE", "RDP", "RDC_ID",
            "REGION_NAME", "REGION_ID", "FI_NAME", "FI_ID",
            "DEPOSIT_DECLARED_AMOUNT", "OVERCAP_AMOUNT", "TRANSACTION_ID",
            "RDC_CAPACITY_AMOUNT", "DEPOSIT_TYPE"
        ],
        "mandatory_columns": [],
        "timestamp_columns": {
            "DEPOSIT_REQUEST_DATE": NES_TIMESTAMP_FORMATS,
            "OVERCAP_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {},
        "integer_columns": [
            "DEPOSIT_DECLARED_AMOUNT", "OVERCAP_AMOUNT", "RDC_CAPACITY_AMOUNT"
        ],
        "bigint_columns": [],
        "boolean_columns": []
    },
    "rdc_profile": {
        "file_name_regex": r"^nes_rdc_profile_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "RDC_LINK", "SNAPSHOT_DATE", "RDC_NAME", "RDC_ID",
            "ACTIVE_STATUS_CODE", "ACTIVE_STATUS", "VAULT_MAX_CAP",
            "VAULT_OPT_CAP", "MIN_INV_TARGET", "INV_TARGET",
            "OPTIMISE_STATUS_CODE", "OPTIMIZATION_STATUS", "RDC_ADJACENT",
            "RDC_ADJACENT_ORDER", "ADJACENT_FLAG", "PROCESSING_AOC",
            "FI_CARRIER", "BOC_CARRIER"
        ],
        "mandatory_nonzero_columns": [
            "RDC_LINK", "SNAPSHOT_DATE", "RDC_NAME", "RDC_ID",
            "ACTIVE_STATUS_CODE", "ACTIVE_STATUS", "RDC_ADJACENT",
            "RDC_ADJACENT_ORDER", "ADJACENT_FLAG", "PROCESSING_AOC",
            "FI_CARRIER", "BOC_CARRIER"
        ],
        "mandatory_columns": [],
        "timestamp_columns": {
            "SNAPSHOT_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "VAULT_MAX_CAP": "decimal(18,6)",
            "VAULT_OPT_CAP": "decimal(18,6)",
            "MIN_INV_TARGET": "decimal(18,6)",
            "INV_TARGET": "decimal(18,6)",
            "RDC_ADJACENT_ORDER": "decimal(10,0)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "boolean_columns": [
            "ACTIVE_STATUS_CODE", "OPTIMISE_STATUS_CODE", "ADJACENT_FLAG"
        ]
    },
    "rdc_profile_details": {
        "file_name_regex": r"^nes_rdc_profile_details_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "RDC_LINK", "SNAPSHOT_DATE", "BNDS_ACTIVITY_CE", "RDC_NAME",
            "RDC_ID", "DENOMINATION", "NOTE_TYPE", "INV_TARGET_COUNT",
            "INV_TARGET_VALUE", "INV_MIN_TARGET_COUNT", "INV_MIN_TARGET_VALUE",
            "NET_WD_DEP_STATUS", "WD_DEP_FACTOR_VALUE"
        ],
        "mandatory_nonzero_columns": [
            "RDC_LINK", "SNAPSHOT_DATE", "BNDS_ACTIVITY_CE", "RDC_NAME",
            "RDC_ID", "DENOMINATION", "NOTE_TYPE"
        ],
        "mandatory_columns": [
            "INV_TARGET_COUNT",
            "INV_TARGET_VALUE", "INV_MIN_TARGET_COUNT", "INV_MIN_TARGET_VALUE",
            "NET_WD_DEP_STATUS", "WD_DEP_FACTOR_VALUE"
        ],
        "timestamp_columns": {
            "SNAPSHOT_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "INV_TARGET_VALUE": "decimal(18,6)",
            "INV_MIN_TARGET_VALUE": "decimal(18,6)",
            "WD_DEP_FACTOR_VALUE": "decimal(12,8)"
        },
        "integer_columns": ["INV_TARGET_COUNT", "INV_MIN_TARGET_COUNT"],
        "bigint_columns": [],
        "boolean_columns": []
    },
    "counterfeits": {
        "file_name_regex": r"^nes_counterfeits_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "NES_FUNCTION", "COUNTERFEIT_RECORD_LINK_ID", "DEPOSIT_ISA_LINK_ID",
            "RDC_ID", "REGION_NAME", "REGION_ID", "FI_NAME", "FI_ID",
            "DATE_FOUND", "TRANSACTION_ID", "CREATION_DATE", "DENOMINATION",
            "NOTE_TYPE", "NOTE_COUNT", "SERIAL_NUMBER", "SERIES_CODE",
            "ORIGINATING_CITY", "CLIENT", "OTHER_INFO", "USER_LAST_UPDATED",
            "GENUINE", "LAST_MODIFICATION_DATE"
        ],
        "mandatory_nonzero_columns": [
            "NES_FUNCTION", "COUNTERFEIT_RECORD_LINK_ID", "DEPOSIT_ISA_LINK_ID",
            "RDC_ID", "REGION_NAME", "REGION_ID", "FI_NAME", "FI_ID", "TRANSACTION_ID",
            "CREATION_DATE", "DENOMINATION", "NOTE_TYPE", "SERIAL_NUMBER", "SERIES_CODE",
            "USER_LAST_UPDATED", "GENUINE", "LAST_MODIFICATION_DATE"
        ],
        "mandatory_columns": ["NOTE_COUNT"],
        "timestamp_columns": {
            "DATE_FOUND": NES_TIMESTAMP_FORMATS,
            "CREATION_DATE": NES_TIMESTAMP_FORMATS,
            "LAST_MODIFICATION_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {},
        "integer_columns": ["NOTE_COUNT"],
        "bigint_columns": [],
        "boolean_columns": []
    },
    "withdrawal": {
        "file_name_regex": r"^nes_withdrawal_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "NES_FUNCTION", "TRANSACTION_ID", "CREATE_DATE", "REGION_NAME",
            "REGION_ID", "FI_NAME", "FI_ID", "REQUEST_RDC_NAME",
            "REQUEST_RDC_CODE", "ORDER_REQUEST_TYPE", "DENOMINATION",
            "NOTE_TYPE", "NOTE_COUNT", "NOTE_VALUE", "TOTAL_WITHDRAWAL_VALUE",
            "TX_STAGE", "SHIPMENT_DATE", "DELIVERY_DATE", "THREE_DAY_FEE_PENALTY"
        ],
        "mandatory_nonzero_columns": [
            "NES_FUNCTION", "TRANSACTION_ID", "CREATE_DATE", "REGION_NAME",
            "REGION_ID", "FI_NAME", "FI_ID", "REQUEST_RDC_NAME",
            "REQUEST_RDC_CODE", "ORDER_REQUEST_TYPE", "DENOMINATION",
            "NOTE_TYPE", "NOTE_COUNT", "NOTE_VALUE", "TOTAL_WITHDRAWAL_VALUE",
            "SHIPMENT_DATE", "DELIVERY_DATE", "THREE_DAY_FEE_PENALTY"
        ],
        "mandatory_columns": [],
        "timestamp_columns": {
            "CREATE_DATE": NES_TIMESTAMP_FORMATS,
            "SHIPMENT_DATE": NES_TIMESTAMP_FORMATS,
            "DELIVERY_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "NOTE_COUNT": "decimal(10,0)",
            "NOTE_VALUE": "decimal(29,6)",
            "TOTAL_WITHDRAWAL_VALUE": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "boolean_columns": []
    },
    "shipments": {
        "file_name_regex": r"^nes_shipments_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "NES_FUNCTION", "TRANSACTION_ID", "SPLIT", "SEAL", "STATUS", "ADJACENT",
            "SHIPPING_RDP_NAME", "SHIPPING_FI_NAME", "SHIPPING_RDP_ID", "SHIPPING_FI_ID",
            "RECEIVING_RDP_NAME", "RECEIVING_FI_NAME", "RECEIVING_RDP_ID", "RECEIVING_FI_ID",
            "TOTAL_SHIPPING_AMOUNT", "DENOMINATION", "NOTE_TYPE", "NOTE_COUNT", "NOTE_VALUE",
            "CARRIER_NAME", "CREATION_DATE", "DISPATCH_DATE", "DELIVERY_DATE", "CALLBACK_PARCEL_ID",
            "CALLBACK_PARCEL_COUNT", "CALLBACK_PARCEL_TOTAL", "CALLBACK_LAST_UPDATE_USER"
        ],
        "mandatory_nonzero_columns": [
            "NES_FUNCTION", "TRANSACTION_ID", "SHIPPING_RDP_NAME", "SHIPPING_FI_NAME", "SHIPPING_RDP_ID",
            "SHIPPING_FI_ID", "RECEIVING_RDP_NAME", "RECEIVING_FI_NAME", "RECEIVING_RDP_ID",
            "RECEIVING_FI_ID", "NOTE_TYPE", "CREATION_DATE", "DISPATCH_DATE", "DELIVERY_DATE"
        ],
        "mandatory_columns": [
            "TOTAL_SHIPPING_AMOUNT", "NOTE_COUNT", "NOTE_VALUE"
        ],
        "timestamp_columns": {
            "CREATION_DATE": NES_TIMESTAMP_FORMATS,
            "DISPATCH_DATE": NES_TIMESTAMP_FORMATS,
            "DELIVERY_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "RECEIVING_RDP_ID": "decimal(10,0)",
            "RECEIVING_FI_ID": "decimal(10,0)",
            "TOTAL_SHIPPING_AMOUNT": "decimal(19,4)",
            "NOTE_COUNT": "decimal(38,0)",
            "NOTE_VALUE": "decimal(19,4)",
            "CALLBACK_PARCEL_COUNT": "decimal(18,6)",
            "CALLBACK_PARCEL_TOTAL": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "boolean_columns": [
            "ADJACENT"
        ]
    },
    "inventory_snapshot": {
        "file_name_regex": r"^nes_inventory_snapshot_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "RDC_LINK", "RDC_NAME", "RDC_ID", "SNAPSHOT_DATE", "EOD_CREATION_DATE",
            "EOD_BUSINESS_DATE", "DENOMINATION", "INVENTORY_TYPE_NAME", "NOTE_COUNT",
            "NOTE_VALUE", "TRANSACTION_STATUS_CODE", "TRANSACTION_STATUS", "SEAL_ID",
            "TOTAL_VALUE", "PARCEL_ID", "PARCEL_VALUE", "TRANSACTION_ID",
            "USER_RDC_SETTLEMENT", "USER_AUTHORIZED_SETTLEMENT"
        ],
        "mandatory_nonzero_columns": [
            "RDC_LINK", "RDC_NAME", "RDC_ID", "SNAPSHOT_DATE", "EOD_CREATION_DATE",
            "EOD_BUSINESS_DATE", "INVENTORY_TYPE_NAME", "TRANSACTION_STATUS"
        ],
        "mandatory_columns": ["TRANSACTION_STATUS_CODE"],
        "timestamp_columns": {
            "SNAPSHOT_DATE": NES_TIMESTAMP_FORMATS,
            "EOD_CREATION_DATE": NES_TIMESTAMP_FORMATS,
            "EOD_BUSINESS_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "NOTE_VALUE": "decimal(29,6)",
            "TRANSACTION_STATUS_CODE": "decimal(10,0)",
            "TOTAL_VALUE": "decimal(24,6)",
            "PARCEL_VALUE": "decimal(18,0)"
        },
        "integer_columns": [
            "NOTE_COUNT"
        ],
        "bigint_columns": [],
        "boolean_columns": []
    },
    "non_conformances": {
        "file_name_regex": r"^nes_non_conformances_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(_\d{2})?\.csv$",
        "expected_columns": [
            "NES_FUNCTION", "CREATION_DATE", "TRANSACTION_ID", "SEAL_ID", "NC_RECORD_ID",
            "SHIPPING_RDP_NAME", "SHIPPING_RDP_ID", "SHIPPING_FI_NAME", "SHIPPING_FI_ID",
            "SHIPPING_RDC_ID", "RECEIVING_RDP_NAME", "RECEIVING_RDP_ID", "RECEIVING_FI_NAME",
            "RECEIVING_FI_ID", "RECEIVING_RDC_ID", "RECEPTION_DATE", "SHIPMENT_TYPE",
            "NOTE_TYPE", "TOTAL_SHIPMENT_VALUE", "DEPOSIT_TRANSACTION_ID", "PARCEL_ID",
            "NONCONFORMANCE_REASON", "NUMBER_OF_OCCURRENCES", "NC_REASON_CODE",
            "LAST_MODIFICATION_DATE"
        ],
        "mandatory_nonzero_columns": [
            "NES_FUNCTION", "CREATION_DATE", "TRANSACTION_ID", "NC_RECORD_ID",
            "SHIPPING_RDP_NAME", "SHIPPING_RDP_ID", "SHIPPING_RDC_ID", 
            "RECEIVING_RDP_NAME", "RECEIVING_RDP_ID", "RECEIVING_RDC_ID", 
            "RECEPTION_DATE", "SHIPMENT_TYPE", "NOTE_TYPE",
            "NONCONFORMANCE_REASON", "NUMBER_OF_OCCURRENCES", 
            "NC_REASON_CODE", "LAST_MODIFICATION_DATE"
        ],
        "mandatory_columns": ["TOTAL_SHIPMENT_VALUE"],
        "timestamp_columns": {
            "CREATION_DATE": NES_TIMESTAMP_FORMATS,
            "RECEPTION_DATE": NES_TIMESTAMP_FORMATS,
            "LAST_MODIFICATION_DATE": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "TOTAL_SHIPMENT_VALUE": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "boolean_columns": []
    }
}

# COMMAND ----------

# DBTITLE 1,Validation helper functions
# ===========================================================================
# Path / file discovery helpers
# ===========================================================================

def get_raw_base_path(config):
    """Builds the unity catalogue volume base path for a configured file type."""
    return (
        f"/Volumes/{env}_raw_cat/data_db/vol_internal/nes/{file_key}"
    )


def get_files_for_date(config):
    """
    Lists files in <raw_base_path>/<pull_date>/ that match the config file pattern.
    Returns a list of FileInfo objects from dbutils.fs.ls.
    """
    raw_base_path = get_raw_base_path(config)
    daily_path = f"{raw_base_path}/{pull_date}"
    print(f"  Scanning: {daily_path}")

    try:
        all_files = dbutils.fs.ls(daily_path)
    except Exception:
        raise Exception(
            f"Path not found or inaccessible: {daily_path}"
        )

    return [
        f for f in all_files
        if not f.isDir()
        and f.name.lower().endswith(".csv")
    ]


def read_raw_file(file_path):
    """Reads a raw NES CSV file with all columns as strings."""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", delimiter)
        .load(file_path)
    )


# ===========================================================================
# Check 1 — File size
# ===========================================================================

def check_file_size(file_info):
    """
    Returns True if the file has content, False if it is zero bytes.
    """
    if file_info.size == 0:
        raise Exception(
            f"Zero-byte file detected: {file_info.name}"
        )
    return True


# ===========================================================================
# Check 2 — Empty DataFrame (header row only, no data rows)
# ===========================================================================

def check_empty_dataframe(df, file_name):
    """
    Raises if the file has a valid header but zero data rows.
    A non-zero file size alone does not guarantee data exists.
    """
    if df.rdd.isEmpty():
        raise Exception(
            f"File contains no data rows (header only): {file_name}"
        )

    # remove rows where every column is null/blank
    data_rows = df.filter(
        ~(
            sum(
                (
                    col(c).isNull()
                    | (trim(col(c).cast("string")) == "")
                ).cast("int")
                for c in df.columns
            ) == len(df.columns)
        )
    )

    if data_rows.rdd.isEmpty():
        raise Exception(
            f"File contains no usable data rows: {file_name}"
        )

    print(f"  [OK] File contains data rows: {file_name}")


# ===========================================================================
# Check 3 — File naming convention
# ===========================================================================

def check_file_name(file_name, config):
    """
    Validates that the filename matches the expected NES naming convention:
      nes_<type>_yyyy_MM_dd_HH_mm.csv  (case-insensitive)
    Raises if the pattern does not match.
    """
    pattern = config["file_name_regex"]
    if not re.match(pattern, file_name):
        raise Exception(
            f"File name '{file_name}' does not match expected pattern '{pattern}'"
        )
    print(f"  [OK] File name convention valid: {file_name}")


# ===========================================================================
# Check 4 — Header content (missing / unexpected columns)
# ===========================================================================

def check_header_row(df, config, file_name):
    """
    Validates that the file contains exactly the expected set of columns.
    Reports missing and unexpected columns separately.
    """
    expected = {c.upper().strip() for c in config["expected_columns"]}
    actual = {c.upper().strip() for c in df.columns}

    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)

    errors = []
    if missing:
        errors.append(f"Missing columns:    {missing}")
    if unexpected:
        errors.append(f"Unexpected columns: {unexpected}")

    if errors:
        raise Exception(
            f"Header validation failed for {file_name}:\n  " + "\n  ".join(errors)
        )

    print(f"  [OK] Headers valid for {file_name}")


# ===========================================================================
# Check 5 — Mandatory columns
# ===========================================================================

def check_mandatory_columns(df, config, file_name):
    """
    Supports two types of mandatory columns:

    mandatory_columns
        - cannot be null
        - cannot be empty string
        - 0 is allowed

    mandatory_nonzero_columns
        - cannot be null
        - cannot be empty string
        - cannot be 0
    """

    mandatory_columns = config.get("mandatory_columns", [])
    mandatory_nonzero_columns = config.get("mandatory_nonzero_columns", [])

    if not mandatory_columns and not mandatory_nonzero_columns:
        print(f"  [SKIP] No mandatory columns configured for {file_name}")
        return

    df_upper = df.toDF(*[c.upper().strip() for c in df.columns])

    errors = []

    # -------------------------------------------------------
    # Mandatory (null / blank not allowed, 0 allowed)
    # -------------------------------------------------------
    for col_name in mandatory_columns:
        col_upper = col_name.upper().strip()

        if col_upper not in df_upper.columns:
            continue

        bad_count = (
            df_upper.filter(
                col(col_upper).isNull() | (trim(col(col_upper)) == "")
            ).count()
        )

        if bad_count > 0:
            errors.append(
                f"{col_upper}: {bad_count} null/empty row(s)"
            )

    # -------------------------------------------------------
    # Mandatory non-zero (null / blank / 0 not allowed)
    # -------------------------------------------------------
    for col_name in mandatory_nonzero_columns:
        col_upper = col_name.upper().strip()

        if col_upper not in df_upper.columns:
            continue

        bad_count = (
            df_upper.filter(
                col(col_upper).isNull()
                | (trim(col(col_upper)) == "")
                | (trim(col(col_upper)) == "0")
                | (trim(col(col_upper)) == "0.0")
                | (trim(col(col_upper)) == "0.00")
            ).count()
        )

        if bad_count > 0:
            errors.append(
                f"{col_upper}: {bad_count} null/empty/zero row(s)"
            )

    if errors:
        raise Exception(
            f"Mandatory column violations in {file_name}:\n  "
            + "\n  ".join(errors)
        )

    print(f"  [OK] Mandatory columns verified for {file_name}")


# ===========================================================================
# Check 6 — Timestamp format
# ===========================================================================

def check_timestamp_formats(df, config, file_name):
    """
    Validates timestamp columns against one or more allowed formats.
    A value is considered valid if it can be parsed by at least one format.
    """
    df_upper = df.toDF(*[c.upper().strip() for c in df.columns])
    errors = []

    for col_name, ts_formats in config.get("timestamp_columns", {}).items():
        col_upper = col_name.upper().strip()
        if col_upper not in df_upper.columns:
            continue

        if isinstance(ts_formats, str):
            ts_formats = [ts_formats]

        parsed_expr = coalesce(
            *[to_timestamp(col(col_upper), fmt) for fmt in ts_formats]
        )

        bad_count = (
            df_upper.filter(
                col(col_upper).isNotNull()
                & (trim(col(col_upper)) != "")
                & parsed_expr.isNull()
            ).count()
        )

        if bad_count > 0:
            errors.append(
                f"{col_upper}: {bad_count} value(s) do not match any allowed formats {ts_formats}"
            )

    if errors:
        raise Exception(
            f"Timestamp format violations in {file_name}:\n  " + "\n  ".join(errors)
        )

    print(f"  [OK] Timestamp formats verified for {file_name}")


# ===========================================================================
# Check 7 — Numeric castability (decimal / integer / bigint)
# ===========================================================================

def check_numeric_castability(df, config, file_name):
    """
    Mirrors apply_casts() in 02_raw_to_standardized_NES.
    For each decimal / integer / bigint column, verifies that every non-null
    value can be cast without producing a null (i.e. no silent data loss).
    Raises listing all offending columns and unparseable counts.
    """
    df_upper = df.toDF(*[c.upper().strip() for c in df.columns])
    errors = []

    # Decimal columns
    for col_name, decimal_type in config.get("decimal_columns", {}).items():
        col_upper = col_name.upper().strip()
        if col_upper not in df_upper.columns:
            continue
        bad_count = df_upper.filter(
            col(col_upper).isNotNull()
            & (trim(col(col_upper)) != "")
            & col(col_upper).cast(decimal_type).isNull()
        ).count()
        if bad_count > 0:
            errors.append(
                f"{col_upper}: {bad_count} value(s) cannot be cast to {decimal_type}"
            )

    # Integer columns
    for col_name in config.get("integer_columns", []):
        col_upper = col_name.upper().strip()
        if col_upper not in df_upper.columns:
            continue
        bad_count = df_upper.filter(
            col(col_upper).isNotNull()
            & (trim(col(col_upper)) != "")
            & col(col_upper).cast("integer").isNull()
        ).count()
        if bad_count > 0:
            errors.append(f"{col_upper}: {bad_count} value(s) cannot be cast to integer")

    # Bigint columns
    for col_name in config.get("bigint_columns", []):
        col_upper = col_name.upper().strip()
        if col_upper not in df_upper.columns:
            continue
        bad_count = df_upper.filter(
            col(col_upper).isNotNull()
            & (trim(col(col_upper)) != "")
            & col(col_upper).cast("bigint").isNull()
        ).count()
        if bad_count > 0:
            errors.append(f"{col_upper}: {bad_count} value(s) cannot be cast to bigint")

    if errors:
        raise Exception(
            f"Numeric castability violations in {file_name}:\n  " + "\n  ".join(errors)
        )

    print(f"  [OK] Numeric castability verified for {file_name}")


# ===========================================================================
# Check 8 — Boolean castability
# ===========================================================================

# Spark accepts these raw string values as valid booleans.
_VALID_BOOLEAN_VALUES = {"true", "false", "1", "0", "yes", "no"}

def check_boolean_castability(df, config, file_name):
    """
    For each boolean column, verifies that every non-null value is one of the
    accepted boolean representations Spark understands.
    Raises listing all offending columns and invalid value counts.
    """
    boolean_columns = config.get("boolean_columns", [])
    if not boolean_columns:
        return

    df_upper = df.toDF(*[c.upper().strip() for c in df.columns])
    errors = []

    for col_name in boolean_columns:
        col_upper = col_name.upper().strip()
        if col_upper not in df_upper.columns:
            continue
        bad_count = df_upper.filter(
            col(col_upper).isNotNull()
            & (trim(col(col_upper)) != "")
            & col(col_upper).cast("boolean").isNull()
        ).count()
        if bad_count > 0:
            errors.append(
                f"{col_upper}: {bad_count} value(s) cannot be cast to boolean "
                f"(accepted: {sorted(_VALID_BOOLEAN_VALUES)})"
            )

    if errors:
        raise Exception(
            f"Boolean castability violations in {file_name}:\n  " + "\n  ".join(errors)
        )

    print(f"  [OK] Boolean castability verified for {file_name}")


# ===========================================================================
# Check 9 — Duplicate rows (warning only)
# ===========================================================================

def check_duplicates(df, file_name):
    """
    Counts fully duplicate rows (same as drop_standard_duplicates() in
    02_raw_to_standardized_NES when no merge_key_columns are configured).
    Raises an exception.
    Returns the total, distinct, and duplicate count.
    """
    total_count = df.count()
    distinct_count = df.distinct().count()
    dup_count = total_count - distinct_count

    print(
        f"  [INFO] Duplicate check completed. "
        f"Total={total_count:,}, Distinct={distinct_count:,}, Duplicates={dup_count:,}"
    )

    if dup_count > 0:
         raise Exception(
            f"{dup_count} duplicate row(s) found in {file_name}.\n  "
        )
    else:
        print(f"  [OK] No duplicate rows found in {file_name}")

    return dup_count


# ===========================================================================
# Main validation driver for one file
# ===========================================================================

def validate_file(file_info, config):
    """
    Runs all checks against a single file in order.
    Returns a dict with the result and any warning counts.
    """
    print(f"\n--- Validating: {file_info.name}  ({file_info.size:,} bytes) ---")

    result = {"file": file_info.name, "status": None, "row_count": 0, "duplicates": 0}

    # Check 1 — file size
    if not check_file_size(file_info):
        result["status"] = "zero_size"
        return result

    # Check 3 — naming convention (before reading, no Spark needed)
    check_file_name(file_info.name, config)

    df = read_raw_file(file_info.path)

    # Row count — logged unconditionally for support and investigations
    row_count = df.count()
    print(f"  [INFO] Row count: {row_count:,}")
    result["row_count"] = row_count

    # Check 2 — empty dataframe
    check_empty_dataframe(df, file_info.name)

    # Check 4 — header content (missing / unexpected, no order check)
    check_header_row(df, config, file_info.name)

    # Check 5 — mandatory columns
    check_mandatory_columns(df, config, file_info.name)

    # Check 6 — timestamp formats
    check_timestamp_formats(df, config, file_info.name)

    # Check 7 — numeric castability
    check_numeric_castability(df, config, file_info.name)

    # Check 8 — boolean castability
    check_boolean_castability(df, config, file_info.name)

    # Check 9 — duplicates (warning only)
    result["duplicates"] = check_duplicates(df, file_info.name)

    result["status"] = "valid"
    return result

# COMMAND ----------

# DBTITLE 1,Run validation
# ============================================================
# Select configs to validate
# ============================================================
if file_key == "--please-select--":
    raise Exception("Please select a file type.")

if file_key not in FILE_CONFIGS:
    raise Exception(
        f"Unsupported file_key: {file_key}"
    )
selected_configs = [(file_key, FILE_CONFIGS[file_key])]


# ============================================================
# Validate each file type
# ============================================================

all_results   = []   # list of result dicts
has_zero_size = False
has_no_files  = False

for fk, config in selected_configs:
    print("\n" + "=" * 100)
    print(f"Validating file type: {fk}")
    print("=" * 100)

    files = get_files_for_date(config)

    # Check 10 — multiple files per type
    if len(files) > 1:
        print(
            f"  [WARN] {len(files)} files found for '{fk}' on {pull_date}. "
            f"Expected 1. All will be validated and unioned during ingestion."
        )

    if len(files) == 0:
        print(f"  [WARN] No files found for '{fk}' under pull_date={pull_date}")
        all_results.append({"file_key": fk, "file": "<none>", "status": "no_files", "row_count": 0, "duplicates": 0})
        has_no_files = True
        continue

    for file_info in files:
        result = validate_file(file_info, config)
        result["file_key"] = fk
        all_results.append(result)

        if result["status"] == "zero_size":
            has_zero_size = True


# ============================================================
# Summary
# ============================================================

print("\n" + "=" * 100)
print("VALIDATION SUMMARY")
print("=" * 100)
print(f"{'FILE TYPE':<25} {'FILE NAME':<50} {'STATUS':<12} {'ROWS':>10}  {'DUPLICATES'}")
print("-" * 100)
for r in all_results:
    dup_str = str(r['duplicates']) if r['duplicates'] > 0 else "-"
    row_str = f"{r.get('row_count', 0):,}" if r.get('row_count', 0) > 0 else "-"
    print(f"{r['file_key']:<25} {r['file']:<50} {r['status']:<12} {row_str:>10}  {dup_str}")

print("\n")

if has_zero_size:
    print("[EXIT] zero_size — one or more files were empty.")
    dbutils.notebook.exit("zero_size")
elif has_no_files:
    print("[EXIT] no_files — one or more file types had no files for this date.")
    dbutils.notebook.exit("no_files")
else:
    print("[EXIT] valid — all files passed validation.")
    dbutils.notebook.exit("valid")
