# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # NES Raw-to-Standardized Ingestion Notebook
# MAGIC
# MAGIC ## Supports
# MAGIC
# MAGIC - **Daily load**: loads one `pull_date` folder.
# MAGIC - **Full load**: loads all date-separated folders under each raw path.
# MAGIC - **File selection**: supports loading either all files or one selected file type using `file_key`.

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit, to_date, current_timestamp, trim, when, substring, to_timestamp, coalesce
from pyspark.sql.types import *
import delta

# COMMAND ----------

# MAGIC %run currency/common/utils

# COMMAND ----------

# ============================================================
# 1. Widgets
# ============================================================

dbutils.widgets.text("env", "", "env")
dbutils.widgets.text("pull_date", "", "pull_date")  # yyyy-MM-dd, required for daily

dbutils.widgets.dropdown("load_type", "daily", ["daily", "full"])

dbutils.widgets.dropdown(
    "file_key",
    "all",
    [
        "all",
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
load_type = dbutils.widgets.get("load_type").lower()
file_key = dbutils.widgets.get("file_key").lower()
delimiter = ","

print(f"env: {env}")
print(f"pull_date: {pull_date}")
print(f"load_type: {load_type}")
print(f"file_key: {file_key}")
print(f"delimiter: {delimiter}")


# COMMAND ----------

# ============================================================
# 2. Resolve environment, storage, and Unity Catalog
# ============================================================

if len(env.strip()) == 0:
    env = get_env()

unity_catalog_name = get_unity_catalog_name(env)

print(f"resolved env: {env}")
print(f"unity_catalog_name: {unity_catalog_name}")


# COMMAND ----------

# ============================================================
# 3. File configurations
# ============================================================
NES_TIMESTAMP_FORMATS = [
    "M/d/yyyy h:mm:ss a",
    "dd/MM/yy H:mm"
]

FILE_CONFIGS = {
    "shortage": {
        "file_patterns": ["nes_shortage"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_shortage",
        "column_map": {
            "NES_FUNCTION": "nes_function",
            "ISA_LINK_ID": "isa_link_id",
            "TRANSACTION_ID": "transaction_id",
            "DENOMINATION": "denomination",
            "NOTE_TYPE": "note_type",
            "WITHDRAWAL_ENTRY_DATE": "withdrawal_entry_date",
            "SHORTAGE_DATE": "shortage_date",
            "REGION_NAME": "region_name",
            "REGION_ID": "region_id",
            "FI_NAME": "fi_name",
            "FI_ID": "fi_id",
            "RDC_ID": "rdc_id",
            "WITHDRAWAL_NOTE_COUNT": "withdrawal_note_count",
            "SHORTAGE_NOTE_COUNT": "shortage_note_count",
            "SHORTAGE_NOTE_VALUE": "shortage_note_value"
        },
        "date_columns": {},
        "timestamp_columns": {
            "withdrawal_entry_date": NES_TIMESTAMP_FORMATS,
            "shortage_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "withdrawal_note_count": "decimal(10,0)",
            "shortage_note_count": "decimal(38,0)"
        },
        "integer_columns": [
            "shortage_note_value"
        ],
        "bigint_columns": [],
        "string_columns": [
            "nes_function",
            "isa_link_id",
            "transaction_id",
            "denomination",
            "note_type",
            "region_name",
            "region_id",
            "fi_name",
            "fi_id",
            "rdc_id"
        ]
    },
    "deposits": {
        "file_patterns": ["nes_deposits"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_deposits",
        "column_map": {
            "NES_FUNCTION": "nes_function",
            "DEPOSIT_ISA_LINK_ID": "deposit_isa_link_id",
            "TRANSACTION_ID": "transaction_id",
            "CREATE_DATE": "create_date",
            "LAST_UPDATE_DATE": "last_update_date",
            "STATUS": "status",
            "CREATE_USER": "create_user",
            "RDC_NAME": "rdc_name",
            "RDC_CODE": "rdc_code",
            "NOTE_TYPE": "note_type",
            "TOTAL_DEPOSIT_VALUE": "total_deposit_value",
            "DENOMINATION": "denomination",
            "NOTE_COUNT": "note_count",
            "NOTE_VALUE": "note_value",
            "SEAL_ID": "seal_id",
            "PARCEL_ID": "parcel_id",
            "SECONDARY_SEAL_ID": "secondary_seal_id",
            "PARCEL_TOTAL_VALUE": "parcel_total_value",
            "PARCEL_TOTAL_COUNT": "parcel_total_count",
            "VAULT_MAX_CAP": "vault_max_cap",
            "VAULT_CURRENT_CAP": "vault_current_cap",
            "VAULT_AVAIL_CAP": "vault_avail_cap",
            "READY_TO_RELEASE_VALUE": "ready_to_release_value",
            "PROJECTED_POSITION": "projected_position"
        },
        "date_columns": {},
        "timestamp_columns": {
            "create_date": NES_TIMESTAMP_FORMATS,
            "last_update_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "status": "decimal(10,0)",
            "total_deposit_value": "decimal(18,6)",
            "note_count": "decimal(10,0)",
            "note_value": "decimal(18,6)",
            "parcel_total_value": "decimal(18,6)",
            "parcel_total_count": "decimal(18,6)",
            "vault_max_cap": "decimal(18,6)",
            "vault_current_cap": "decimal(18,6)",
            "vault_avail_cap": "decimal(18,6)",
            "ready_to_release_value": "decimal(18,6)",
            "projected_position": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "string_columns": [
            "nes_function",
            "deposit_isa_link_id",
            "transaction_id",
            "create_user",
            "rdc_name",
            "rdc_code",
            "note_type",
            "denomination",
            "seal_id",
            "parcel_id",
            "secondary_seal_id"
        ]
    },
    "overcap": {
        "file_patterns": ["nes_overcap"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_overcap",
        "column_map": {
            "NES_FUNCTION": "nes_function",
            "DEPOSIT_OVERCAP_BILLING_EVENT_ISA_LINK_ID": "deposit_overcap_billing_event_isa_link_id",
            "DEPOSIT_REQUEST_DATE": "deposit_request_date",
            "OVERCAP_DATE": "overcap_date",
            "RDP": "rdp",
            "RDC_ID": "rdc_id",
            "REGION_NAME": "region_name",
            "REGION_ID": "region_id",
            "FI_NAME": "fi_name",
            "FI_ID": "fi_id",
            "DEPOSIT_DECLARED_AMOUNT": "deposit_declared_amount",
            "OVERCAP_AMOUNT": "overcap_amount",
            "TRANSACTION_ID": "transaction_id",
            "RDC_CAPACITY_AMOUNT": "rdc_capacity_amount",
            "DEPOSIT_TYPE": "deposit_type"
        },
        "date_columns": {},
        "timestamp_columns": {
            "deposit_request_date": NES_TIMESTAMP_FORMATS,
            "overcap_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {},
        "integer_columns": [
            "deposit_declared_amount",
            "overcap_amount",
            "rdc_capacity_amount"
        ],
        "bigint_columns": [],
        "string_columns": [
            "nes_function",
            "deposit_overcap_billing_event_isa_link_id",
            "rdp",
            "rdc_id",
            "region_name",
            "region_id",
            "fi_name",
            "fi_id",
            "transaction_id",
            "deposit_type"
        ]
    },
    "rdc_profile": {
        "file_patterns": ["nes_rdc_profile"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_rdc_profile",
        "column_map": {
            "RDC_LINK": "rdc_link",
            "SNAPSHOT_DATE": "snapshot_date",
            "RDC_NAME": "rdc_name",
            "RDC_ID": "rdc_id",
            "ACTIVE_STATUS_CODE": "active_status_code",
            "ACTIVE_STATUS": "active_status",
            "VAULT_MAX_CAP": "vault_max_cap",
            "VAULT_OPT_CAP": "vault_opt_cap",
            "MIN_INV_TARGET": "min_inv_target",
            "INV_TARGET": "inv_target",
            "OPTIMISE_STATUS_CODE": "optimise_status_code",
            "OPTIMIZATION_STATUS": "optimization_status",
            "RDC_ADJACENT": "rdc_adjacent",
            "RDC_ADJACENT_ORDER": "rdc_adjacent_order",
            "ADJACENT_FLAG": "adjacent_flag",
            "PROCESSING_AOC": "processing_aoc",
            "FI_CARRIER": "fi_carrier",
            "BOC_CARRIER": "boc_carrier"
        },
        "date_columns": {},
        "timestamp_columns": {
            "snapshot_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "vault_max_cap": "decimal(18,6)",
            "vault_opt_cap": "decimal(18,6)",
            "min_inv_target": "decimal(18,6)",
            "inv_target": "decimal(18,6)",
            "rdc_adjacent_order": "decimal(10,0)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "string_columns": [
            "rdc_link",
            "rdc_name",
            "rdc_id",
            "active_status",
            "optimization_status",
            "rdc_adjacent",
            "processing_aoc",
            "fi_carrier",
            "boc_carrier"
        ],
        "boolean_columns": [
            "active_status_code",
            "optimise_status_code",
            "adjacent_flag"
        ]
    },
    "rdc_profile_details": {
        "file_patterns": ["nes_rdc_profile_details"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_rdc_profile_details",
        "column_map": {
            "RDC_LINK": "rdc_link",
            "SNAPSHOT_DATE": "snapshot_date",
            "BNDS_ACTIVITY_CE": "bnds_activity_ce",
            "RDC_NAME": "rdc_name",
            "RDC_ID": "rdc_id",
            "DENOMINATION": "denomination",
            "NOTE_TYPE": "note_type",
            "INV_TARGET_COUNT": "inv_target_count",
            "INV_TARGET_VALUE": "inv_target_value",
            "INV_MIN_TARGET_COUNT": "inv_min_target_count",
            "INV_MIN_TARGET_VALUE": "inv_min_target_value",
            "NET_WD_DEP_STATUS": "net_wd_dep_status",
            "WD_DEP_FACTOR_VALUE": "wd_dep_factor_value"
        },
        "date_columns": {},
        "timestamp_columns": {
            "snapshot_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "inv_target_value": "decimal(18,6)",
            "inv_min_target_value": "decimal(18,6)",
            "wd_dep_factor_value": "decimal(12,8)"
        },
        "integer_columns": [
            "inv_target_count",
            "inv_min_target_count"
        ],
        "bigint_columns": [],
        "string_columns": [
            "rdc_link",
            "bnds_activity_ce",
            "rdc_name",
            "rdc_id",
            "denomination",
            "note_type",
            "net_wd_dep_status"
        ]
    },
    "counterfeits": {
        "file_patterns": ["nes_counterfeits"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_counterfeits",
        "column_map": {
            "NES_FUNCTION": "nes_function",
            "COUNTERFEIT_RECORD_LINK_ID": "counterfeit_record_link_id",
            "DEPOSIT_ISA_LINK_ID": "deposit_isa_link_id",
            "RDC_ID": "rdc_id",
            "REGION_NAME": "region_name",
            "REGION_ID": "region_id",
            "FI_NAME": "fi_name",
            "FI_ID": "fi_id",
            "DATE_FOUND": "date_found",
            "TRANSACTION_ID": "transaction_id",
            "CREATION_DATE": "creation_date",
            "DENOMINATION": "denomination",
            "NOTE_TYPE": "note_type",
            "NOTE_COUNT": "note_count",
            "SERIAL_NUMBER": "serial_number",
            "SERIES_CODE": "series_code",
            "ORIGINATING_CITY": "originating_city",
            "CLIENT": "client",
            "OTHER_INFO": "other_info",
            "USER_LAST_UPDATED": "user_last_updated",
            "GENUINE": "genuine",
            "LAST_MODIFICATION_DATE": "last_modification_date"
        },
        "date_columns": {},
        "timestamp_columns": {
            "date_found": NES_TIMESTAMP_FORMATS,
            "creation_date": NES_TIMESTAMP_FORMATS,
            "last_modification_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {},
        "integer_columns": [
            "note_count"
        ],
        "bigint_columns": [],
        "string_columns": [
            "nes_function",
            "counterfeit_record_link_id",
            "deposit_isa_link_id",
            "rdc_id",
            "region_name",
            "region_id",
            "fi_name",
            "fi_id",
            "transaction_id",
            "denomination",
            "note_type",
            "serial_number",
            "series_code",
            "originating_city",
            "client",
            "other_info",
            "user_last_updated",
            "genuine"
        ]
    },
    "withdrawal": {
        "file_patterns": ["nes_withdrawal"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_withdrawal",
        "column_map": {
            "NES_FUNCTION": "nes_function",
            "TRANSACTION_ID": "transaction_id",
            "CREATE_DATE": "create_date",
            "REGION_NAME": "region_name",
            "REGION_ID": "region_id",
            "FI_NAME": "fi_name",
            "FI_ID": "fi_id",
            "REQUEST_RDC_NAME": "request_rdc_name",
            "REQUEST_RDC_CODE": "request_rdc_code",
            "ORDER_REQUEST_TYPE": "order_request_type",
            "DENOMINATION": "denomination",
            "NOTE_TYPE": "note_type",
            "NOTE_COUNT": "note_count",
            "NOTE_VALUE": "note_value",
            "TOTAL_WITHDRAWAL_VALUE": "total_withdrawal_value",
            "TX_STAGE": "tx_stage",
            "SHIPMENT_DATE": "shipment_date",
            "DELIVERY_DATE": "delivery_date",
            "THREE_DAY_FEE_PENALTY": "three_day_fee_penalty"
        },
        "date_columns": {},
        "timestamp_columns": {
            "create_date": NES_TIMESTAMP_FORMATS,
            "shipment_date": NES_TIMESTAMP_FORMATS,
            "delivery_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "note_count": "decimal(10,0)",
            "note_value": "decimal(29,6)",
            "total_withdrawal_value": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "string_columns": [
            "nes_function",
            "transaction_id",
            "region_name",
            "region_id",
            "fi_name",
            "fi_id",
            "request_rdc_name",
            "request_rdc_code",
            "order_request_type",
            "denomination",
            "note_type",
            "tx_stage",
            "three_day_fee_penalty"
        ]
    },
    "shipments": {
        "file_patterns": ["nes_shipments"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_shipments",
        "column_map": {
            "NES_FUNCTION": "nes_function",
            "TRANSACTION_ID": "transaction_id",
            "SPLIT": "split",
            "SEAL": "seal",
            "STATUS": "status",
            "ADJACENT": "adjacent",
            "SHIPPING_RDP_NAME": "shipping_rdp_name",
            "SHIPPING_FI_NAME": "shipping_fi_name",
            "SHIPPING_RDP_ID": "shipping_rdp_id",
            "SHIPPING_FI_ID": "shipping_fi_id",
            "RECEIVING_RDP_NAME": "receiving_rdp_name",
            "RECEIVING_FI_NAME": "receiving_fi_name",
            "RECEIVING_RDP_ID": "receiving_rdp_id",
            "RECEIVING_FI_ID": "receiving_fi_id",
            "TOTAL_SHIPPING_AMOUNT": "total_shipping_amount",
            "DENOMINATION": "denomination",
            "NOTE_TYPE": "note_type",
            "NOTE_COUNT": "note_count",
            "NOTE_VALUE": "note_value",
            "CARRIER_NAME": "carrier_name",
            "CREATION_DATE": "creation_date",
            "DISPATCH_DATE": "dispatch_date",
            "DELIVERY_DATE": "delivery_date",
            "CALLBACK_PARCEL_ID": "callback_parcel_id",
            "CALLBACK_PARCEL_COUNT": "callback_parcel_count",
            "CALLBACK_PARCEL_TOTAL": "callback_parcel_total",
            "CALLBACK_LAST_UPDATE_USER": "callback_last_update_user"
        },
        "date_columns": {},
        "timestamp_columns": {
            "creation_date": NES_TIMESTAMP_FORMATS,
            "dispatch_date": NES_TIMESTAMP_FORMATS,
            "delivery_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "receiving_rdp_id": "decimal(10,0)",
            "receiving_fi_id": "decimal(10,0)",
            "total_shipping_amount": "decimal(19,4)",
            "note_count": "decimal(38,0)",
            "note_value": "decimal(19,4)",
            "callback_parcel_count": "decimal(18,6)",
            "callback_parcel_total": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "boolean_columns": [
            "adjacent"
        ],
        "string_columns": [
            "nes_function",
            "transaction_id",
            "split",
            "seal",
            "status",
            "shipping_rdp_name",
            "shipping_fi_name",
            "shipping_rdp_id",
            "shipping_fi_id",
            "receiving_rdp_name",
            "receiving_fi_name",
            "denomination",
            "note_type",
            "carrier_name",
            "callback_parcel_id",
            "callback_last_update_user"
        ]
    },
    "inventory_snapshot": {
        "file_patterns": ["nes_inventory_snapshot"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_inventory_snapshot",
        "column_map": {
            "RDC_LINK": "rdc_link",
            "RDC_NAME": "rdc_name",
            "RDC_ID": "rdc_id",
            "SNAPSHOT_DATE": "snapshot_date",
            "EOD_CREATION_DATE": "eod_creation_date",
            "EOD_BUSINESS_DATE": "eod_business_date",
            "DENOMINATION": "denomination",
            "INVENTORY_TYPE_NAME": "inventory_type_name",
            "NOTE_COUNT": "note_count",
            "NOTE_VALUE": "note_value",
            "TRANSACTION_STATUS_CODE": "transaction_status_code",
            "TRANSACTION_STATUS": "transaction_status",
            "SEAL_ID": "seal_id",
            "TOTAL_VALUE": "total_value",
            "PARCEL_ID": "parcel_id",
            "PARCEL_VALUE": "parcel_value",
            "TRANSACTION_ID": "transaction_id",
            "USER_RDC_SETTLEMENT": "user_rdc_settlement",
            "USER_AUTHORIZED_SETTLEMENT": "user_authorized_settlement"
        },
        "date_columns": {},
        "timestamp_columns": {
            "snapshot_date": NES_TIMESTAMP_FORMATS,
            "eod_creation_date": NES_TIMESTAMP_FORMATS,
            "eod_business_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "note_value": "decimal(29,6)",
            "transaction_status_code": "decimal(10,0)",
            "total_value": "decimal(24,6)",
            "parcel_value": "decimal(18,0)"
        },
        "integer_columns": [
            "note_count"
        ],
        "bigint_columns": [],
        "boolean_columns": [],
        "string_columns": [
            "rdc_link",
            "rdc_name",
            "rdc_id",
            "denomination",
            "inventory_type_name",
            "transaction_status",
            "seal_id",
            "parcel_id",
            "transaction_id",
            "user_rdc_settlement",
            "user_authorized_settlement"
        ]
    },
    "non_conformances": {
        "file_patterns": ["nes_non_conformances"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_nes_non_conformances",
        "column_map": {
            "NES_FUNCTION": "nes_function",
            "CREATION_DATE": "creation_date",
            "TRANSACTION_ID": "transaction_id",
            "SEAL_ID": "seal_id",
            "NC_RECORD_ID": "nc_record_id",
            "SHIPPING_RDP_NAME": "shipping_rdp_name",
            "SHIPPING_RDP_ID": "shipping_rdp_id",
            "SHIPPING_FI_NAME": "shipping_fi_name",
            "SHIPPING_FI_ID": "shipping_fi_id",
            "SHIPPING_RDC_ID": "shipping_rdc_id",
            "RECEIVING_RDP_NAME": "receiving_rdp_name",
            "RECEIVING_RDP_ID": "receiving_rdp_id",
            "RECEIVING_FI_NAME": "receiving_fi_name",
            "RECEIVING_FI_ID": "receiving_fi_id",
            "RECEIVING_RDC_ID": "receiving_rdc_id",
            "RECEPTION_DATE": "reception_date",
            "SHIPMENT_TYPE": "shipment_type",
            "NOTE_TYPE": "note_type",
            "TOTAL_SHIPMENT_VALUE": "total_shipment_value",
            "DEPOSIT_TRANSACTION_ID": "deposit_transaction_id",
            "PARCEL_ID": "parcel_id",
            "NONCONFORMANCE_REASON": "nonconformance_reason",
            "NUMBER_OF_OCCURRENCES": "number_of_occurrences",
            "NC_REASON_CODE": "nc_reason_code",
            "LAST_MODIFICATION_DATE": "last_modification_date"
        },
        "date_columns": {},
        "timestamp_columns": {
            "creation_date": NES_TIMESTAMP_FORMATS,
            "reception_date": NES_TIMESTAMP_FORMATS,
            "last_modification_date": NES_TIMESTAMP_FORMATS
        },
        "decimal_columns": {
            "total_shipment_value": "decimal(18,6)"
        },
        "integer_columns": [],
        "bigint_columns": [],
        "boolean_columns": [],
        "string_columns": [
            "nes_function",
            "transaction_id",
            "seal_id",
            "nc_record_id",
            "shipping_rdp_name",
            "shipping_rdp_id",
            "shipping_fi_name",
            "shipping_fi_id",
            "shipping_rdc_id",
            "receiving_rdp_name",
            "receiving_rdp_id",
            "receiving_fi_name",
            "receiving_fi_id",
            "receiving_rdc_id",
            "shipment_type",
            "note_type",
            "deposit_transaction_id",
            "parcel_id",
            "nonconformance_reason",
            "number_of_occurrences",
            "nc_reason_code"
        ]
    }
}

# COMMAND ----------

# DBTITLE 1,Helper functions
# ============================================================
# 4. Helper functions
# ============================================================

def normalize_col_name(name):
    """
    Normalizes source file headers so minor spacing/case differences
    do not break the column mapping.
    """
    return " ".join(name.strip().upper().split())


def config_list(value):
    """
    Supports either list config values or legacy semicolon-delimited strings.
    """
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        return [x.strip() for x in value.split(";") if x.strip()]

    return []


def get_raw_base_path(config):
    """
    Builds the unity catalogue volume base path for a configured file type.
    """
    return (
        f"/Volumes/{env}_raw_cat/data_db/vol_internal/nes/{file_key}"
    )


def file_matches_config(file_name, config):
    """
    Checks whether a file belongs to the current file config based on filename pattern.
    """
    file_name_upper = file_name.upper()

    return any(
        pattern.upper() in file_name_upper
        for pattern in config["file_patterns"]
    )


def get_files_to_load(file_config_key, config):
    """
    Finds files to load.

    daily:
      scans one folder:
      <raw_base_bath>/<pull_date>/

    full:
      scans all date-separated folders:
      <raw_base_bath>/<date_folder>/
    """
    raw_base_path = get_raw_base_path(config)
    files_to_load = []

    print(f"Scanning raw path for {file_config_key}: {raw_base_path}")

    if load_type == "daily":
        if len(pull_date.strip()) == 0:
            raise Exception("pull_date is required when load_type = daily")

        daily_path = f"{raw_base_path}/{pull_date}"
        print(f"Daily path: {daily_path}")

        for file in dbutils.fs.ls(daily_path):
            if not file.isDir() and file_matches_config(file.name, config):
                files_to_load.append({
                    "file_key": file_config_key,
                    "file_path": file.path,
                    "audit_file_name": file.name,
                    "audit_file_date": pull_date,
                })

    elif load_type == "full":
        date_folders = dbutils.fs.ls(raw_base_path)

        for folder in date_folders:
            if folder.isDir():
                folder_date = folder.name.replace("/", "")
                print(f"Scanning date folder: {folder_date}")

                for file in dbutils.fs.ls(folder.path):
                    if not file.isDir() and file_matches_config(file.name, config):
                        files_to_load.append({
                            "file_key": file_config_key,
                            "file_path": file.path,
                            "audit_file_name": file.name,
                            "audit_file_date": folder_date,
                        })

    else:
        raise Exception(f"Unsupported load_type: {load_type}")

    return files_to_load


def read_raw_file(file_path):
    """
    Reads a raw delimited file.
    """
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", delimiter)
        .load(file_path)
    )


def standardize_columns(df, config):
    """
    Normalizes raw headers and renames them to target table columns.
    """
    for raw_col in df.columns:
        normalized_col = normalize_col_name(raw_col)

        if raw_col != normalized_col:
            df = df.withColumnRenamed(raw_col, normalized_col)

    normalized_column_map = {
        normalize_col_name(raw_col): target_col
        for raw_col, target_col in config["column_map"].items()
    }

    for raw_col, target_col in normalized_column_map.items():
        if raw_col in df.columns:
            df = df.withColumnRenamed(raw_col, target_col)
        else:
            raise Exception(f"Missing expected source column: {raw_col}")

    return df


def clean_values(df):
    """
    Matches standard function behavior:
      - trims all column values
      - converts empty strings to null
    """
    df = df.select([
        trim(col(c).cast("string")).alias(c)
        for c in df.columns
    ])

    df = df.select([
        when(col(c) == "", None).otherwise(col(c)).alias(c)
        for c in df.columns
    ])

    return df

def drop_all_null_rows(df, config):
    """
    Drops rows where every mapped source column is null
    (e.g. blank lines or rows of only delimiters/whitespace).
    Must run after clean_values so empty strings are already null,
    and before audit columns are added (those are never null).
    """
    data_columns = [
        c for c in config["column_map"].values()
        if c in df.columns
    ]

    return df.na.drop(how="all", subset=data_columns)


def drop_configured_columns(df, config):
    """
    Drops configured unwanted columns, if supplied.
    This is optional because select_target_columns also removes extras later.
    """
    drop_columns = config_list(config.get("drop_columns"))

    if len(drop_columns) > 0:
        df = df.drop(*drop_columns)

    return df


def apply_casts(df, config):
    """
    Applies configured string, date, decimal, float, integer, bigint,
    and timestamp casts.
    """
    for col_name in config_list(config.get("string_columns")):
        df = df.withColumn(col_name, col(col_name).cast("string"))

    for col_name, date_format in config.get("date_columns", {}).items():
        df = df.withColumn(
            col_name,
            to_date(to_timestamp(col(col_name), date_format))
        )

    for col_name, data_type in config.get("decimal_columns", {}).items():
        df = df.withColumn(col_name, col(col_name).cast(data_type))

    for col_name in config_list(config.get("float_columns")):
        df = df.withColumn(col_name, col(col_name).cast("float"))

    for col_name in config_list(config.get("integer_columns")):
        df = df.withColumn(col_name, col(col_name).cast("integer"))

    for col_name in config_list(config.get("boolean_columns")):
        df = df.withColumn(col_name, col(col_name).cast("boolean"))

    for col_name in config_list(config.get("bigint_columns")):
        df = df.withColumn(col_name, col(col_name).cast("bigint"))

    for col_name, timestamp_formats in config.get("timestamp_columns", {}).items():
        if isinstance(timestamp_formats, str):
            timestamp_formats = [timestamp_formats]
        parsed_exprs = [
            to_timestamp(col(col_name), fmt)
            for fmt in timestamp_formats
        ]
        df = df.withColumn(
            col_name,
            coalesce(*parsed_exprs)
        )

    return df


def add_audit_columns(df, config, file_info):
    """
    Adds standard audit/control columns.
    """
    df = (
        df
        .withColumn("audit_file_name", lit(file_info["audit_file_name"]))
        .withColumn(
            "audit_file_date",
            to_date(lit(file_info["audit_file_date"]), "yyyy-MM-dd")
        )
        .withColumn("audit_load_ts", current_timestamp())
    )

    return df


def select_target_columns(df, config):
    """
    Selects only columns expected by the target table, in target order.
    """
    target_columns = list(config["column_map"].values())

    target_columns.extend([
        "audit_file_name",
        "audit_file_date",
        "audit_load_ts",
    ])

    return df.select(*target_columns)


def drop_standard_duplicates(df, config):
    """
    Matches standard duplicate handling:
      - if merge_key_columns exists, dedupe by those columns
      - otherwise dedupe by full row
    """
    merge_key_columns = config_list(config.get("merge_key_columns"))

    if len(merge_key_columns) > 0:
        return df.dropDuplicates(merge_key_columns)

    return df.dropDuplicates()


def transform_file(df_raw, config, file_info):
    """
    Applies all transformations for one raw file.
    """
    try:
        df = standardize_columns(df_raw, config)
        df = clean_values(df)
        df = drop_all_null_rows(df, config)
        df = drop_configured_columns(df, config)
        df = apply_casts(df, config)
        df = add_audit_columns(df, config, file_info)
        df = select_target_columns(df, config)
        df = drop_standard_duplicates(df, config)

        return df

    except Exception as e:
        raise Exception("Error in standardizing raw dataframe: " + str(e))


def union_dataframes(df_list):
    """
    Unions multiple transformed files for the same target table.
    """
    if len(df_list) == 0:
        return None

    df_all = df_list[0]

    for df in df_list[1:]:
        df_all = df_all.unionByName(df)

    return df_all


def delete_existing_data(config):
    """
    Soft-key delete-before-append logic.

    daily:
      deletes rows for the selected audit_file_date.

    full:
      deletes all rows from the selected target table.
    """
    target_table = config["target_table"]
    dt = delta.DeltaTable.forName(spark, target_table)

    if load_type == "daily":
        print(
            f"Deleting existing data from {target_table} "
            f"where audit_file_date = {pull_date}"
        )

        dt.delete(
            col("audit_file_date") == to_date(lit(pull_date), "yyyy-MM-dd")
        )

    elif load_type == "full":
        print(f"Deleting all existing data from {target_table}")

        dt.delete()


def write_to_delta(df, config):
    """
    Appends transformed data to the target Delta table.

    audit_file_date acts as the soft key because delete_existing_data()
    removes the existing slice before this append runs.
    """
    target_table = config["target_table"]

    print(f"Writing data to {target_table}")

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )


def ingest_one_file_type(file_config_key, config):
    """
    Main ingestion driver for one file type.
    """
    print("=" * 100)
    print(f"Starting ingestion for file_key = {file_config_key}")
    print("=" * 100)

    files_to_load = get_files_to_load(file_config_key, config)

    print(f"Files found for {file_config_key}: {len(files_to_load)}")

    if len(files_to_load) == 0:
        print(f"No files found for {file_config_key}. Skipping.")
        return

    transformed_dfs = []

    for file_info in files_to_load:
        print(f"Reading file: {file_info['file_path']}")

        df_raw = read_raw_file(file_info["file_path"])

        if df_raw.rdd.isEmpty():
            print(f"File is empty. Skipping: {file_info['file_path']}")
            continue

        df_transformed = transform_file(df_raw, config, file_info)
        transformed_dfs.append(df_transformed)

    df_all = union_dataframes(transformed_dfs)

    if df_all is None or df_all.rdd.isEmpty():
        print(f"No non-empty data found for {file_config_key}. Nothing to write.")
        return

    print(f"Preview transformed data for {file_config_key}")
    df_all.limit(10).display()

    delete_existing_data(config)
    write_to_delta(df_all, config)

    print(f"Completed ingestion for file_key = {file_config_key}")
    

# COMMAND ----------

# ============================================================
# 5. Select file configs and run ingestion
# ============================================================

if file_key == "all":
    selected_configs = FILE_CONFIGS.items()
else:
    if file_key not in FILE_CONFIGS:
        raise Exception(f"Unsupported file_key: {file_key}")

    selected_configs = [(file_key, FILE_CONFIGS[file_key])]


for selected_file_key, selected_config in selected_configs:
    ingest_one_file_type(selected_file_key, selected_config)


print("Ingestion complete.")
