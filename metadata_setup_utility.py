# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata-Driven Batch Ingestion (UC Volumes) — Setup & Metadata Spec
# MAGIC
# MAGIC **What this notebook does**
# MAGIC - Defines the **metadata contract** (JSON) stored in a **Unity Catalog Volume**
# MAGIC - Provides **example metadata files** for *Delimited*, *Fixed-length*, and *JSON*
# MAGIC - Creates optional **audit / registry tables** (Delta)
# MAGIC
# MAGIC **Assumptions (as requested)**
# MAGIC - Landing files are in **Unity Catalog Volumes only**
# MAGIC - **Batch ingestion only**
# MAGIC - **Validation failures are quarantined (row-level)**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Set your UC Volume paths
# MAGIC Update these to match your environment.

# COMMAND ----------

import json

# Editable parameters
CATALOG = "wdfe"
SCHEMA  = "mgmt"
VOLUME  = "root"

# Folder holding dataset metadata JSON files
METADATA_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/metadata/"

# Optional: default landing root (each dataset can override landing_path in its metadata)
LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/landing/"
ARCHIVE_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/archive/"

print("METADATA_DIR:", METADATA_DIR)
print("LANDING_ROOT:", LANDING_ROOT)
print("ARCHIVE_ROOT:", ARCHIVE_ROOT)

# COMMAND ----------

PROJECT_CATALOG = "samplecatalog"
PROJECT_SCHEMA  = "sampleschema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Metadata JSON Contract (per dataset)
# MAGIC Store **one JSON file per dataset** under `METADATA_DIR`.
# MAGIC
# MAGIC Minimum keys:
# MAGIC - `dataset`
# MAGIC - `file_format`: `delimited` | `fixed` | `json`
# MAGIC - `landing_path`
# MAGIC - `archive_path` (optional)
# MAGIC - `target`: catalog/schema/table names
# MAGIC - schema definition (`schema` for delimited/json; `fixed_layout` for fixed)
# MAGIC - `validations` (optional)
# MAGIC - `ingestion.write_mode` (optional, default `append`)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Example metadata: Delimited (CSV/TSV/pipe)

# COMMAND ----------

EXAMPLE_DELIMITED = {
  "dataset": "customer",
  "file_format": "delimited",
  "landing_path": f"{LANDING_ROOT}customer/",
  "archive_path": f"{ARCHIVE_ROOT}customer/",
  "delimiter": ",",
  "header": True,
  "encoding": "UTF-8",
  "multiline": False,
  "mode": "PERMISSIVE",
  "target": {
    "catalog": PROJECT_CATALOG,
    "schema": PROJECT_SCHEMA,
    "bronze_table": "customer_bronze",
    "silver_table": "customer_silver",
    "quarantine_table": "customer_quarantine"
  },
  "schema": [
    {"name": "customer_id", "type": "string", "nullable": False},
    {"name": "name", "type": "string", "nullable": False},
    {"name": "email", "type": "string", "nullable": True},
    {"name": "age", "type": "int", "nullable": True},
    {"name": "created_ts", "type": "timestamp", "nullable": True}
  ],
  "validations": [
    {"col": "email", "rule": "regex", "pattern": "^[^@]+@[^@]+\.[^@]+$", "when_not_null": True},
    {"col": "age", "rule": "range", "min": 0, "max": 120, "when_not_null": True}
  ],
  "ingestion": {
    "write_mode": "append",
    "dedup_keys": ["customer_id"],
    "dedup_order_col": "created_ts"
  }
}

print(json.dumps(EXAMPLE_DELIMITED, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Example metadata: Fixed-length

# COMMAND ----------

EXAMPLE_FIXED = {
  "dataset": "account",
  "file_format": "fixed",
  "landing_path": f"{LANDING_ROOT}account/",
  "archive_path": f"{ARCHIVE_ROOT}account/",
  "target": {
    "catalog": PROJECT_CATALOG,
    "schema": PROJECT_SCHEMA,
    "bronze_table": "account_bronze",
    "silver_table": "account_silver",
    "quarantine_table": "account_quarantine"
  },
  "fixed_layout": [
    {"name": "account_id", "start": 1, "length": 10, "type": "string", "nullable": False, "trim": True},
    {"name": "status", "start": 11, "length": 1, "type": "string", "nullable": False, "trim": True},
    {"name": "balance", "start": 12, "length": 12, "type": "decimal(18,2)", "nullable": True, "trim": True},
    {"name": "as_of_date", "start": 24, "length": 8, "type": "date", "nullable": True, "format": "yyyyMMdd", "trim": True}
  ],
  "validations": [
    {"col": "status", "rule": "in", "values": ["A", "I"]}
  ],
  "ingestion": {"write_mode": "append"}
}

print(json.dumps(EXAMPLE_FIXED, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Example metadata: JSON

# COMMAND ----------

EXAMPLE_JSON = {
  "dataset": "events",
  "file_format": "json",
  "landing_path": f"{LANDING_ROOT}events/",
  "archive_path": f"{ARCHIVE_ROOT}events/",
  "multiline": False,
  "target": {
    "catalog": PROJECT_CATALOG,
    "schema": PROJECT_SCHEMA,
    "bronze_table": "events_bronze",
    "silver_table": "events_silver",
    "quarantine_table": "events_quarantine"
  },
  "schema": [
    {"name": "event_id", "type": "string", "nullable": False},
    {"name": "event_type", "type": "string", "nullable": False},
    {"name": "event_ts", "type": "timestamp", "nullable": True},
    {"name": "payload", "type": "string", "nullable": True}
  ],
  "validations": [
    {"col": "event_id", "rule": "not_null"},
    {"col": "event_type", "rule": "not_null"}
  ],
  "ingestion": {"write_mode": "append"}
}

print(json.dumps(EXAMPLE_JSON, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Helpers: write example metadata files into the Volume (optional)

# COMMAND ----------

def mkdirs(path: str):
    try:
        dbutils.fs.mkdirs(path)
    except Exception as e:
        print("mkdirs failed:", path, e)


def write_json_to_volume(obj: dict, path: str, overwrite: bool = False):
    if not overwrite:
        try:
            dbutils.fs.head(path, 1)
            print(f"File exists (skip): {path}")
            return
        except:
            pass
    dbutils.fs.put(path, json.dumps(obj, indent=2), overwrite=True)
    print(f"Wrote: {path}")

mkdirs(METADATA_DIR)

# Uncomment to write example metadata files
write_json_to_volume(EXAMPLE_DELIMITED, f"{METADATA_DIR}customer.json", overwrite=False)
write_json_to_volume(EXAMPLE_FIXED,     f"{METADATA_DIR}account.json",  overwrite=False)
write_json_to_volume(EXAMPLE_JSON,      f"{METADATA_DIR}events.json",   overwrite=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) One-time Delta Tables: Audit + File Registry
# MAGIC These tables help with:
# MAGIC - run-level auditing
# MAGIC - preventing reprocessing of already ingested files (idempotency)

# COMMAND ----------

# Create audit + registry tables (safe to run multiple times)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ingestion_audit (
  run_id STRING,
  dataset STRING,
  file_path STRING,
  ingested_at TIMESTAMP,
  record_count BIGINT,
  valid_count BIGINT,
  invalid_count BIGINT,
  status STRING,
  message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ingestion_errors (
  run_id STRING,
  dataset STRING,
  file_path STRING,
  error_ts TIMESTAMP,
  error_type STRING,
  error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ingestion_file_registry (
  dataset STRING,
  file_path STRING,
  file_name STRING,
  file_size BIGINT,
  discovered_at TIMESTAMP,
  processed_at TIMESTAMP,
  run_id STRING,
  status STRING
) USING DELTA
""")

print("Audit + registry tables ensured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC Import and run Notebook **02_UC_Metadata_Driven_Ingestion_Framework** to ingest all datasets/files.
