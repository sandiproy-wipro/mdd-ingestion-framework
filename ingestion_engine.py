# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata-Driven Batch Ingestion Framework (UC Volumes) — Run
# MAGIC
# MAGIC **Capabilities**
# MAGIC - Reads dataset metadata JSON files from a **UC Volume**
# MAGIC - Ingests **Delimited**, **Fixed-length**, or **JSON** files (batch)
# MAGIC - Applies **row-level validations**; invalid rows are **quarantined**
# MAGIC - Writes **Bronze / Silver / Quarantine** as Delta tables in UC
# MAGIC - Maintains an **audit log** and **file registry** for idempotency

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Parameters
# MAGIC - Set `METADATA_DIR` to the volume folder containing `*.json` metadata
# MAGIC - Choose whether to process `ALL` datasets or a single dataset

# COMMAND ----------

import json, uuid, datetime
from typing import Dict, Any, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# --------- Update these defaults ---------
CATALOG = "wdfe"
SCHEMA  = "mgmt"
VOLUME  = "root"
METADATA_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/metadata/"

# Widget controls
dbutils.widgets.dropdown("mode", "ALL", ["ALL", "ONE"], "Run mode")
dbutils.widgets.text("dataset", "", "Dataset (when mode=ONE)")
dbutils.widgets.dropdown("archive", "YES", ["YES", "NO"], "Archive processed files")

RUN_MODE = dbutils.widgets.get("mode")
DATASET_FILTER = dbutils.widgets.get("dataset").strip()
ARCHIVE_FILES = (dbutils.widgets.get("archive") == "YES")

print("METADATA_DIR:", METADATA_DIR)
print("RUN_MODE:", RUN_MODE, "DATASET_FILTER:", DATASET_FILTER, "ARCHIVE_FILES:", ARCHIVE_FILES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Metadata loading utilities

# COMMAND ----------

def list_json_files(dir_path: str) -> List[str]:
    files = dbutils.fs.ls(dir_path)
    return [f.path for f in files if f.path.lower().endswith(".json")]


def load_metadata(path: str) -> Dict[str, Any]:
    # For larger files, prefer dbutils.fs.open; metadata should remain small.
    raw = dbutils.fs.head(path, 1024 * 1024)
    return json.loads(raw)


def spark_type(type_str: str) -> T.DataType:
    t = type_str.lower().strip()
    if t == "string": return T.StringType()
    if t in ("int", "integer"): return T.IntegerType()
    if t == "bigint": return T.LongType()
    if t == "double": return T.DoubleType()
    if t == "float": return T.FloatType()
    if t == "boolean": return T.BooleanType()
    if t == "date": return T.DateType()
    if t == "timestamp": return T.TimestampType()
    if t.startswith("decimal"):
        inside = t[t.find("(")+1:t.find(")")]
        p, s = inside.split(",")
        return T.DecimalType(int(p), int(s))
    raise ValueError(f"Unsupported type: {type_str}")


def build_struct(schema_meta: List[Dict[str, Any]]) -> T.StructType:
    fields = []
    for c in schema_meta:
        fields.append(T.StructField(c["name"], spark_type(c["type"]), c.get("nullable", True)))
    return T.StructType(fields)


def fq_table(meta: Dict[str, Any], table_key: str) -> str:
    t = meta["target"]
    return f'{t["catalog"]}.{t["schema"]}.{t[table_key]}'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Readers: Delimited, JSON, Fixed-length

# COMMAND ----------

def read_delimited(meta: Dict[str, Any], file_path: str) -> DataFrame:
    schema = build_struct(meta["schema"])
    df = (spark.read.format("csv")
          .option("header", str(meta.get("header", True)).lower())
          .option("sep", meta.get("delimiter", ","))
          .option("encoding", meta.get("encoding", "UTF-8"))
          .option("multiLine", str(meta.get("multiline", False)).lower())
          .option("mode", meta.get("mode", "PERMISSIVE"))
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .schema(schema)
          .load(file_path))
    return df


def read_json(meta: Dict[str, Any], file_path: str) -> DataFrame:
    schema = build_struct(meta["schema"])
    df = (spark.read.format("json")
          .option("multiLine", str(meta.get("multiline", False)).lower())
          .schema(schema)
          .load(file_path))
    return df


def read_fixed(meta: Dict[str, Any], file_path: str) -> DataFrame:
    layout = meta["fixed_layout"]
    raw = spark.read.text(file_path).withColumnRenamed("value", "_line")

    cols = []
    for c in layout:
        start = int(c["start"]) - 1
        length = int(c["length"])
        expr = F.substring(F.col("_line"), start + 1, length)
        if c.get("trim", False):
            expr = F.trim(expr)
        cols.append(expr.alias(c["name"]))

    df = raw.select(F.col("_line").alias("_raw_line"), *cols)

    # Cast to required types
    for c in layout:
        name = c["name"]
        typ = c["type"].lower()
        if typ == "date" and c.get("format"):
            df = df.withColumn(name, F.to_date(F.col(name), c["format"]))
        elif typ == "timestamp" and c.get("format"):
            df = df.withColumn(name, F.to_timestamp(F.col(name), c["format"]))
        else:
            df = df.withColumn(name, F.col(name).cast(spark_type(c["type"])))

    return df


def read_by_format(meta: Dict[str, Any], file_path: str) -> DataFrame:
    fmt = meta["file_format"].lower()
    if fmt == "delimited":
        return read_delimited(meta, file_path)
    if fmt == "json":
        return read_json(meta, file_path)
    if fmt == "fixed":
        return read_fixed(meta, file_path)
    raise ValueError(f"Unsupported file_format: {fmt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Validation Engine (row-level) — quarantine failures
# MAGIC Supported rules:
# MAGIC - `not_null`
# MAGIC - `regex`
# MAGIC - `range` (min/max)
# MAGIC - `in` (allowed values)
# MAGIC - `expr` (Spark SQL expression)

# COMMAND ----------

def apply_validations(df: DataFrame, meta: Dict[str, Any]) -> DataFrame:
    """Return df with an _errors array<string> column."""
    rules = meta.get("validations", [])

    df2 = df.withColumn("_errors", F.array().cast("array<string>"))

    # enforce nullable=false from schema (for delimited/json)
    for c in meta.get("schema", []):
        if c.get("nullable", True) is False:
            colname = c["name"]
            df2 = df2.withColumn(
                "_errors",
                F.when(F.col(colname).isNull(),
                       F.array_union(F.col("_errors"), F.array(F.lit(f"{colname}:NOT_NULL"))))
                 .otherwise(F.col("_errors"))
            )

    # fixed-length nullable=false
    for c in meta.get("fixed_layout", []):
        if c.get("nullable", True) is False:
            colname = c["name"]
            df2 = df2.withColumn(
                "_errors",
                F.when(F.col(colname).isNull(),
                       F.array_union(F.col("_errors"), F.array(F.lit(f"{colname}:NOT_NULL"))))
                 .otherwise(F.col("_errors"))
            )

    # corrupt record for delimited
    if "_corrupt_record" in df2.columns:
        df2 = df2.withColumn(
            "_errors",
            F.when(F.col("_corrupt_record").isNotNull(),
                   F.array_union(F.col("_errors"), F.array(F.lit("_corrupt_record:PARSE_ERROR"))))
             .otherwise(F.col("_errors"))
        )

    for r in rules:
        colname = r.get("col")
        rule = r.get("rule")
        when_not_null = r.get("when_not_null", False)
        base = (F.col(colname).isNotNull()) if (colname and when_not_null) else F.lit(True)

        if rule == "not_null":
            cond = base & F.col(colname).isNull()
            msg = f"{colname}:NOT_NULL"

        elif rule == "regex":
            pattern = r["pattern"]
            cond = base & (~F.col(colname).rlike(pattern))
            msg = f"{colname}:REGEX"

        elif rule == "range":
            mn = r.get("min")
            mx = r.get("max")
            fail = F.lit(False)
            if mn is not None:
                fail = fail | (F.col(colname) < F.lit(mn))
            if mx is not None:
                fail = fail | (F.col(colname) > F.lit(mx))
            cond = base & fail
            msg = f"{colname}:RANGE"

        elif rule == "in":
            values = r["values"]
            cond = base & (~F.col(colname).isin(values))
            msg = f"{colname}:IN_SET"

        elif rule == "expr":
            expression = r["expression"]
            cond = base & (~F.expr(expression))
            msg = r.get("message", f"EXPR_FAIL:{expression}")

        else:
            raise ValueError(f"Unsupported validation rule: {rule}")

        df2 = df2.withColumn(
            "_errors",
            F.when(cond, F.array_union(F.col("_errors"), F.array(F.lit(msg))))
             .otherwise(F.col("_errors"))
        )

    return df2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Audit + File Registry utilities

# COMMAND ----------

def log_audit(run_id: str, dataset: str, file_path: str,
              record_count: int, valid_count: int, invalid_count: int,
              status: str, message: str = None, catalog: str = None, schema: str = None):
    cat = catalog or CATALOG
    sch = schema or SCHEMA
    audit_tbl = f"{cat}.{sch}.ingestion_audit"

    row = [(run_id, dataset, file_path, datetime.datetime.utcnow(),
            int(record_count), int(valid_count), int(invalid_count), status, message)]

    adf = spark.createDataFrame(row, schema="""
      run_id string, dataset string, file_path string, ingested_at timestamp,
      record_count long, valid_count long, invalid_count long, status string, message string
    """)
    adf.write.format("delta").mode("append").saveAsTable(audit_tbl)


def log_error(run_id: str, dataset: str, file_path: str,
              error_type: str, error_message: str, catalog: str = None, schema: str = None):
    cat = catalog or CATALOG
    sch = schema or SCHEMA
    err_tbl = f"{cat}.{sch}.ingestion_errors"

    row = [(run_id, dataset, file_path, datetime.datetime.utcnow(), error_type, error_message)]
    edf = spark.createDataFrame(row, schema="""
      run_id string, dataset string, file_path string, error_ts timestamp,
      error_type string, error_message string
    """)
    edf.write.format("delta").mode("append").saveAsTable(err_tbl)


def file_already_processed(dataset: str, file_path: str, catalog: str, schema: str) -> bool:
    reg_tbl = f"{catalog}.{schema}.ingestion_file_registry"
    q = f"""
      SELECT 1 FROM {reg_tbl}
      WHERE dataset = '{dataset}' AND file_path = '{file_path}' AND status = 'PROCESSED'
      LIMIT 1
    """
    return spark.sql(q).count() > 0


def register_file(dataset: str, file_path: str, file_name: str, file_size: int,
                  status: str, run_id: str = None, catalog: str = None, schema: str = None):
    cat = catalog or CATALOG
    sch = schema or SCHEMA
    reg_tbl = f"{cat}.{sch}.ingestion_file_registry"
    now = datetime.datetime.utcnow()

    row = [(dataset, file_path, file_name, int(file_size), now, now if status == "PROCESSED" else None,
            run_id, status)]

    rdf = spark.createDataFrame(row, schema="""
      dataset string, file_path string, file_name string, file_size long,
      discovered_at timestamp, processed_at timestamp, run_id string, status string
    """)

    rdf.write.format("delta").mode("append").saveAsTable(reg_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Delta Writers

# COMMAND ----------

def write_delta(df: DataFrame, table_name: str, mode: str = "append"):
    (df.write.format("delta")
       .mode(mode)
       .option("mergeSchema", "true")
       .saveAsTable(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Ingest a single file (Bronze → validate → Silver + Quarantine)

# COMMAND ----------

def ingest_one_file(meta: Dict[str, Any], file_info, archive_files: bool = True):
    """file_info is an object from dbutils.fs.ls()"""
    run_id = str(uuid.uuid4())
    dataset = meta["dataset"]
    target = meta["target"]
    catalog, schema = CATALOG, SCHEMA

    file_path = file_info.path
    file_name = file_info.name
    file_size = file_info.size

    bronze_tbl = fq_table(meta, "bronze_table")
    silver_tbl = fq_table(meta, "silver_table")
    quarantine_tbl = fq_table(meta, "quarantine_table")

    write_mode = meta.get("ingestion", {}).get("write_mode", "append")

    # idempotency
    if file_already_processed(dataset, file_path, catalog, schema):
        print(f"SKIP (already processed): {file_path}")
        return

    register_file(dataset, file_path, file_name, file_size, status="DISCOVERED", run_id=run_id, catalog=catalog, schema=schema)

    try:
        df = read_by_format(meta, file_path)

        # Bronze: raw-ish with lineage columns
        df_bronze = (df
                     .withColumn("_source_file", F.lit(file_path))
                     .withColumn("_ingest_ts", F.current_timestamp())
                     .withColumn("_run_id", F.lit(run_id)))

        write_delta(df_bronze, bronze_tbl, mode=write_mode)

        # Validate (row-level)
        df_v = apply_validations(df_bronze, meta)

        total = df_v.count()
        invalid_df = df_v.filter(F.size(F.col("_errors")) > 0)
        invalid = invalid_df.count()
        valid = total - invalid

        # Silver = valid only (drop _errors)
        valid_df = df_v.filter(F.size(F.col("_errors")) == 0).drop("_errors")

        # Optional dedup for Silver
        dedup_keys = meta.get("ingestion", {}).get("dedup_keys")
        dedup_order = meta.get("ingestion", {}).get("dedup_order_col")
        if dedup_keys and dedup_order and dedup_order in valid_df.columns:
            w = Window.partitionBy(*[F.col(k) for k in dedup_keys]).orderBy(F.col(dedup_order).desc_nulls_last())
            valid_df = valid_df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

        write_delta(valid_df, silver_tbl, mode="append")

        # Quarantine invalid rows (keep _errors)
        if invalid > 0:
            write_delta(invalid_df, quarantine_tbl, mode="append")

        log_audit(run_id, dataset, file_path, total, valid, invalid, "SUCCESS", None, catalog=catalog, schema=schema)

        # Mark processed
        register_file(dataset, file_path, file_name, file_size, status="PROCESSED", run_id=run_id, catalog=catalog, schema=schema)

        # Archive (optional)
        if archive_files and meta.get("archive_path"):
            arch = meta["archive_path"].rstrip("/") + "/"
            dbutils.fs.mkdirs(arch)
            dbutils.fs.mv(file_path, arch + f"{file_name}.{run_id}", True)
            print(f"Archived to: {arch}{file_name}.{run_id}")

    except Exception as e:
        msg = str(e)
        log_audit(run_id, dataset, file_path, 0, 0, 0, "FAILED", msg, catalog=catalog, schema=schema)
        log_error(run_id, dataset, file_path, "INGESTION_FAILURE", msg, catalog=catalog, schema=schema)
        register_file(dataset, file_path, file_name, file_size, status="FAILED", run_id=run_id, catalog=catalog, schema=schema)
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Driver: Process ALL metadata datasets or ONE dataset

# COMMAND ----------

def list_files(path: str):
    try:
        return [f for f in dbutils.fs.ls(path) if f.isFile()]
    except Exception as e:
        print(f"No files or path not found: {path} ({e})")
        return []


def run(metadata_dir: str, run_mode: str = "ALL", dataset_filter: str = "", archive_files: bool = True):
    meta_files = list_json_files(metadata_dir)
    if not meta_files:
        raise ValueError(f"No metadata JSON files found in {metadata_dir}")

    for mf in meta_files:
        meta = load_metadata(mf)
        dataset = meta.get("dataset")
        if run_mode == "ONE" and dataset_filter and dataset != dataset_filter:
            continue

        landing = meta["landing_path"]
        files = list_files(landing)

        print(f"\nDataset={dataset} | landing={landing} | files={len(files)}")
        for fi in files:
            print(f"  Ingesting: {fi.path}")
            ingest_one_file(meta, fi, archive_files=archive_files)

# Disable ANSI strict casting so that invalid values (e.g. empty strings) return NULL
_ansi_prev = spark.conf.get("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.sql.ansi.enabled", "false")
try:
    run(METADATA_DIR, RUN_MODE, DATASET_FILTER, ARCHIVE_FILES)
finally:
    spark.conf.set("spark.sql.ansi.enabled", _ansi_prev)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes / Extensions
# MAGIC - To add new validations, extend `apply_validations()`.
# MAGIC - To implement SCD2 or MERGE, replace Silver append with a MERGE routine.
# MAGIC - For very large files: avoid `.count()` twice; you can compute counts via single aggregation.
