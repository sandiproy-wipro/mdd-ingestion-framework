# Metadata‑Driven Batch Ingestion Framework for Databricks

# MDD Ingestion Framework

A **metadata‑driven ingestion framework** for onboarding **batch data feeds** into **Databricks Unity Catalog**, using a **configuration‑first, no‑code‑per‑dataset** approach.

Designed for **enterprise‑scale ingestion** with strong **governance, auditability, repeatability, and operational consistency**.



## ✨ Why This Framework?

Traditional ingestion pipelines typically require **custom Spark code per dataset**, making them expensive, error‑prone, and difficult to scale.

This framework enables:

- ✅ Zero code per dataset (metadata only)
- ✅ Consistent validation with automatic quarantine
- ✅ Built‑in auditability, error tracking, and idempotency
- ✅ Standardised **Landing → Bronze → Silver** ingestion pattern
- ✅ Native support for **Databricks Unity Catalog**



## 🚀 Key Features

### Metadata‑Driven Ingestion
- One **JSON configuration per dataset**
- Onboard new datasets without writing Spark code
- Configuration‑first and reusable ingestion patterns

### Ingestion Capabilities
- Batch ingestion using **Unity Catalog Volumes**
- Supported formats:
  - Delimited files (CSV / TSV / Pipe)
  - Fixed‑length files
  - JSON

### Data Quality & Reliability
- Row‑level validations
- Invalid records are never dropped
- Automatic quarantine with failure reasons
- Delta Lake outputs:
  - Bronze
  - Silver
  - Quarantine

### Audit & Governance
- Central ingestion audit table
- Error tracking table
- File registry for replay protection and idempotent processing

### Databricks‑Native
- Apache Spark
- Delta Lake
- Unity Catalog
- `dbutils`



## 📂 Repository Contents

| File / Folder | Purpose |
|--|--|
| `metadata_setup_utility.py` | Defines metadata contract and creates Unity Catalog system tables |
| `ingestion_engine.py` | Core, generic metadata‑driven ingestion runner |
| `metadata/` | Sample dataset configuration JSON files |
| `docs/` | Architecture diagrams and screenshots |



## ⚙️ Prerequisites

- Databricks workspace with **Unity Catalog enabled**
- Unity Catalog **Volumes** created for:
  - `metadata/`
  - `landing/`
  - `archive/` *(optional)*
- Databricks Runtime with **Spark 3.x**



## 🏃 Quick Start

### 1️⃣ Setup Metadata & System Tables

Run the setup utility metadata_setup_utility.py

This action defines the metadata JSON contract and creates system tables viz. ingestion_audit, ingestion_errors & ingestion_file_registry

### 2️⃣ Add a Dataset (No Code)
Create a dataset metadata file under `/Volumes/<catalog>/<schema>/<volume>/metadata/<dataset>.json`

Supported dataset formats Delimited, Fixed‑length & JSON

### 3️⃣ Run Ingestion
Execute ingestion_engine.py using widgets


# Observability, Validation & Governance



## 🔍 Observability & Governance

### Audit Table
Tracks:
- Dataset name
- Execution timestamp
- Number of records read, written, and quarantined
- Run status and descriptive message

### Error Table
Stores:
- File‑level and run‑level failures
- Exception messages and stack traces

### File Registry
- Prevents duplicate file processing
- Enables safe re‑runs
- Ensures idempotent ingestion behaviour



## 🧪 Validation & Quarantine

### Supported Validation Rules
- `not_null`
- `regex`
- `range`
- `in`
- `expr` *(Spark SQL expressions)*

### Invalid Record Handling
- Records are **not dropped**
- Written to a dedicated quarantine Delta table
- Validation failure reasons captured at row level



## ❓ FAQ

**Q: Is streaming supported?**  
❌ No. The framework is **batch‑only by design**.

**Q: Does this support non‑Unity Catalog paths (DBFS / S3 / ADLS directly)?**  
❌ No. Only **Unity Catalog Volumes** are supported.

**Q: Can MERGE or SCD2 be implemented?**  
✅ Yes. The current implementation uses append mode. Silver logic can be extended to support MERGE / SCD2.

**Q: Can multiple files be ingested in a single run?**  
✅ Yes. All eligible files in the landing path are processed unless already registered.

**Q: What happens if the schema changes?**
- Ingestion fails validation
- Records may be quarantined
- Schema evolution can be enabled in Delta as an extension



## 🛠 Troubleshooting

### Issue: No data ingested
Check:
- Landing path correctness
- File registry (file already processed?)
- Dataset widget filters

### Issue: All records quarantined
Review:
- Validation rules in metadata
- Datatype mismatches
- ANSI casting behaviour

### Issue: Table not found
Ensure:
- Correct catalog and schema values in metadata
- System tables were created successfully
- Unity Catalog permissions are granted



## 🧭 Roadmap

- ✅ Modular validation engine
- ✅ MERGE / SCD2 support
- ✅ Schema drift handling
- ✅ Orchestration via Databricks Jobs / Airflow
- 🔮 AI‑assisted metadata generation




## 🧩 Architecture

```text

┌─────────────┐
│ Landing     │  (Unity Catalog Volume)
│ Files       │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│ Ingestion Engine    │
│  • Read             │
│  • Validate         │
│  • Quarantine       │
│  • Audit            │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ Delta Tables (Unity Catalog)             │
│  • Bronze                                │
│  • Silver                                │
│  • Quarantine                            │
│  • Audit / Error / File Registry         │
└─────────────────────────────────────────┘




