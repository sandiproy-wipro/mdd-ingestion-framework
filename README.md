# MDD Ingestion Framework

A **metadata-driven ingestion framework** for onboarding **batch data feeds** into **Databricks Unity Catalog**, using a **configuration-first, no-code-per-dataset** approach.

Designed for **enterprise-scale ingestion** with strong **governance, auditability, and repeatability**.


## ✨ Why This Framework?

Traditional ingestion pipelines require **custom code per dataset**, making them expensive and difficult to scale.

This framework enables:

- ✅ Zero code per dataset (metadata only)
- ✅ Consistent validation and automatic quarantine
- ✅ Built-in audit, error tracking, and idempotency
- ✅ Standardised **Landing → Bronze → Silver** pattern
- ✅ Native design for **Databricks Unity Catalog**


## 🚀 Key Features

### Metadata-Driven Ingestion
- One **JSON configuration per dataset**
- Add new feeds **without writing Spark code**

### Ingestion Capabilities
- Batch ingestion using **Unity Catalog Volumes**
- Supported formats:
  - Delimited (CSV / TSV / Pipe)
  - Fixed-length files
  - JSON

### Data Quality & Reliability
- Row-level validations
- Invalid records are **quarantined automatically**
- Delta Lake outputs:
  - Bronze
  - Silver
  - Quarantine

### Audit & Governance
- Ingestion Audit table
- Error tracking table
- File registry for **replay protection and idempotency**

### Databricks-Native
- Apache Spark
- Delta Lake
- Unity Catalog
- dbutils


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
