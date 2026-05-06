**MDD Ingestion Framework**

A metadata driven ingestion framework for onboarding batch data feeds into Databricks Unity Catalog using a configuration first, no code-per-dataset approach.
Designed for enterprise scale ingestion with strong governance, auditability, and repeatability.

**✨ Why this framework?**
Traditional ingestion pipelines require custom code per feed, making them expensive to scale.
This framework enables:
✅ Zero code per dataset (metadata only)
✅ Consistent validation + quarantine
✅ Built in audit, error tracking, and idempotency
✅ Standardized Landing → Bronze/Silver pattern
✅ Designed for Databricks Unity Catalog

**🚀 Key Features**
•	Metadata driven ingestion 
o	One JSON config per dataset
o	Add new feeds without writing Spark code
•	Batch ingestion (UC Volumes)
•	Supported formats 
o	Delimited (CSV / TSV / pipe)
o	Fixed length files
o	JSON
•	Row level validations 
o	Invalid rows automatically quarantined
•	Delta Lake outputs 
o	Bronze / Silver / Quarantine tables
•	Audit & governance 
o	Ingestion audit table
o	Error table
o	File registry for replay protection
•	Databricks native 
o	Spark, Delta, Unity Catalog, dbutils

🧩 Architecture
┌─────────────┐
│ Landing     │  (UC Volume)
│ Files       │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│ Ingestion Engine    │
│  - Read             │
│  - Validate         │
│  - Quarantine       │
│  - Audit            │
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

**📂 Repository Contents**
File	Purpose
metadata_setup_utility.py	Metadata contract, examples, UC table setup
ingestion_engine.py	Core ingestion runner
docs/	Architecture + screenshots (recommended)
metadata/	Sample dataset configs (recommended)

**⚙️ Prerequisites**
•	Databricks workspace with Unity Catalog enabled
•	UC Volumes for: 
o	metadata/
o	landing/
o	archive/ (optional)
•	Databricks Runtime (Spark 3.x)

**🏃 Quick Start**
1️⃣ Setup metadata & system tables
Run:
Plain Text
metadata_setup_utility.py
Show more lines
This will:
•	Define the metadata JSON structure
•	Create: 
o	ingestion_audit
o	ingestion_errors
o	ingestion_file_registry

2️⃣ Add a dataset (no code)
Create a JSON file under:
/Volumes/<catalog>/<schema>/<volume>/metadata/<dataset>.json
Supported formats:
•	Delimited
•	Fixed length
•	JSON
3️⃣ Run ingestion
Run:
ingestion_engine.py
Using widgets:
•	mode: ALL or ONE
•	dataset: dataset name (if ONE)
•	archive: YES / NO

**🔍 Observability & Governance**
Audit Table
Tracks:
•	Dataset
•	Run time
•	Records read / written / quarantined
•	Status & message
Error Table
Stores:
•	File level and run level failures
•	Exception details
File Registry
•	Prevents duplicate processing
•	Enables idempotent ingestion

**🧪 Validation & Quarantine**
Supported validation rules:
•	not_null
•	regex
•	range
•	in
•	expr (Spark SQL)
Invalid rows:
•	Are not dropped
•	Are written to a quarantine Delta table
•	Include failure reason(s)

**❓ FAQ**
Q: Is streaming supported?
❌ No. This framework is batch only by design.
Q: Does this support non UC paths (DBFS / S3 directly)?
❌ No. Only Unity Catalog Volumes are supported.
Q: Can I do MERGE / SCD2?
✅ Yes. Current version uses append, but Silver logic can be extended to MERGE.
Q: Can this ingest multiple files per run?
✅ Yes. All files in landing path are processed unless already registered.
Q: What happens if schema changes?
•	Ingestion will fail validation
•	Records may be quarantined
•	Schema evolution can be enabled in Delta if desired (extendable)
**🛠 Troubleshooting**
Issue: No data ingested
•	Check: 
o	Landing path correctness
o	File registry (file already processed?)
o	Dataset filter widget
Issue: All records in quarantine
•	Review: 
o	Validation rules in metadata
o	Data type mismatches
o	ANSI casting settings
Issue: Table not found
•	Ensure: 
o	Correct catalog/schema in metadata
o	Tables created by setup notebook
o	UC permissions granted

**🧭 Roadmap**
•	✅ Modular validation engine
•	✅ SCD2 / MERGE support
•	✅ Schema drift handling
•	✅ Orchestration via Databricks Jobs / Airflow
•	✅ AI assisted metadata generation (future)

