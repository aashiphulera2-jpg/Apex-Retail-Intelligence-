
# Apex-Retail-Intelligence-— End-to-End Data Engineering Pipeline

**Celebal Technologies | CEI'26 Internship Programme — Major Project**
**Author:** Aashi Phulera
**Domain:** Data Engineering / Big Data
**Stack:** Apache Spark (PySpark), Databricks, Delta Lake, Unity Catalog
**Architecture:** Medallion Architecture (Bronze → Silver → Gold)

---

## Overview

Apex Retail Intelligence is a fully automated, end-to-end data pipeline built for a fictional fast-growing retail company. It ingests raw, messy CSV data (customer profiles, product catalogs, sales transactions) and progressively refines it through Bronze, Silver, and Gold layers into a business-ready Star Schema, culminating in five key business KPIs — all computed natively within Databricks with no external BI tools.

The pipeline is fault-tolerant, auditable, and idempotent, satisfying enterprise-grade data engineering standards.

---

## Architecture

```
Raw CSVs (Volume)
      │
      ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│   Raw Zone       │────▶│  Landing Zone     │────▶│  Bronze Layer    │────▶│  Silver Layer     │
│  (String CSVs)    │     │ (Parquet + Audit) │     │  (Delta + meta)  │     │ (DQ, SCD, MERGE)  │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └──────────────────┘
                                                                                     │
                                                                                     ▼
                                                                          ┌──────────────────┐
                                                                          │   Gold Layer      │
                                                                          │  (Star Schema)    │
                                                                          └──────────────────┘
                                                                                     │
                                                                                     ▼
                                                                          ┌──────────────────┐
                                                                          │   KPI Reporting   │
                                                                          └──────────────────┘
```

---

## Folder Structure

```
=======
Raw CSVs (Volume)
│
▼
┌─────────────────┐ ┌──────────────────┐ ┌─────────────────┐ ┌──────────────────┐
│ Raw Zone │────▶│ Landing Zone │────▶│ Bronze Layer │────▶│ Silver Layer │
│ (String CSVs) │ │ (Parquet + Audit) │ │ (Delta + meta) │ │ (DQ, SCD, MERGE) │
└─────────────────┘ └──────────────────┘ └─────────────────┘ └──────────────────┘
│
▼
┌──────────────────┐
│ Gold Layer │
│ (Star Schema) │
└──────────────────┘
│
▼
┌──────────────────┐
│ KPI Reporting │
└──────────────────┘

---

## Repository Structure
>>>>>>> ad4f7c63d932e286ea93e4c7daa1cf7530b9b7fb
celebal project/
├── README.md
├── notebooks/
│   ├── 01_Raw_Landing_Script.html
│   ├── 02_Bronze_Layer_Script.html
│   ├── 03_Silver_Layer_Notebook.html
│   └── 04_Gold_Layer_KPI_Notebook.html
├── Datasets/
<<<<<<< HEAD
│   ├── historical_data/
│   ├── incremental_data/
│   ├── audit_landing/
│   └── audit_silver/
└── SS/
    ├── 00_catalog_setup.png
    ├── 01_phase1_raw_ingestion.png
    ├── 02_phase2_landing_audit.png
    ├── 03_phase3_bronze_layer.png
    ├── 04_phase4_customer_cleaning.png
    ├── 05_phase4_scd2_customers.png
    ├── 06_phase4_scd1_products.png
    ├── 07_phase4_sales_ledger.png
    ├── 08_phase4_merge_explanation_md.png
    ├── 09_phase4_silver_audit.png
    ├── 10_phase5_gold_star_schema.png
    ├── 11_phase6_kpi1_net_margin.png
    └── 12_phase6_kpi_outputs.png
=======
│   └── (your original CSVs — optional to include, some programmes don't want raw data in submission)
└── SS/
    ├── 00_catalog_setup.png
    ├── 01_phase1_raw_ingestion.png
    ├── ... (renamed screenshots)

```
---

## Pipeline Phases

### Phase 1 — Raw Zone (`01_Raw_Landing_Script`)
Ingests all incoming CSVs, casts every column to String format, and organizes output into separate `raw/historical/` and `raw/incremental/` directories per dataset.

### Phase 2 — Landing Zone (`01_Raw_Landing_Script`)
Converts Raw CSVs to Parquet. Dynamically reads `audit_landing*.csv` files and validates actual vs. expected row counts, producing a structured PASS/FAIL report. Pipeline halts on audit failure.

### Phase 3 — Bronze Layer (`02_Bronze_Layer_Script`)
Writes Landing Parquet data into Delta Lake tables with an `ingested_at` timestamp for full audit trail. Historical and incremental loads are kept in separate append-only tables — no deduplication at this stage, by design.

### Phase 4 — Silver Layer (`03_Silver_Layer_Notebook`)
=======
### Phase 1 — Raw Zone (`01_Raw_Landing_Script.py`)
Ingests all incoming CSVs, casts every column to String format, and organizes output into separate `raw/historical/` and `raw/incremental/` directories per dataset.

### Phase 2 — Landing Zone (`01_Raw_Landing_Script.py`)
Converts Raw CSVs to Parquet. Dynamically reads `audit_landing*.csv` files and validates actual vs. expected row counts, producing a structured PASS/FAIL report. Pipeline halts on audit failure.

### Phase 3 — Bronze Layer (`02_Bronze_Layer_Script.py`)
Writes Landing Parquet data into Delta Lake tables with an `ingested_at` timestamp for full audit trail. Historical and incremental loads are kept in separate append-only tables — no deduplication at this stage, by design.

### Phase 4 — Silver Layer (`03_Silver_Layer_Notebook.py`)

The core transformation layer:
- **Data Quality Rules:** drops rows with missing primary keys, removes duplicates, casts numeric fields, fills missing values
- **Customers — SCD Type 2:** historical tracking via `effective_start_date`, `effective_end_date`, `is_active`
- **Products — SCD Type 1:** in-place MERGE, no history retained
- **Sales — Immutable Ledger:** strict deduplication via `row_number()` window function before MERGE
- **Surrogate Keys:** `customer_sk`, `product_sk`, `sales_sk` generated for Gold-layer joins
- **Assertions:** row-count and duplicate-check assertions validate every merge
- **Silver Audit Validation:** cross-checked against `*_silver_audit.csv` files
- Includes a dedicated markdown cell explaining MERGE outcomes for all three tables

### Phase 5 — Gold Layer (`04_Gold_Layer_KPI_Notebook`)
Builds the Star Schema and registers all tables under Unity Catalog's `GOLD_tables` schema:
- `dim_customer`, `dim_product`, `dim_promotion`, `dim_date`, `fact_sales`

### Phase 6 — KPI Reporting (`04_Gold_Layer_KPI_Notebook`)
=======
### Phase 5 — Gold Layer (`04_Gold_Layer_KPI_Notebook.py`)
Builds the Star Schema and registers all tables under Unity Catalog's `GOLD_tables` schema:
- `dim_customer`, `dim_product`, `dim_promotion`, `dim_date`, `fact_sales`

### Phase 6 — KPI Reporting (`04_Gold_Layer_KPI_Notebook.py`)

Five business KPIs computed via native PySpark DataFrame/SQL operations, rendered inline:
1. Net Margin by Region
2. Average Order Value (AOV) by Promotion
3. Demographic Churn Heatmap (state × loyalty programme)
4. Product Quality Index (return rate by category)
5. Store Traffic by Hour and Day of Week

---

## Data Summary

| Dataset | Historical Rows | Incremental Rows | Silver (Clean) Rows |
|---|---|---|---|
| Customers | 1,052 | 1,053 | 1,050 |
| Products | 1,043 | 1,041 | 1,041 |
| Sales | 1,002 | 1,000 | 2,000 |

---

## Gold Layer Summary

| Table | Row Count |
|---|---|
| dim_customer | 1,050 |
| dim_product | 1,041 |
| dim_promotion | 864 |
| dim_date | 3,652 |
| fact_sales | 2,000 |

---

## Key Design Decisions

- **No watermarking:** all incremental processing uses Delta Lake MERGE semantics, as explicitly required by the assignment.
- **Idempotency:** Landing, Silver, and Gold layers use `overwrite`/`MERGE` and are safely re-runnable. Bronze is intentionally append-only per the assignment spec.
- **Schema reconciliation:** incremental source files occasionally contained extra columns not present in historical files (e.g. pre-existing SCD metadata artifacts in the customer incremental file, `last_updated` in the product incremental file). These were reconciled explicitly — irrelevant artifacts dropped, meaningful new fields retained via `unionByName(allowMissingColumns=True)`.
=======
## Key Design Decisions

- **No watermarking:** all incremental processing uses Delta Lake MERGE semantics, as explicitly required.
- **Idempotency:** Landing, Silver, and Gold layers use `overwrite`/`MERGE` and are safely re-runnable. Bronze is intentionally append-only per the assignment spec.
- **Schema reconciliation:** incremental source files occasionally contained extra columns not present in historical files (e.g. pre-existing SCD metadata artifacts in the customer incremental file, `last_updated` in product incremental). These were reconciled explicitly — irrelevant artifacts dropped, meaningful new fields retained via `unionByName(allowMissingColumns=True)`.

- **Audit reconciliation note:** Silver-layer audit files describing only incremental batch sizes will not match final merged/deduplicated Silver counts by design — this is documented explicitly in the Silver notebook rather than treated as a failure.

---

## How to Run

1. Upload all source CSVs (historical, incremental, and audit files) to `/Volumes/apex_retail/bronze/incoming_data/`.
2. Run notebooks in order: `01` → `02` → `03` → `04`.
3. Each notebook is independently executable and reads from Delta tables/Volumes produced by the prior phase — no in-memory dependencies between notebooks.

---

## Academic Integrity

This project was completed individually as part of the Celebal Technologies CEI'26 Internship Programme.
