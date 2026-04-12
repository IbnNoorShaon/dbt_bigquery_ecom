# 🛒 E-Commerce Analytics Engineering Project
### GCP | dbt Cloud | BigQuery | GitHub

---

## 📌 Project Overview
This project demonstrates a modern analytics engineering pipeline built on 
Google Cloud Platform using dbt Cloud. It transforms raw e-commerce data from 
the publicly available **TheLook Ecommerce** dataset (BigQuery Public Data) 
into clean, tested, and documented data models ready for BI and AI consumption.

---

## 🏗️ Architecture
bigquery-public-data.thelook_ecommerce  (Source)
↓
pr-ecom.raw        (Views  — light casting + audit columns)
↓
pr-ecom.prep       (Views  — cleaning, renaming, business logic)
↓
pr-ecom.serve      (Tables — final incremental models for BI & AI)

---

## 🔧 Tech Stack

| Tool | Purpose |
|---|---|
| **Google BigQuery** | Cloud data warehouse |
| **dbt Cloud** | Data transformation & orchestration |
| **GitHub** | Version control & CI/CD |
| **TheLook Ecommerce** | Public e-commerce dataset |

---

## 📂 Project Structure
pr_ecom/
├── models/
│   ├── raw/          ← 1:1 views from source with casting & audit cols
│   ├── prep/         ← Cleaned, renamed, business logic applied
│   └── serve/        ← Final incremental tables for BI & AI
├── macros/
│   ├── audit_columns.sql     ← Reusable audit metadata macro
│   └── generate_cast.sql     ← Dynamic column casting macro
├── tests/
├── snapshots/
└── dbt_project.yml

---

## 🔄 Data Pipeline

### Raw Layer
- Materialized as **views** in `pr-ecom.raw`
- Light transformation using **Jinja macros**
- Every model includes audit columns: `_loaded_at`, `_source_table`, `_dbt_invocation_id`, `_environment`
- Sources defined from `bigquery-public-data.thelook_ecommerce`

### Prep Layer *(in progress)*
- Materialized as **views** in `pr-ecom.prep`
- Data cleaning, column renaming, business logic
- Relationships and data quality tests applied

### Serve Layer *(in progress)*
- Materialized as **incremental tables** in `pr-ecom.serve`
- Optimized for BI tools and AI consumption
- Final facts and dimensions

---

## 💡 Key Features

- ✅ **Reusable Jinja Macros** — dynamic casting and audit columns
- ✅ **Three-tier architecture** — raw → prep → serve
- ✅ **dbt tests** — unique, not_null on all primary keys
- ✅ **Source freshness** — defined for all timestamp-based tables
- ✅ **Full documentation** — every model and column described in YML
- ✅ **GitHub workflow** — feature branches and PRs per layer

---

## 📊 Source Tables

| Table | Description |
|---|---|
| `orders` | Customer orders |
| `order_items` | Individual line items per order |
| `products` | Product catalog |
| `users` | Customer profiles |
| `inventory_items` | Inventory units |
| `distribution_centers` | Warehouse locations |
| `events` | Web clickstream events |

---
## Looker Dashboard Link: https://lookerstudio.google.com/s/jhUCh6XoNwM

## 🚀 Getting Started

### Prerequisites
- GCP account with BigQuery enabled
- dbt Cloud account (free Developer plan)
- GitHub account

### Setup
1. Fork this repository
2. Connect dbt Cloud to your GitHub repo
3. Configure BigQuery connection in dbt Cloud
4. Run the pipeline:
```bash
dbt run --select tag:raw
dbt test --select tag:raw
```

---

## 👤 Author
**Your Name**  
[LinkedIn](https://www.linkedin.com/in/muhammad-n-alam/) | [GitHub](https://github.com/IbnNoorShaon)
