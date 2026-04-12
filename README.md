# 🛒 E-Commerce Analytics Engineering
### GCP · dbt Cloud · BigQuery · Looker · GitHub

A production-style analytics engineering pipeline built on GCP using dbt Cloud. 
Transforms raw e-commerce data from BigQuery's public **TheLook Ecommerce** dataset 
into clean, tested, and documented data models — serving a 5-page Looker dashboard 
and ready for AI consumption.

👉 [View Live Looker Dashboard](https://lookerstudio.google.com/s/jhUCh6XoNwM)

---

## 🏗️ Architecture
bigquery-public-data.thelook_ecommerce  (Source)
↓
pr-ecom.raw        (Views  — light casting + audit columns)
↓
pr-ecom.prep       (Views  — cleaning, renaming, business logic)
↓
pr-ecom.serve      (Incremental Tables — optimized for BI & AI)

---

## 🔧 Tech Stack

| Tool | Purpose |
|---|---|
| **Google BigQuery** | Cloud data warehouse |
| **dbt Cloud** | Transformation, orchestration & CI/CD |
| **Looker Studio** | 5-page BI dashboard |
| **GitHub** | Version control & feature branching |
| **TheLook Ecommerce** | Public e-commerce source dataset |

---

## 💡 Key Features

- ✅ **Three-tier Medallion architecture** — Raw → Prep → Serve
- ✅ **Incremental Serve layer** — partitioned by `partition_key` and filtered by `run_date` for efficient loads and easy data removal
- ✅ **Reusable Jinja Macros** — dynamic casting and audit columns
- ✅ **dbt tests** — unique, not_null on all primary keys
- ✅ **Source freshness** — defined for all timestamp-based tables
- ✅ **Full documentation** — every model and column described in YML
- ✅ **5-page Looker Dashboard** — sales, orders, customers, products & inventory
- ✅ **AI-ready Serve layer** — clean, consistent naming for LLM consumption

---

## 🔄 Data Pipeline

### Raw Layer
- Materialized as **views** in `pr-ecom.raw`
- 1:1 mapping from source with light casting via Jinja macros
- Every model includes audit columns: `_loaded_at`, `_source_table`, `_dbt_invocation_id`, `_environment`

### Prep Layer
- Materialized as **views** in `pr-ecom.prep`
- Data cleaning, column renaming, and business logic applied
- Data quality and relationship tests enforced

### Serve Layer
- Materialized as **incremental tables** in `pr-ecom.serve`
- Uses `run_date` as the incremental filter to process only new or changed data
- Partitioned by `partition_key` for optimized query performance and cost efficiency
- Partition-based design allows clean, surgical data removal without full reloads
- Final facts and dimensions ready for Looker and AI agents

---

## 📂 Project Structure
pr_ecom/
├── models/
│   ├── raw/          ← 1:1 views from source with casting & audit cols
│   ├── prep/         ← Cleaned, renamed, business logic applied
│   └── serve/        ← Incremental tables partitioned for BI & AI
├── macros/
│   ├── audit_columns.sql     ← Reusable audit metadata macro
│   └── generate_cast.sql     ← Dynamic column casting macro
├── tests/
├── snapshots/
└── dbt_project.yml

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
**Muhammad Noor-E-Alam**  
[LinkedIn](https://www.linkedin.com/in/muhammad-n-alam/) | [GitHub](https://github.com/IbnNoorShaon)
