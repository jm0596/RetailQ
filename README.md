# RetailQ — End-to-End Retail Analytics Platform

RetailQ is a medallion-architecture analytics project on Databricks that ingests retail data from three sources (Azure Blob CSV, PostgreSQL, Salesforce), lands it in bronze, refines it into silver and gold layers via a Lakeflow Spark Declarative Pipeline (SDP), exposes business metrics through a Unity Catalog semantic layer, and publishes the results to AI/BI dashboards — all orchestrated by a single Lakeflow Job.

## Architecture Overview

```
Azure Blob (CSV)  ──►  blob_to_bronze (notebook)              ──► retail_q.blob_bronze
PostgreSQL        ──►  RetailQ_Postgres_to_Bronze (pipeline)  ──► retail_q.bronze
Salesforce        ──►  RetailQ_Salesforce_to_bronze (pipeline) ──► retail_q.salesforce_bronze

        ┌──────────────────────────────────────────────┐
        │  retail_transformations (SDP pipeline)         │
        │  bronze_to_silver/  → retail_q.retail_silver  │
        │  silver_to_gold/    → retail_q.gold           │
        └──────────────────────────────────────────────┘
                          │
        metric_view (notebook) → retail_q.semantic_layer.retail_metrics
                          │
        Product Performance & RetailQ Sales Analysis dashboards
```

## Orchestration — RetailQ End to End Job

The [RetailQ End to End Job](/jobs/54766936243184) runs the full workflow in dependency order:

| # | Task | Type | Description |
| --- | --- | --- | --- |
| 1 | `Blob_to_bronze` | Notebook | Auto Loader (`cloudFiles`, CSV, AvailableNow trigger) streams transactions from `/Volumes/retail_q/source/blob_source/transactions/` into `retail_q.blob_bronze.transactions` |
| 2 | `Postgres_to_Bronze` | Managed ingestion pipeline | Replicates PostgreSQL tables into `retail_q.bronze` |
| 3 | `Salesforce_to_bronze` | Managed ingestion pipeline | Replicates Salesforce objects (accounts, opportunities) into `retail_q.salesforce_bronze` |
| 4 | `bronze_to_silver_to_gold` | SDP pipeline | `retail_transformations` — cleans and models the data (runs after tasks 1–3) |
| 5 | `Metric_Views` | Notebook | Creates the `retail_q.semantic_layer.retail_metrics` metric view |
| 6 | `Product_and_Sales_Dashboard` | Dashboard task | Refreshes **Product Performance: Catalog, Inventory & Sales** |
| 7 | `Sales_Dashboad` | Dashboard task | Refreshes **RetailQ Sales Analysis** |

Tasks 1–3 run in parallel; everything else is sequential.

## Pipeline — retail_transformations

Serverless SDP pipeline (ID `244fea0a-2a14-44ff-a1da-ac70717e56bf`), catalog `retail_q`, default schema `retail_silver`. Source files live in `retail_transformations/`.

### Bronze → Silver (`bronze_to_silver/*.py`)

Streaming tables published to `retail_q.retail_silver`:

* `account` (accounts.py) — from Salesforce accounts
* `opportunity` (opportunity.py) — from Salesforce opportunities
* `inventory` (inventory.py) — Auto CDC (SCD2: `__START_AT` / `__END_AT` / `is_active`)
* `product_catalog` (product_catalog.py)
* `transactions` (transactions.py)

### Silver → Gold (`silver_to_gold/*.py`)

Materialized views published to `retail_q.gold` via fully-qualified names in the decorators:

* `dim_product` (dim_product.py) — active products only
* `dim_customer` (dim_customer.py)
* `dim_calendar` (dim_calendar.py) — calendar dates 2020–2030 (generated via `explode(sequence(...))`)
* `fact_sales` (fact_sales.py) — joins `dim_calendar` on `full_date = transaction date` and `dim_product` on product key
* `fact_inventory` (inventory.sql) — `CREATE OR REFRESH MATERIALIZED VIEW` from silver inventory

> PK / NOT NULL constraints are not representable in SDP and are kept as post-creation `ALTER TABLE` statements.

### Metric view (one-time DDL, not a pipeline source)

The `metric_view` notebook creates `retail_q.semantic_layer.retail_metrics`, joining `fact_sales` to `dim_calendar` (date_key), `dim_customer` (customer_id), and `dim_product` (product_id). All joins are `at_most_one_match` with 100% key coverage.

**Measures:** Transaction Count, Total Units Sold, Total Sales (gross = SUM `selling_price`), Total Discounts, Revenue (SUM `net_amount`), Average Order Value, Unique Customers, Discount Rate, Units per Transaction, Revenue per Customer. No profit measure exists — there is no cost/COGS column anywhere in gold or silver.

**Price semantics:** `selling_price` = gross transaction total; `net_amount` = `selling_price − discount_amount`; silver `discount_amount` maps to gold `discount_price`.

**Dimensions:** time (date/year/quarter/month/day-of-week/is-weekend), customer (type/city/state/country/industry), product (category/subcategory/brand/segment), payment mode, sales channel, store.

## Dashboards & Genie

* **Product Performance: Catalog, Inventory & Sales** — refreshed by the job on every run
* **RetailQ Sales Analysis** — refreshed by the job on every run
* **Retail Q Sales and Inventory Analysis** — Genie Agent space over the gold tables for natural-language Q&A

## Repository Layout

```
RetailQ/
├── README.md
├── blob_to_bronze/                # Auto Loader notebook (task 1)
├── retail_transformations/
│   ├── bronze_to_silver/          # SDP streaming tables → retail_silver
│   ├── silver_to_gold/            # SDP materialized views → gold
│   └── metric_view                # one-time DDL for the metric view
├── Product Performance... .lvdash.json
└── RetailQ Sales Analysis.lvdash.json
```

## Known Open Items


* `fact_inventory` selects base columns without an SCD2 (`is_active = true`) filter — it currently holds all row versions from silver inventory. Add a filter if only current inventory state is desired.
* `dim_customer.py` contains a module-level `spark.sql(CREATE SCHEMA IF NOT EXISTS retail_q.gold)` that may fail at pipeline import; it should be removed (the schema already exists).

