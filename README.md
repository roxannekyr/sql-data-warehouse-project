# Data Warehouse & Analytics Engineering Project
##### SQL Server · Medallion Architecture · Star Schema · Business Intelligence

---

## Project Overview

This project delivers a **production-grade Data Warehouse** built on **SQL Server**, following industry-standard **Medallion Architecture** (Bronze → Silver → Gold). Raw business data from two source systems (CRM & ERP) is ingested, cleansed, integrated, and transformed into a **Star Schema data mart** ready for BI reporting, ad-hoc SQL queries, and machine learning consumption.

---

## Business Problem Solved

Organizations operating with siloed CRM and ERP systems struggle to get a unified view of customers, products, and sales performance. This warehouse solves that by:

- **Unifying** customer and product data from disparate source systems into a single source of truth
- **Enabling self-serve analytics** through clean, business-friendly dimensional models
- **Accelerating reporting** with pre-built KPI views for product performance and customer segmentation
- **Ensuring data quality** through a structured, auditable transformation pipeline

---

## High-Level Architecture

![Diagram](documents/High_Level_Architecture.png)

> **Medallion Architecture** (Bronze → Silver → Gold) on SQL Server

| Layer | Object Type | Load Pattern | Transformations | Data Model |
|-------|------------|--------------|-----------------|------------|
| 🥉 **Bronze** | Tables | Batch / Full Load (Truncate + Insert) | None — AS-IS | Raw |
| 🥈 **Silver** | Tables | Batch / Full Load (Truncate + Insert) | Cleansing, Standardization, Normalization, Derived Columns, Enrichment | None — AS-IS |
| 🥇 **Gold** | Views | No Load — on-demand | Data Integration, Aggregations, Business Logic | Star Schema, Flat Tables, Aggregated Tables |

---

## Data Model — Star Schema

The Gold layer implements a **Star Schema** optimised for analytical queries:

![Diagram](documents/Data_Model.png)

### `gold.dim_customers`
| Column | Description |
|--------|-------------|
| `customer_key` | Surrogate key (generated) |
| `customer_id` | Source CRM ID |
| `customer_number` | Business key |
| `first_name`, `last_name` | Cleaned & trimmed |
| `country` | Resolved from ERP location data |
| `marital_status` | Standardized (S→Single, M→Married) |
| `gender` | Resolved with priority: CRM > ERP |
| `birthdate` | Validated (no future dates) |
| `create_date` | Customer acquisition date |

### `gold.dim_products`
| Column | Description |
|--------|-------------|
| `product_key` | Surrogate key (generated) |
| `product_number` | Business key |
| `product_name` | Cleaned |
| `category`, `subcategory` | Joined from ERP category data |
| `cost`, `product_line` | Standardized (S/M/R/T → full names) |
| `maintenance` | ERP enrichment |
| `start_date` | Active product filter applied |

### `gold.fact_sales`
| Column | Description |
|--------|-------------|
| `order_number` | Transaction identifier |
| `product_key` | FK → `dim_products` |
| `customer_key` | FK → `dim_customers` |
| `order_date`, `shipping_date`, `due_date` | Validated & type-cast dates |
| `sales_amount` | Recalculated if inconsistent |
| `quantity`, `price` | Derived/corrected where null/negative |

---

## Integration Model

The following source tables are joined to build the dimensional model:

![Diagram](documents/Integration_Model.png)

- **CRM → ERP join keys:** `cst_key = cid` (after prefix stripping & hyphen removal)
- **Product join keys:** `category_id = id` (after substring extraction & replacement)

---

## Analytics Layer — Business Reports

Two pre-built analytical views surface KPIs for immediate consumption:

### `gold.report_products`
Aggregated product performance metrics:

| KPI | Definition |
|-----|-----------|
| `total_unique_orders` | Distinct order count per product |
| `total_unique_customers` | Distinct buyers per product |
| `total_sales` | Gross revenue |
| `avg_selling_price` | `SUM(sales) / SUM(quantity)` |
| `product_lifespan` | Months between first and last sale |
| `recency_in_months` | Months since last sale |
| `avg_order_revenue` | Revenue per order |
| `avg_monthly_revenue` | Revenue normalized by lifespan |
| `product_segment` | High-Performer / Mid-Range / Low-Performer |

### `gold.report_customers`
Customer behaviour and segmentation metrics:

| KPI | Definition |
|-----|-----------|
| `total_unique_orders` | Orders per customer |
| `total_spending` | Lifetime spend |
| `total_products_ordered` | Breadth of purchase |
| `lifespan` | Months between first and last order |
| `recency` | Months since last purchase |
| `average_order_value` | `total_spending / orders` |
| `average_monthly_spend` | Spend normalized by lifespan |
| `customer_segment` | VIP / Regular / New |
| `age_group` | Under 20 / 20-29 / 30-39 / 40-49 / 50+ |

---

## Data Flow & Lineage

![Diagram](documents/Data_Flow.png)

---

## Data Quality Transformations (Silver Layer)

| Issue Found | Fix Applied | Table |
|-------------|-------------|-------|
| Duplicate customers (same `cst_id`) | `ROW_NUMBER()` partition by `cst_id`, keep latest `cst_create_date` | `crm_cust_info` |
| Abbreviated gender/marital status | CASE decode: `'M'→'Male'`, `'F'→'Female'`, `'S'→'Single'` | `crm_cust_info`, `erp_cust_az12` |
| Leading/trailing whitespace | `TRIM()` on all string columns | All tables |
| Composite `prd_key` (category embedded) | `SUBSTRING` + `REPLACE` to split into `category_id` + `product_key` | `crm_prd_info` |
| Open-ended product history | `LEAD()` window function to derive `prd_end_dt` | `crm_prd_info` |
| Dates stored as integers (YYYYMMDD) | `CAST` chain with length validation (`≠8` or `=0` → NULL) | `crm_sales_details` |
| Null/negative/inconsistent prices | Recalculate: `sales = quantity × ABS(price)` | `crm_sales_details` |
| `NAS` prefix on ERP customer IDs | `SUBSTRING(cid, 4, LEN(cid))` | `erp_cust_az12` |
| Future birthdates | Set to NULL | `erp_cust_az12` |
| Abbreviated country codes | CASE decode: `'DE'→'Germany'`, `'US'/'USA'→'United States'` | `erp_loc_a101` |
| Hyphen in ERP customer ID | `REPLACE(cid, '-', '')` | `erp_loc_a101` |
| NULL costs | `ISNULL(prd_cost, 0)` | `crm_prd_info` |
| Gender conflict between CRM & ERP | CRM takes priority; ERP used as fallback via `COALESCE` | `dim_customers` |

---

## Repository Structure

```
├── datasets/
│   ├── source_crm/
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
│
├── documents/
│   ├── draw.io/
│   │   ├── High_Level_Architecture.drawio
│   │   ├── Data_Flow.drawio
│   │   ├── Integration_Model.drawio
│   │   └── Data_Model.drawio
│   └── data_catalogue.md
│
├── scripts/
│   ├── 01_init_database.sql          # DB + schema creation
│   ├── 02_bronze_ddl.sql             # Bronze table DDL
│   ├── 03_bronze_load.sql            # bronze.load_bronze procedure
│   ├── 04_silver_ddl.sql             # Silver table DDL
│   ├── 05_silver_load.sql            # silver.load_silver procedure
│   ├── 06_gold_dim_customers.sql     # gold.dim_customers view
│   ├── 07_gold_dim_products.sql      # gold.dim_products view
│   ├── 08_gold_fact_sales.sql        # gold.fact_sales view
│   ├── 09_gold_report_products.sql   # gold.report_products view
│   └── 10_gold_report_customers.sql  # gold.report_customers view
│
└── README.md
```

---

## Technical Stack

| Component | Technology |
|-----------|-----------|
| Database | Microsoft SQL Server |
| Query Language | T-SQL |
| Load Pattern | Stored Procedures (Truncate + Bulk Insert) |
| Transformation | SQL CTEs, Window Functions, CASE expressions |
| Modeling | Star Schema (Facts + Dimensions) |
| Diagramming | draw.io |

---

## Key Engineering Decisions

**Why stored procedures for loading?**  
Encapsulating load logic in procedures (`bronze.load_bronze`, `silver.load_silver`) enables scheduled execution, error handling with `TRY/CATCH`, and load duration logging — production-ready patterns straight from enterprise data engineering.

**Why views for the Gold layer?**  
Gold layer views compute on-demand from Silver tables, eliminating redundant storage while keeping the transformation logic version-controlled and easily updatable without re-running load jobs.

**Why surrogate keys?**  
Business keys from source systems are unreliable for joins (format changes, re-use). Generated surrogate keys (`ROW_NUMBER() OVER (ORDER BY ...)`) decouple the warehouse from source system volatility.

**Why LEAD() for product end dates?**  
Source data only contains product start dates. Using `LEAD()` partitioned by `prd_key` derives the end date as `next_version_start - 1 day`, enabling proper Slowly Changing Dimension (SCD Type 2) handling.




