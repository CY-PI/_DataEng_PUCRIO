# 📊 MVP - Data Engineering — Olist Analysis

> End-to-end data pipeline (medallion architecture) built on the public **Olist** dataset, a Brazilian marketplace, to answer business questions about sales, sellers, marketing, and churn.

🇧🇷 *Versão em português: [README.md](README.md)*

## 📑 Table of Contents

- [Context](#-context)
- [Objectives](#-objectives)
- [Documentation](#-documentation)
- [Data Collection](#-data-collection)
- [Data Modeling](#data-modeling)
- [Data Loading and Pipeline](#-data-loading-and-pipeline)
- [Data Quality](#-data-quality)
- [Data Analysis](#-data-analysis)
- [Self-Assessment](#-self-assessment)
- [Databricks Jobs & Pipeline](#databricks-jobs-pipeline)

<br>

---

## 🧭 Context

In a business environment, commercial and marketing analyses are essential to know where the opportunities and risks are. For this MVP, I chose a dataset (**Olist E-commerce**, from Kaggle) that allows exploring a scenario similar to a corporate environment, turning raw data into performance insights.

Olist is a Brazilian marketplace platform that connects small and medium-sized sellers to large sales channels. The dataset contains order history, as well as data on seller acquisition for the platform.

### 🎯 Objectives

Using the dataset above, the project aims to answer the following questions:

1. Which months have the highest peaks?
2. Where does the revenue come from — does the Pareto rule apply to the platform's sellers?
3. Which product categories bring in the most revenue?
4. Is any marketing channel better (in terms of conversion and average sales cycle)?
5. Is there a relationship between low revenue and a higher risk of leaving the platform? (LTV, churn)

> 💡 The dataset would allow answering additional questions, but for the purposes of this MVP the scope was limited to the questions above.

### 📚 Documentation

The data pipeline was built following the **medallion architecture**, which provides data traceability from its raw state to its availability for analysis:

| Layer | Role |
|---|---|
| 🥉 **Bronze** | Raw data, no treatment |
| 🥈 **Silver** | Standardized and cleaned data |
| 🥇 **Gold** | Analysis-ready data |

The data catalog in Databricks follows this same division (bronze / silver / gold). The notebooks are organized into `ingestion`, `bronze`, `silver`, `gold`, `analise` (analysis), and `common` (functions shared between the silver and gold notebooks).

<br>

---

## 📥 Data Collection

The files used in this project come from **Kaggle**, a repository that provides transparency regarding the license and origin of the data, avoiding privacy and copyright issues.

**Sources:**
- [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download)
- [Marketing Funnel by Olist](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist)

**📄 License:** both datasets are available under **CC BY-NC-SA 4.0** (Attribution, Non-Commercial, ShareAlike), which is compatible with the academic use of this MVP.

**⚠️ Considerations:**
- Originally there are more spreadsheets available on Kaggle; for simplicity, only those that serve the objectives of this work were included.
- Not all columns in the tables below are used in the final model, since they are not needed to answer the initial questions, which focus on sales and marketing indicators. They were dropped in the Silver layer and are marked in *italics* in the tables below.

Click the tables below to view details.

<br>

<details>
<summary><strong>📁 olist_order_items_dataset.csv</strong></summary>

| Column | Type |
|---|---|
| order_id | string |
| *order_item_id* | int |
| product_id | string |
| seller_id | string |
| *shipping_limit_date* | timestamp |
| price | double |
| *freight_value* | double |

</details>
<br>
<details>
<summary><strong>📁 olist_customers_dataset.csv</strong></summary>

| Column | Type |
|---|---|
| *customer_id* | string |
| *customer_unique_id* | string |
| *customer_zip_code_prefix* | string |
| *customer_city* | string |
| customer_state | string |

</details>
<br>
<details>
<summary><strong>📁 olist_orders_dataset.csv</strong></summary>

| Column | Type |
|---|---|
| order_id | string |
| customer_id | string |
| order_status | string |
| order_purchase_timestamp | timestamp |
| *order_approved_at* | timestamp |
| *order_delivered_carrier_date* | timestamp |
| *order_delivered_customer_date* | timestamp |
| *order_estimated_delivery_date* | timestamp |

</details>
<br>
<details>
<summary><strong>📁 olist_sellers_dataset.csv</strong></summary>

| Column | Type |
|---|---|
| seller_id | string |
| *seller_zip_code_prefix* | string |
| seller_city | string |
| seller_state | string |

</details>
<br>
<details>
<summary><strong>📁 olist_marketing_qualified_leads_dataset.csv</strong></summary>

| Column | Type |
|---|---|
| mql_id | string |
| first_contact_date | date |
| *landing_page_id* | string |
| origin | string |

</details>
<br>
<details>
<summary><strong>📁 olist_closed_deals_dataset.csv</strong></summary>

| Column | Type |
|---|---|
| mql_id | string |
| seller_id | string |
| sdr_id | string |
| sr_id | string |
| won_date | timestamp |
| business_segment | string |
| lead_type | string |
| *lead_behaviour_profile* | string |
| *has_company* | boolean |
| *has_gtin* | boolean |
| *average_stock* | string |
| *business_type* | string |
| *declared_product_catalog_size* | double |
| *declared_monthly_revenue* | double |

</details>
<br>
<details>
<summary><strong>📁 olist_products_dataset.csv</strong></summary>

| Column | Type |
|---|---|
| product_id | string |
| product_category_name | string |
| *product_name_lenght* | int |
| *product_description_lenght* | int |
| *product_photos_qty* | int |
| *product_weight_g* | int |
| *product_length_cm* | int |
| *product_height_cm* | int |
| *product_width_cm* | int |

</details>

<br>

---
<a id="data-modeling"></a>

## 🏗️ Data Modeling

### ⭐ Star Schema

Since the goal is to create an OLAP environment, a star schema was used.
Initially the model included dim_customer, but this dimension was removed because only one column was relevant to the business questions defined in the objectives. `purchase_state` (the state where the order was placed) is the only attribute needed and is kept directly in fato_vendas (the sales fact table). This decision can be revisited if future analyses require more customer granularity (e.g., number of buyers).

<img width="744" height="431" alt="image" src="https://github.com/user-attachments/assets/936be817-9e8f-4801-820b-2a3a3f749c99" />



#### `fato_vendas`

Sales fact table with the metrics and dimensions needed for commercial analyses.

| Column | Type | Description | Source | Domain |
|---|---|---|---|---|
| `order_id` | `STRING` | Part of the composite PK (`order_id + product_id + seller_id`) — order ID | `bronze.orders` | Unique alphanumeric identifier |
| `mql_id` | `STRING` | FK of the marketing lead that originated this sale, when applicable | `silver.closed_deals` via `LEFT JOIN` | Nullable; not every sale has an associated lead |
| `seller_id` | `STRING` | FK → `dim_sellers.seller_id` + part of the composite PK — seller on the platform. Represents the platform's real customer | `bronze.sellers` via `dim_sellers` | Alphanumeric value |
| `purchase_state` | `STRING` | State (UF) where the order was placed | `silver.customers` — `customer_state` | Brazilian state abbreviation, 2 letters |
| `product_id` | `STRING` | FK → `dim_products.product_id` + part of the composite PK — product sold | `bronze.order_items` | Alphanumeric value |
| `order_date` | `DATE` | FK of the purchase date | `bronze.orders` — `order_purchase_timestamp` | Format `YYYY-MM-DD`; range from `2016-09-04` to `2018-10-17` |
| `order_status` | `STRING` | Current status of the order in the order lifecycle | `bronze.orders` | `approved`, `created`, `delivered`, `invoiced`, `processing`, `shipped` |
| `quantity` | `INT` | Number of items sold in the order | `bronze.order_items` — aggregated `COUNT` | Greater than `0`; integer |
| `sales_value` | `DECIMAL` | Total value sold in the order | `bronze.order_items` | Greater than `0`; range from `0.85` to `6735.00` |
| `commission` | `DECIMAL` | Platform commission on the sale | Calculated as `sales_value * 0.1` | 10% of `sales_value` |

> 📌 **Order exclusion:** orders with `order_status` = `canceled` or `unavailable` are excluded from the fact table, since they do not represent real sales.

> 📌 **MVP assumption:** the 10% rate is a simplification adopted for this project — the original dataset does not provide Olist's actual commission. The `commission` column exists to enable platform revenue analyses.

<br>

#### `dim_leads`

All marketing qualified leads (MQLs), including both those that converted into sellers and those that did not. Consolidates lead and conversion information in a single dimension to facilitate full-funnel analyses.

**Modeling rationale:** the Primary Key of this table is `mql_id`, since it is the only column that is always non-null and unique. `won_date` and `sales_cycle` are `NULL` for non-converted leads.

*Source:* `bronze.marketing_qualified_leads` + `bronze.closed_deals` (for converted leads).

| Column | Type | Description | Domain |
|---|---|---|---|
| `mql_id` | `STRING` | PK — unique lead ID | Unique alphanumeric identifier |
| `origin` | `STRING` | Channel through which the lead arrived | `direct_traffic`, `display`, `email`, `organic_search`, `other`, `other_publicities`, `paid_search`, `referral`, `social`, `unknown` |
| `first_contact_date` | `DATE` | Date of first contact | Format `YYYY-MM-DD` |
| `won_date` | `DATE` | Date on which the lead converted into a sale | Format `YYYY-MM-DD`; `NULL` for non-converted leads |
| `sales_cycle` | `INT` | Time, in days, between first contact and conversion | Calculated as `DATEDIFF(won_date, first_contact_date)`; positive integer; `NULL` for non-converted leads |

<br>

#### `dim_sellers`

Sellers registered on the platform. Enables geographic analyses by city and state.

*Source:* `silver.sellers`

| Column | Type | Description | Domain |
|---|---|---|---|
| `seller_id` | `STRING` | PK — unique seller ID | Unique alphanumeric identifier |
| `seller_city` | `STRING` | City where the seller is located | Brazilian cities; `"unknown"` for null values |
| `seller_state` | `STRING` | State (UF) where the seller is located | Brazilian state abbreviation, 2 letters |

<br>

#### `dim_products`

Products available on the platform.

*Source:* `silver.products`

| Column | Type | Description | Domain |
|---|---|---|---|
| `product_id` | `STRING` | PK — unique product ID | Unique alphanumeric identifier |
| `category` | `STRING` | Product category | 74 categories in Portuguese (e.g., `agro_industria_e_comercio`, `alimentos`, …); `"unknown"` for null values |

<br>

#### `dim_dates`

Time dimension for period-based analyses.

| Column | Type | Description | Source | Domain |
|---|---|---|---|---|
| `order_date` | `DATE` | PK — date (YYYY-MM-DD) | Derived from `order_purchase_timestamp` | Format `YYYY-MM-DD`; range from `2016-09-04` to `2018-10-17` |
| `year` | `INT` | Year (YYYY) | Extracted from `order_date` | Range: `2016–2018` |
| `month` | `INT` | Month (1–12) | Extracted from `order_date` | `1–12` |
| `quarter` | `INT` | Quarter (Q1–Q4) | Calculated from `month` | `1–4` |
| `day_of_week` | `INT` | Day of the week | Extracted from `order_date` | `1–7` (`1 = Sunday`, `7 = Saturday`) |

<br>

---

## 🔄 Data Loading and Pipeline

### 📥 Ingestion

To avoid configuring Kaggle API credentials directly in the Databricks environment, the files were downloaded manually and saved to GitHub. Databricks Repos keeps a synchronized copy (via git pull) of the GitHub repository directly in the Workspace; from there, the ingestion notebook copies the files to a Unity Catalog Volume (`mvp_pucrio.raw_files.csv_files`).

Script with explanations: [`ingestion.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/ingestion.ipynb)

Final view of the raw_files.csv_files volume in Databricks:

<img width="530" height="513" alt="image" src="https://github.com/user-attachments/assets/185038e6-4f3b-43ce-bf29-39440443a462" />

<br>

### 🥉 Bronze

The tables were created in the bronze layer in their raw format, but with **defined types** (explicit PySpark schemas for each table). Each table receives two control columns: `_source_file` (name of the source CSV file) and `_ingested_at` (load timestamp). Tables are named by removing the prefixes (`olist_`) and suffixes (`_dataset`) from the original file names.
To write the tables, overwrite mode was used in this MVP. However, in a production environment with recurring data updates, an incremental/MERGE strategy would be more appropriate, avoiding reprocessing the entire dataset on every run.

After each table is created, the `check_completion` function verifies that the number of rows in the bronze table matches the number of rows in the source CSV file, confirming that all rows were correctly ingested.

The script for this layer is at: [`bronze.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/bronze.ipynb)

<br>

**Data Catalog — Bronze Layer:**

All tables and columns were documented in Unity Catalog via `COMMENT ON TABLE` and `COMMENT ON COLUMN`. Click the tables below to view details:

<br>
<details>
<summary><strong>📋 customers</strong> — Data on customers who made purchases on the platform. Each record represents a unique customer identified by customer_id.</summary>

| Column | Type | Description | Domain |
|---|---|---|---|
| `customer_id` | `STRING` | Unique customer ID (PK) | Unique alphanumeric identifier |
| `customer_unique_id` | `STRING` | Universal unique customer ID — used to identify the same customer across multiple orders | Unique alphanumeric identifier |
| `customer_zip_code_prefix` | `STRING` | Customer's ZIP code prefix (first 5 digits) | ZIP code prefix (5 digits) |
| `customer_city` | `STRING` | Customer's city of residence | Brazilian cities |
| `customer_state` | `STRING` | Customer's state (UF) of residence | Brazilian state abbreviation (2 letters) |
| `_source_file` | `STRING` | Name of the source CSV file | CSV file name |
| `_ingested_at` | `TIMESTAMP` | Load timestamp | Load timestamp |

</details>
<br>
<details>
<summary><strong>📋 orders</strong> — Orders placed by customers. Contains status information and timestamps of the order lifecycle (purchase, approval, shipping, delivery).</summary>

| Column | Type | Description | Domain |
|---|---|---|---|
| `order_id` | `STRING` | Unique order ID (PK) | Unique alphanumeric identifier |
| `customer_id` | `STRING` | ID of the customer who placed the order (FK to customers) | Unique alphanumeric identifier |
| `order_status` | `STRING` | Current order status | `approved`, `canceled`, `created`, `delivered`, `invoiced`, `processing`, `shipped`, `unavailable` |
| `order_purchase_timestamp` | `TIMESTAMP` | Purchase date and time | Format `YYYY-MM-DD HH:MM:SS` |
| `order_approved_at` | `TIMESTAMP` | Payment approval date and time | Format `YYYY-MM-DD HH:MM:SS`; nullable |
| `order_delivered_carrier_date` | `TIMESTAMP` | Date and time of handover to the carrier | Format `YYYY-MM-DD HH:MM:SS`; nullable |
| `order_delivered_customer_date` | `TIMESTAMP` | Date and time of delivery to the customer | Format `YYYY-MM-DD HH:MM:SS`; nullable |
| `order_estimated_delivery_date` | `TIMESTAMP` | Estimated delivery date given to the customer | Format `YYYY-MM-DD HH:MM:SS`; nullable |
| `_source_file` | `STRING` | Name of the source CSV file | CSV file name |
| `_ingested_at` | `TIMESTAMP` | Load timestamp | Load timestamp |

</details>
<br>
<details>
<summary><strong>📋 order_items</strong> — Individual items of each order. An order can contain multiple items. Links orders to products and sellers.</summary>

| Column | Type | Description | Domain |
|---|---|---|---|
| `order_id` | `STRING` | Order ID (composite PK, FK to orders) | Unique alphanumeric identifier |
| `order_item_id` | `INT` | Sequential number of the item within the order | Integer >= 1 |
| `product_id` | `STRING` | Product ID (composite PK, FK to products) | Unique alphanumeric identifier |
| `seller_id` | `STRING` | Seller ID (composite PK, FK to sellers) | Unique alphanumeric identifier |
| `shipping_limit_date` | `TIMESTAMP` | Deadline for the seller to hand the product to the carrier | Format `YYYY-MM-DD HH:MM:SS` |
| `price` | `DOUBLE` | Item unit price (R$) | Positive value |
| `freight_value` | `DOUBLE` | Freight value for this item (R$) | Value >= 0 |
| `_source_file` | `STRING` | Name of the source CSV file | CSV file name |
| `_ingested_at` | `TIMESTAMP` | Load timestamp | Load timestamp |

</details>
<br>
<details>
<summary><strong>📋 products</strong> — Catalog of products available on the platform. Includes physical and descriptive characteristics of the products.</summary>

| Column | Type | Description | Domain |
|---|---|---|---|
| `product_id` | `STRING` | Unique product ID (PK) | Unique alphanumeric identifier |
| `product_category_name` | `STRING` | Product category (in Portuguese) | 74 categories in Portuguese; nullable |
| `product_name_lenght` | `INT` | Length of the product name (number of characters) | Integer >= 0; nullable |
| `product_description_lenght` | `INT` | Length of the product description (number of characters) | Integer >= 0; nullable |
| `product_photos_qty` | `INT` | Number of product photos | Integer >= 0; nullable |
| `product_weight_g` | `INT` | Product weight (grams) | Integer > 0; nullable |
| `product_length_cm` | `INT` | Product length (cm) | Integer > 0; nullable |
| `product_height_cm` | `INT` | Product height (cm) | Integer > 0; nullable |
| `product_width_cm` | `INT` | Product width (cm) | Integer > 0; nullable |
| `_source_file` | `STRING` | Name of the source CSV file | CSV file name |
| `_ingested_at` | `TIMESTAMP` | Load timestamp | Load timestamp |

</details>
<br>
<details>
<summary><strong>📋 sellers</strong> — Sellers/suppliers registered on the platform. Each seller can sell multiple products.</summary>

| Column | Type | Description | Domain |
|---|---|---|---|
| `seller_id` | `STRING` | Unique seller ID (PK) | Unique alphanumeric identifier |
| `seller_zip_code_prefix` | `STRING` | Seller's ZIP code prefix (first 5 digits) | ZIP code prefix (5 digits) |
| `seller_city` | `STRING` | City where the seller is located | Brazilian cities |
| `seller_state` | `STRING` | State (UF) where the seller is located | Brazilian state abbreviation (2 letters) |
| `_source_file` | `STRING` | Name of the source CSV file | CSV file name |
| `_ingested_at` | `TIMESTAMP` | Load timestamp | Load timestamp |

</details>
<br>
<details>
<summary><strong>📋 marketing_qualified_leads</strong> — Marketing qualified leads (MQLs). Prospects who showed interest and were qualified by the marketing team.</summary>

| Column | Type | Description | Domain |
|---|---|---|---|
| `mql_id` | `STRING` | Unique marketing qualified lead ID (PK) | Unique alphanumeric identifier |
| `first_contact_date` | `DATE` | Date of first contact with the lead | Format `YYYY-MM-DD` |
| `landing_page_id` | `STRING` | ID of the lead's source landing page | Alphanumeric identifier |
| `origin` | `STRING` | Lead source channel | `direct_traffic`, `display`, `email`, `organic_search`, `other`, `other_publicities`, `paid_search`, `referral`, `social`, `unknown`; nullable |
| `_source_file` | `STRING` | Name of the source CSV file | CSV file name |
| `_ingested_at` | `TIMESTAMP` | Load timestamp | Load timestamp |

</details>
<br>
<details>
<summary><strong>📋 closed_deals</strong> — Closed deals. Contains information on leads that converted into active sellers on the platform.</summary>

| Column | Type | Description | Domain |
|---|---|---|---|
| `mql_id` | `STRING` | Lead ID (PK, FK to marketing_qualified_leads) | Unique alphanumeric identifier |
| `seller_id` | `STRING` | ID of the seller resulting from the conversion (FK to sellers) | Unique alphanumeric identifier |
| `sdr_id` | `STRING` | ID of the responsible SDR (Sales Development Representative) | Alphanumeric identifier |
| `sr_id` | `STRING` | ID of the responsible SR (Sales Representative) | Alphanumeric identifier |
| `won_date` | `TIMESTAMP` | Date and time the deal was closed | Format `YYYY-MM-DD HH:MM:SS` |
| `business_segment` | `STRING` | Seller's business segment | Free text (e.g., `pet`, `health_beauty`, `electronics`) |
| `lead_type` | `STRING` | Lead type/size | `online_small`, `online_medium`, `online_big`, etc. |
| `lead_behaviour_profile` | `STRING` | Lead's behavioral profile during the sales process | Free text; nullable |
| `has_company` | `BOOLEAN` | Indicates whether the seller has a CNPJ (Brazilian company registration) | `true`/`false`; nullable |
| `has_gtin` | `BOOLEAN` | Indicates whether the seller has GTIN codes on their products | `true`/`false`; nullable |
| `average_stock` | `STRING` | Average stock declared by the seller | Free text; nullable |
| `business_type` | `STRING` | Business type | `reseller`, `manufacturer`, etc.; nullable |
| `declared_product_catalog_size` | `DOUBLE` | Declared product catalog size | Value >= 0; nullable |
| `declared_monthly_revenue` | `DOUBLE` | Declared monthly revenue (R$) | Value >= 0; nullable |
| `_source_file` | `STRING` | Name of the source CSV file | CSV file name |
| `_ingested_at` | `TIMESTAMP` | Load timestamp | Load timestamp |

</details>

<br>

Final view of the bronze layer in Databricks:

<img width="468" height="392" alt="image" src="https://github.com/user-attachments/assets/b081d00b-fe07-45f3-9a01-2d78118acf58" />

<br>

### 🥈 Silver

Script: [`silver.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/silver.ipynb)

Before any transformation, a data diagnosis is performed (nulls, duplicates, inconsistencies) to know what needs to be cleaned.

<br>

**Diagnosis (raw data in the Bronze layer):**

| Table | Column | Nulls found |
|---|---|---|
| `products` | `product_category_name` | 610 |
| `marketing_qualified_leads` | `origin` | 60 |
| Other tables and columns | — | 0 |

- **Duplicates:** no table showed duplicates in its primary keys.
- **Referential integrity:** all FK relationships were intact, except `seller_id` in the `closed_deals` table: 462 sellers in this table do not exist in the `sellers` table. These sellers closed marketing deals but probably never actually sold anything. They were not added to the `sellers` table due to a lack of additional information (city, state) and the absence of associated sales records.
- **Categorical value consistency:** states (Brazilian UFs) in `customers` and `sellers` were also validated, as were the order statuses in `orders` (`delivered`, `shipped`, `canceled`, `processing`, `unavailable`, `invoiced`, `created`, `approved`). No invalid values were found. Cities (`seller_city`, `customer_city`) were not validated for format/standardization, since they are not used in the final analysis. However, in a production scenario, it would be advisable to validate city names against a **reference table** (e.g., IBGE municipalities) to ensure that spelling variations (e.g., "sao paulo" vs "são paulo") are normalized.
- **Aggregation required in `order_items`:** records grouped by `order_id + product_id + seller_id`. These items were aggregated by summing `price` (as `sales_value`) and counting the number of rows (to form the `quantity` column).

<br>

**Transformations applied:**

1. Removal of columns outside the scope of the final model (e.g., `freight_value`, `shipping_limit_date`, `customer_unique_id`, etc.)
2. Grouping of `order_items` by `order_id + product_id + seller_id`, summing `price` into `sales_value` and counting rows into `quantity` (112,650 → 102,425 rows)
3. Replacement of missing values with `"unknown"` (610 in `product_category_name`, 60 in `origin`)
4. Definition of explicit types (e.g., `order_purchase_timestamp` → `DATE` as `order_date`)
5. Standardization of product category names in `products`: `eletrodomesticos_2` → `eletrodomesticos`, `casa_conforto_2` → `casa_conforto`, `construcao_ferramentas_jardim` → `ferramentas_jardim`, `alimentos` and `bebidas` → `alimentos_bebidas` (73 → 69 categories)

<br>

**Post-cleaning verification:**

| Check | Result |
|---|---|
| Nulls in all silver tables | ✅ 0 |
| Duplicates in all silver tables | ✅ 0 |

<br>

**Checksum (validation of the aggregation):**

| Source | Sum of `sales_value` |
|---|---|
| `bronze.order_items` (before) | R$ 13,591,643.70 |
| `silver.order_items` (after) | R$ 13,591,643.70 |
| **Difference (checksum)** | **0** ✅ |

The aggregation did not change the total sales value — no sales were lost or duplicated.

<br>

Final view of the silver layer in Databricks:

<img width="468" height="388" alt="image" src="https://github.com/user-attachments/assets/eaebac2b-927b-4ccc-9abe-72f5e9d8c6b2" />

<br>

### 🥇 Gold

In the gold layer, tables are created and documented according to the star schema defined above, with Primary Key/Foreign Key constraints (informational only — not enforced by Databricks).

Script: [`gold.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/gold.ipynb) (also includes the data quality checks)

<br>

**Creation of `dim_leads`** — the `LEFT JOIN` with `closed_deals` preserves the leads that did not convert, filling `won_date` and `sales_cycle` with `NULL`. This enables full-funnel analyses — from first contact to conversion — without losing leads that did not advance in the funnel:

```sql
CREATE OR REPLACE TABLE dim_leads AS
SELECT 
    mql.mql_id,
    mql.origin,
    mql.first_contact_date,
    CAST(cd.won_date AS DATE) AS won_date,
    DATEDIFF(CAST(cd.won_date AS DATE), mql.first_contact_date) AS sales_cycle
FROM silver.marketing_qualified_leads mql
LEFT JOIN silver.closed_deals cd
    ON mql.mql_id = cd.mql_id;
```

<br>

**Creation of `fato_vendas`** — the fact table involves three joins:

1. `INNER JOIN` between `orders` and `order_items` — each row of the fact table represents an item sold within an order.
2. `LEFT JOIN` with `closed_deals` **at the seller level** (by `seller_id`, not by `order_id`) — every sale by a seller inherits the same `mql_id`, i.e., the acquisition channel is attributed to the seller, not to the individual sale. Sellers without a tracked lead receive `"unknown"`.
3. `LEFT JOIN` with `customers` (by `customer_id`) — brings in `purchase_state` (the customer's state), representing where the product was sold.

The platform commission is fixed at 10% of the sale value: `ROUND(sales_value * 0.10, 2)`.

> 📌 **Order filter:** orders with `order_status` = `canceled` or `unavailable` are excluded from the fact table via `WHERE`, since they do not represent real sales.

```sql
CREATE OR REPLACE TABLE fato_vendas AS
SELECT 
    o.order_id,
    COALESCE(c.mql_id, "unknown") AS mql_id,
    oi.seller_id,
    cust.customer_state AS purchase_state,
    oi.product_id,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date,
    COALESCE(o.order_status, 'unknown') AS order_status,
    oi.quantity,
    oi.sales_value,
    ROUND(oi.sales_value * 0.10, 2) AS commission
FROM silver.orders o
INNER JOIN silver.order_items oi 
    ON o.order_id = oi.order_id
LEFT JOIN silver.closed_deals c 
    ON oi.seller_id = c.seller_id
LEFT JOIN silver.customers cust
    ON o.customer_id = cust.customer_id
WHERE o.order_status NOT IN ('canceled', 'unavailable');
```

<br>

**Checksum (validation of the JOINs):**

Comparing the sum of `sales_value` between `silver.order_items` and `gold.fato_vendas` (valid orders only — `canceled` and `unavailable` excluded from fato_vendas):

| Comparison | Result |
|---|---|
| `SUM(silver.order_items.sales_value) − SUM(fato_vendas.sales_value)` | **0** ✅ |

The `JOIN`s did not duplicate or lose records in the fact table.

<br>


Final view of the gold layer in Databricks:

<img width="662" height="489" alt="image" src="https://github.com/user-attachments/assets/8ed287d8-8117-4c77-bcc2-ebe08a2d1a90" />


> 📌 The utility functions shared between silver and gold are in the notebook [`common.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/common.ipynb), which is executed inside each notebook.

<br>

---

## ✅ Data Quality

Completeness, consistency, uniqueness, and accuracy of the data were checked, along with the identification of outliers that could distort statistical analyses. No adjustments were made — everything is documented at the **end of the `gold` notebook**.

Script: [`gold.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/gold.ipynb) (also includes the data quality checks)

<details>
<summary><strong>📋 Details of the quality checks (click here) </strong></summary>

| Dimension | Result |
|---|---|
| **Completeness** | ✅ No table had null values in the model columns. The cleaning done in the Silver layer (missing values filled with `"unknown"`) ensured completeness through to the final layer. |
| **Consistency** | ✅ Validated at two levels: (1) referential integrity checked via `LEFT ANTI JOIN` between `fato_vendas` and each dimension: `product_id` → `dim_products`, `seller_id` → `dim_sellers`, `order_date` → `dim_dates` — all returned zero orphans, confirming that every foreign key in the fact table has a match in the dimension. The `mql_id` → `dim_leads` FK was not checked because the `"unknown"` value is intentional: sellers that did not come from tracked leads receive this value via `COALESCE` when `fato_vendas` is created, and it does not represent an integrity violation, but rather a documented design decision; (2) the checksum between `silver.order_items` and `fato_vendas` confirmed that the joins did not alter the sales value (valid orders only — `canceled` and `unavailable` excluded from `fato_vendas`). The consistency of categorical values (states, order statuses) was already verified in the Silver layer and carries over to Gold by inheritance. |
| **Uniqueness** | ✅ There are no duplicate rows or violations of the composite primary key (`order_id + product_id + seller_id`) in the `fato_vendas` table. |
| **Temporal accuracy** | ⚠️ `dim_leads` had 17 converted-lead records with a `won_date` later than the last sales date recorded in the dataset, and 1 record with a `won_date` earlier than `first_contact_date` (a logical inconsistency — a deal cannot be closed before first contact). It was decided to keep these records and document the limitation, since they represent less than 0.1% of the lead base and do not affect the sales metrics (`fato_vendas`) — only specific sales cycle analyses that use these isolated cases. |
| **Outliers** | ⚠️ 4,195 rows (~4% of `fato_vendas`) have a unit price outside the expected IQR range for the respective product. When inspecting the highest-value cases, they correspond to categories consistent with high prices (electronics, watches, computing, construction tools), with `quantity=1` — suggesting legitimate price variation (different versions/models of the same `product_id`, or price changes over time) rather than typing errors. It was decided not to remove these rows, but to record the decision. |
| **`sales_value` validation** | ✅ Since `canceled` and `unavailable` orders were excluded from `fato_vendas`, there are no negative values in `sales_value` (verified). All sales in the fact table represent real transactions. |

<br>

**Code for the temporal accuracy and outlier checks:**

```python
def check_temporal_accuracy(df, table_name, last_date):
    """Verifica se datas fazem sentido no contexto do negócio."""
    # 1. Datas no futuro (impossível)
    # 2. Won_date deve ser >= first_contact_date (para leads convertidos)
    ...

def detect_outliers(df, table_name):
    """Detecta outliers em preços unitários usando método IQR (Interquartile Range)."""
    # Calcula Q1, Q3 por produto e filtra linhas fora de [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
    ...

def check_referential_integrity(df, table_name):
    """Verifica integridade referencial: toda FK da fato_vendas deve existir na dimensão correspondente."""
    # LEFT ANTI JOIN para cada FK: product_id → dim_products, seller_id → dim_sellers, order_date → dim_dates
    # mql_id não é verificado — o valor "unknown" é intencional (sellers sem lead rastreado)
    ...
```

<br>

**Check output:**

- Temporal accuracy: 17 rows with `won_date` in the future + 1 with `won_date < first_contact_date`
- Outliers: 4,195 rows detected (high-price categories: electronics, watches, computing)
- Referential integrity: ✅ zero orphans for `product_id` → `dim_products`, `seller_id` → `dim_sellers`, `order_date` → `dim_dates` (`mql_id` not checked — "unknown" is intentional)
- Final result: 🎉 No tables with critical problems — the ones found were documented and kept

</details>

<br>

---

## 📈 Data Analysis

Script: [`analise.ipynb`](https://github.com/CY-PI/_DataEng_PUCRIO/blob/main/analise.ipynb)

Below are the answers to the project's initial questions, with insights and recommended actions.
<br>

### 1️⃣ Which months have the highest peaks?
<br>

<details>
<summary><strong>📊 Monthly sales — total revenue per month (click here for output) </strong></summary>

<img width="828" height="349" alt="image" src="https://github.com/user-attachments/assets/e35e01d6-e2f3-4784-8d17-5501b7887678" />


</details>

**November 2017 (R$ 1M)** — peak driven by Black Friday. Sales grow gradually from Oct 2016 to mid-2018, but fall 15% between Jul and Sep 2018 (from R$ 1M to R$ 850K), suggesting seasonality or loss of traction.

> 📊 **Suggested period for analysis:** Sep 2017 to Aug 2018 (one full year, after the platform's initial "stabilization").

<br>

💡 **Recommendation:** Reinforce seller retention and reactivation efforts in the post-Black Friday period (Dec–Feb), historically the weakest; investigate whether the 2018 decline persists in the following months to distinguish seasonality from loss of traction.

> ⚠️ A longer period would be needed (3 to 5 years from 2018 onward) to effectively understand whether there are recurring sales peaks. The **next analyses** focus on the period from September 2017 to August 2018, to have a full year.

<br>

### 2️⃣ Where does the revenue come from? Does the Pareto rule apply to sellers?

**Yes.** 19.3% of sellers (512 of 2,653) account for 80% of sales, confirming Pareto. Seller #1 alone accounts for R$ 203K (2% of the total). Among the top 10, the average ticket varies from R$ 67 to R$ 581, revealing two distinct business models among the largest sellers. Since the commission is fixed at 10%, the top sellers are also the ones that generate the most revenue for the platform (top seller: R$ 20.4K in commission over the period). The remaining 81% (2,141 sellers) share only 20% of sales — a long tail typical of marketplaces.


<br>
<details>
<summary><strong>📊 Summary: Top 19% vs Remaining 81% (click here for details)</strong></summary>

| Group | # Sellers | % Sellers | Orders | Sales | % Sales | Commission | Average ticket |
|---|---|---|---|---|---|---|---|
| Top 19% | 512 | 19.3% | 56,646 | R$ 8,294,050.98 | 80% | R$ 829,425.36 | R$ 324.22 |
| Remaining 81% | 2,141 | 80.7% | 19,813 | R$ 2,074,772.89 | 20% | R$ 207,483.75 | R$ 159.54 |

</details>
<br>
<details>
<summary><strong>📊 Top sellers (click here for output)</strong></summary>


<img width="989" height="419" alt="image" src="https://github.com/user-attachments/assets/c6c5bc57-79bd-44ad-ae0c-2d0302f90ca2" />


</details>

<br>

💡 **Recommendation:** Although the Pareto rule is confirmed, revenue is well distributed within the group of 512 top-selling sellers (the largest accounts for only 2% of the total), so it is advisable to closely monitor their performance and act quickly if any of them starts selling less. For the rest, a deeper analysis is recommended to understand whether they have few registered products, whether pricing is not competitive, whether there is a lack of promotion, or another reason, and, based on that, define an action plan.

<br>

### 3️⃣ Which product categories bring in the most revenue?

**beleza_saude (health & beauty) leads with R$ 1.01M** (7,027 orders), followed by relogios_presentes (watches & gifts) (R$ 989K, 4,798 orders). Cama_mesa_banho (bed, bath & table), esporte_lazer (sports & leisure), and informatica_acessorios (computer accessories) complete the top 5 (total = R$ 4.2M).
<br> 

<details>
<summary><strong>📊 Categories by revenue (click here for output)</strong></summary>

<img width="673" height="348" alt="image" src="https://github.com/user-attachments/assets/88ec4dc3-0008-4f0e-94ac-4b1f296b79b1" />


</details>

These 5 categories represent ~43% of the total sold, which indicates a diversified mix, with no critical dependence on a single category.

<br>

💡 **Recommendation:** Invest in categories with a high ticket but low volume (e.g., PCs, R$ 1,223 average ticket, but only 150 orders in the period) to increase revenue without hurting margin.

<br>

### 4️⃣ Is any marketing channel better (conversion and sales cycle)?

**Paid_search (12.3%) and organic_search (11.8%)** lead in conversion and lead volume. Social has low conversion (5.6%) but brings 75 conversions thanks to its high volume. Display has a short cycle (10 days vs 50–60 for the leaders) but only 6 total conversions.

<details>
<summary><strong>📊 Full output by channel (click here for output)</strong></summary>

<img width="977" height="179" alt="image" src="https://github.com/user-attachments/assets/f9afdda0-43ff-4691-8b61-2689321f35c1" />


> The `unknown`, `other`, and `other_publicities` channels were excluded from the analysis because they are not traceable.

</details>

<br>

💡 **Recommendations:**

- **Prioritize investment in paid_search and organic_search** — best ROI (higher conversion + high volume). Paid_search already brings 195 conversions; increasing investment could scale it further.
- **Optimize social** — it has high volume (1,350 leads) but conversion at half the average (5.6%). Improving lead quality or the post-click journey could double conversions without increasing acquisition cost.
- **Reassess display** — the short cycle (10 days) is attractive, but a 5.1% conversion and only 6 total conversions suggest low return. Consider reallocating budget to higher-converting channels.

<br>

### 5️⃣ Is there a relationship between low revenue and a higher risk of churn? (LTV, churn)

**Yes, a strong relationship.** Active sellers (81.6%) have an LTV 6.3x higher than churned ones: R$ 462 vs R$ 73. Active sellers make 34 orders on average vs only 4 for churned ones. Churn is associated with low engagement in the first sales.

<details>
<summary><strong>📊 Full output: Active vs churned sellers (click here for output)</strong></summary>

> Churn criterion: no sales for more than 180 days (reference: 08/31/2018).

<img width="889" height="74" alt="image" src="https://github.com/user-attachments/assets/62c9f7b0-01be-4e2c-b527-714e80376a26" />

</details>

<br>

💡 **Recommendation:** Since churn is linked to sellers who fail to sell well early on, it would make sense to automatically identify those who made a first sale but not a second one after some time (e.g., 90 days) and reach out offering help.

<br>

---

## 📝 Self-Assessment

In this project, I learned to use Databricks and apply the medallion architecture.
I was able to answer most of the questions that were part of the objective of this work, with the caveat that the dataset's time span was short, with the first year still showing a growth stage of the platform.
One of the problems I faced was some indecision regarding the data structure and the presentation of the notebooks.
I started the star model by including dim_customer. However, this table was not actually used — only one of its fields was relevant. I chose to remove it; perhaps if I later seek more granularity in the analyses, it will be worth having it there.
As for presentation, I first opted for a single notebook that included everything. But it became extremely long and cluttered. Midway through the project, I decided to build one notebook for each stage of the project.
What stood out to me the most, however, was the experience of using AI as a work tool. I started by asking "Genie" to validate how I was planning to begin the project, but I received a good part of the code ready-made. My first reaction was negative; I was irritated because the point of the MVP was for me to do the project. Genie deleted everything and began to accompany me through the build. Over time, I realized that repetitive tasks, particularly table documentation, could be delegated without my losing control. As I became more comfortable with Databricks and realized how much time AI saved, I started using it with more confidence, including when I decided to drastically restructure the notebooks and remove tables. Without Genie, it would have taken me much longer to make these changes.
In an industry that values experience with AI in automating projects and analyses, seen in almost all job postings in the field, this project was extremely important to me. In the end, I felt that I was the one doing the thinking and saving time because I had AI doing the heavy lifting.
As future work, I could:
- Reintroduce dim_customer if future analyses need more granularity;
- Migrate the Bronze layer load from overwrite to an incremental/MERGE model, which would be better suited to a production scenario with recurring updates;
- Investigate a longer data period, to identify any seasonality with more confidence.

<br>

---
<a id="databricks-jobs-pipeline"></a>
## ⚙️ Using Databricks Jobs & Pipeline

Out of curiosity, to automate the execution of the data pipeline, a job was created using the **Jobs & Pipelines** feature of Databricks.

<img width="548" height="254" alt="image" src="https://github.com/user-attachments/assets/666639ac-b7aa-4550-8b7b-ab2d8129c43f" />
