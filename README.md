# Dream Taxi Analytics: End-to-End Enterprise Lakehouse on Azure

An enterprise-grade, metadata-driven, and passwordless Data Lakehouse platform built on Microsoft Azure and Databricks. This project automates the ingestion, transformation, and optimization of transactional ride-hailing data (Careem/Dream Taxi simulated datasets) through Medallion architecture (Bronze, Silver, Gold) and serves optimized analytical insights directly to Power BI.

---

## 📐 Architecture Diagram

```text
 [1. On-Prem / App DB] ────(ADF Metadata CDC Ingestion)────> [2. ADLS Gen2 - Bronze]
  - Azure SQL (books DB)                                       - Raw CSV Ingestion
  - control.IngestionMetadata                                  - Managed Identities (SAMI)
                                                                       │
                                                         (Generic PySpark Ingestion)
                                                                       ▼
 [4. Analytical Serving Layer] <───(JDBC Write)─── [3. Gold Star Schema] <── [ADLS Gen2 - Silver]
  - Azure SQL Database                             - Databricks SQL        - Delta Lake Format
  - gold.dim_customers                             - dim_drivers           - OPTIMIZE & Z-ORDER
  - gold.fact_trips                                - fact_trips
            │
            ▼
     [5. Power BI]
   - Executive Dashboard


🌟 Key Technical Highlights
1. Zero-Trust, Passwordless Security Model
No Hardcoded Secrets: Completely eliminated storage keys and database passwords from
the codebase using Azure System-Assigned Managed Identities (SAMI) and Azure Access Connectors.
Secret Templating: Orchestrated credential rotation securely by referencing Azure Key Vault
secrets within the Databricks cluster-level Spark configurations using the {{secrets/scope/key}}
format, ensuring zero credential exposure on Git commits.
2. Metadata-Driven Orchestration (ADF)
Dynamic Pipelines: Designed a single, generic ADF pipeline using a ForEach loop that dynamically
ingests any number of source tables based on configurations inside a SQL control.IngestionMetadata
table.
Change Tracking (CDC): Configured incremental ingestion for high-volume transactions (Trips) using
SQL Server’s built-in Change Tracking engine, updating the system LastChangeVersion automatically
on every run.
Automated Error Monitoring: Configured an event-driven Azure Logic App (Consumption) connected to
ADF Web Activities via failure dependencies (Red Arrows) to send immediate email notifications
with the exact failure Run IDs.
3. High-Performance PySpark Engine (Silver Layer)
Dynamic Data Sanitization: Developed a highly optimized PySpark notebook (STG_Bronze_to_Silver_Generic)
that sanitizes column headers (lowercase, trimming, character replacement) dynamically across all
incoming datasets.
Parallel Execution Plan: Optimized memory usage and lineage execution by building a list of Spark Column
expressions and executing them simultaneously using .select(*clean_expressions) instead of slow
.withColumn loops.
Type-Based Data Cleansing: Configured automatic handling of strings (nulls to "N/A", whitespace trim),
financials (regex stripping of currency signs, casting to high-precision DecimalType(18,2)), and
timestamps (handling malformed strings safely using try_to_timestamp to prevent pipeline crashes).
File Compaction & Z-Ordering: Implemented automated Delta Lake file compaction and Z-ORDER BY on
primary keys to sort data on disk and minimize physical storage scanning.
4. Hybrid Slowly Changing Dimensions (Gold Layer)
Single-Pass SCD Type 1 & Type 2 Merge: Implemented an advanced SQL MERGE statement inside Databricks
using the UNION ALL / NULL merge key trick to execute both history tracking (SCD Type 2 for customer city relocations)
and in-place updates (SCD Type 1 for names, emails, and phone numbers) in a single, atomic
database transaction.
Star Schema Optimization: Modeled the final clean tables into a classical Star Schema
(gold.dim_customers, gold.dim_drivers, gold.dim_locations, gold.fact_trips) using
surrogate keys (_sk), loading them back to Azure SQL via PySpark JDBC for fast
Power BI consumption.

📂 Repository Structure

.
├── adf/                        # Azure Data Factory Source Code (Managed via Git Integration)
│     ├── pipeline/             # Ingestion and Orchestration pipelines
│     ├── dataset/              # Parameterized SQL and ADLS Gen2 datasets
│     └── trigger/              # Automated daily schedule triggers
│
├── databricks/                 # Databricks Notebooks (Managed via Databricks Repos)
│     ├── 01_silver/
│     │     └── STG_Bronze_to_Silver_Generic (Python)
│     │
│     ├── 02_gold/
│     │     ├── gold_dim_customers (SQL)
│     │     ├── gold_dim_drivers (SQL)
│     │     ├── gold_dim_locations (SQL)
│     │     └── gold_fact_trips (SQL)
│     │
│     └── 03_presentation/
│           ├── Executive_Sales_Summary (SQL Notebook)
│           └── Dream_Taxi_Executive_Dashboard.lvdash.json # Version-controlled dashboard
│
└── power_bi/
      └── Dream_Taxi_Analytics_Dashboard.pbix # Final Interactive BI Dashboard

📊 Analytical Insights Delivered (Power BI)
Executive Revenue (CEO View): Tracks Month-over-Month (MoM) revenue growth, total trip volume,and
average ticket sizes across different customer segments.
Regional Performance (Operations View): Displays geographic heatmaps of ride origins and destinations
using Power BI map hierarchies, highlighting the top 10 busiest pickup zones.
Peak Demand Analytics: Visualizes hourly traffic curves, allowing the revenue team to isolate the morning
and evening commute peaks for dynamic surge pricing.
Unit Economics (Finance View): Segments trips by distance (Short, Medium, Long) to calculate the average
net commission (20%) and revenue earned per kilometer.

-- =========================================================================
-- POWER BI DAX MEASURES 
-- =========================================================================

-- 1. Total Revenue KPI
Total Revenue = SUM(fact_trips[fare_amount])

-- 2. Total Trips KPI
Total Trips = COUNT(fact_trips[trip_id])

-- 3. Unique Active Customers KPI
Unique Active Customers = DISTINCTCOUNT(fact_trips[customer_sk])

-- 4. Average Ticket Size (Average Fare per Ride)
Average Ticket Size = DIVIDE([Total Revenue], [Total Trips], 0)

-- 5. Company Net Commission Revenue (20% Careem Standard)
Company Net Revenue = [Total Revenue] * 0.20

-- 6. Average Revenue Earned per Kilometer
Revenue per KM = DIVIDE([Total Revenue], SUM(fact_trips[distance_km]), 0)

-- 7. Month-over-Month (MoM) Revenue Growth Percentage
MoM Revenue Growth % = 
VAR CurrentMonthRevenue = [Total Revenue]
VAR PreviousMonthRevenue = 
    CALCULATE(
        [Total Revenue], 
        DATEADD(fact_trips[trip_start_time].[Date], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentMonthRevenue - PreviousMonthRevenue, PreviousMonthRevenue, 0)
