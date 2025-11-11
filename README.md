# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project showcases a complete data warehousing and analytics solution, built using SQL Server. It covers the entire process — from data integration and ETL development to data modeling and business-ready insights. Designed as a portfolio project, it reflects best practices in data engineering, warehousing, and analytics.

---
## 🏗️ Data Architecture
The data architecture for this project is designed using the Medallion Architecture framework, consisting of **Bronze**, **Silver**, and **Gold layers**:

1. **Bronze Layer**: Ingests and stores raw data directly from CRM and ERP sources (CSV files) into the SQL Server database without any transformations.
2. **Silver Layer**: Performs data cleansing, standardization, normalization, and enrichment to create consistent and reliable datasets ready for integration.
3. **Gold Layer**: Contains business-ready, analytical data modeled using a Star Schema for reporting, visualization, and advanced analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

---

## 🛠️ Tools Used

- **SQL Server** – For data storage, ETL, and modeling.
- **Notion** – For project planning and documentation.
- **Draw.io** – For data architecture and ER diagram design.

---

## 🧩 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective:
Develop a modern data warehouse using SQL Server to consolidate sales data from multiple sources, supporting analytical reporting and informed decision-making.

#### Specifications:

- **Data Sources**: Import data from two systems — ERP and CRM (CSV format).
- **Data Quality**: Clean and standardize data to resolve inconsistencies before analysis.
- **Integration**: Merge both sources into a unified, analytics-ready data model.
- **Scope**: Focus on the latest dataset (no data historization required).
- **Documentation**: Deliver a clear data model reference for business and analytics teams.

  ---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.
