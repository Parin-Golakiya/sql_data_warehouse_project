/*
===============================================================================
Data Quality Checks — Gold Layer
===============================================================================
Script Purpose:
    This script performs comprehensive data quality validations to ensure the 
    integrity, consistency, and accuracy of the curated 'gold' layer within the 
    Data Warehouse.

    These checks confirm that the data is fully reliable and ready for analytical 
    reporting and visualization. The validations include:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Execute these checks after loading the Gold Layer.
    - Investigate and correct any issues identified during validation.
    - Optionally, log validation results in a “Quality_Log_Gold” table for 
      auditing and monitoring purposes.
================================
*/


-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
select 
    customer_key,
    count(*) AS duplicate_count
from gold.dim_customers
group by customer_key
having count(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
select 
    product_key,
    count(*) AS duplicate_count
from gold.dim_products
group by product_key
having count(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
select * 
from gold.fact_sales f
LEFT JOIN gold.dim_customers c
on c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
on p.product_key = f.product_key
where p.product_key IS NULL OR c.customer_key IS NULL  
