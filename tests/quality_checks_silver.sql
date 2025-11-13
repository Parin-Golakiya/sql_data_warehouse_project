/*
===============================================================================
Data Quality Checks — Silver Layer
===============================================================================
Script Purpose:
    This script executes a series of data quality checks to ensure data accuracy, 
    consistency, and standardization within the 'silver' schema of the Data Warehouse.

    The checks are designed to validate the reliability of transformed data before 
    it is moved to the 'gold' (analytics-ready) layer. 

Key Checks Performed:
    - Detection of NULL or duplicate values in primary key columns.
    - Identification of unwanted leading/trailing spaces in string fields.
    - Validation of standardized categorical values (e.g., gender, status codes).
    - Verification of valid date ranges and chronological order.
    - Consistency checks between related fields (e.g., foreign key relationships).

Usage Notes:
    - Run these checks **after loading data into the Silver Layer** and before 
      triggering the Gold Layer load.
    - Any discrepancies or failed checks should be logged, reviewed, and corrected 
      before proceeding to the next ETL stage.
===============================================================================
*/


-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select 
    cst_id,
    count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id is null;

-- Check for Unwanted Spaces
-- Expectation: No Results
select 
    cst_key 
from silver.crm_cust_info
where cst_key != trim(cst_key);

-- Data Standardization & Consistency
select distinct 
    cst_marital_status 
from silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
select 
    prd_id,
    COUNT(*) 
from silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
select 
    prd_nm 
from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
select 
    prd_cost 
from silver.crm_prd_info
where prd_cost < 0 OR prd_cost is null;

-- Data Standardization & Consistency
select distinct 
    prd_line 
from silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
select 
    * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
select 
    NULLIF(sls_due_dt, 0) as sls_due_dt 
from bronze.crm_sales_details
where sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
select 
    * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
select distinct 
    sls_sales,
    sls_quantity,
    sls_price 
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
select distinct 
    bdate 
from silver.erp_cust_az12
where bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
select distinct 
    gen 
from silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
select distinct 
    cntry 
from silver.erp_loc_a101
order by cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
select 
    * 
from silver.erp_px_cat_g1v2
where cat != trim(cat) 
   or subcat != trim(subcat) 
   or maintenance != trim(maintenance);

-- Data Standardization & Consistency
select distinct 
    maintenance 
from silver.erp_px_cat_g1v2;
