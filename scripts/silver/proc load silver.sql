
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure executes the ETL (Extract, Transform, Load) process 
    that transfers data from the 'bronze' schema (raw layer) to the 'silver' 
    schema (cleansed layer) within the Data Warehouse.

Data Flow:
    Bronze (Raw Data) -> Silver (Cleansed & Transformed Data)

Parameters:
    None — this stored procedure does not accept input parameters 
    or return output values.

Usage Example:
    EXEC silver.load_silver;

Warning:
    This procedure truncates all data in the 'silver' tables before reloading. 
    Execute only when you are ready to refresh the entire dataset.
===============================================================================
*/



use DataWarehouse;

CREATE or ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		print '===================================';
		print 'Loading Silver Layer';
		print '===================================';

		print '-----------------------------------'
		print 'Loading CRM Tables'
		print '-----------------------------------'

		-- Insert Data Bronze(bronze.crm_cust_info) to Silver(silver.crm_cust_info) data transfer with cleaning and normalize 
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		print '>> Inserting Data Into: silver.crm_cust_info';
		Insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,

		case when upper(trim(cst_marital_status)) = 'S' then 'Single'
			 when upper(trim(cst_marital_status))= 'M' then 'Married'
			 else 'n/a'
		end cst_marital_status,

		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr))= 'M' then 'Male'
			 else 'n/a'
		end cst_gndr,
		cst_create_date
		from(
		select
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null
		)t where flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		-- Insert Data Bronze(bronze.crm_prd_info) to Silver(silver.crm_prd_info) data transfer with cleaning and normalize 
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		print '>> Inserting Data Into: silver.crm_prd_info';
		Insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
			prd_id,
			replace(SUBSTRING(prd_key, 1, 5),'-','_') as cat_id, -- Replace for same in crm & erp (Ecxtract category ID)
			SUBSTRING(prd_key, 7, len(prd_key)) as prd_key, -- Extract pruduct key
			prd_nm,
			isnull(prd_cost,0) as prd_cost,
			case upper(trim(prd_line))
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'n/a'
			end as prd_line,
			cast(prd_start_dt as DATE) as prd_start_dt,
			cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as DATE) as prd_end_dt
		from bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		-- Insert Data Bronze(bronze.crm_sales_details) to Silver(silver.crm_sales_details) data transfer with cleaning and normalize 
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Inserting Data Into: silver.crm_sales_details';
		Insert into silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			 else CAST(cast(sls_order_dt as varchar) as date)
		end sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			 else CAST(cast(sls_ship_dt as varchar) as date)
		end sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			 else CAST(cast(sls_due_dt as varchar) as date)
		end sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
				then sls_quantity * ABS(sls_price)
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0 
				then sls_sales / nullif(sls_quantity,0)
			 else sls_price
		end as sls_price
		from bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		-- Insert Data Bronze(bronze.erp_cust_az12) to Silver(silver.erp_cust_az12) data transfer with cleaning and normalize 
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		print '>> Inserting Data Into: silver.erp_cust_az12';
		Insert into silver.erp_cust_az12 (cid, bdate, gen)
		select
		case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
			 else cid
		end as cid,
		case when bdate > getdate() then null
			 else bdate
		end as bdate,
		case when upper(trim(gen)) in ('F','Female') then 'Female'
			 when upper(trim(gen)) in ('M','Male') then 'Male'
			 else 'n/a'
		end as gen
		from bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';

		-- Insert Data Bronze(bronze.erp_loc_a101) to Silver(bronze.erp_loc_a101) data transfer with cleaning and normalize 
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		print '>> Inserting Data Into: silver.erp_loc_a101';
		Insert into silver.erp_loc_a101
		(cid,cntry)
		select
		replace(cid, '-', '') cid,
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US','USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
			 else trim(cntry)
		end as cntry
		from bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		-- Insert Data Bronze(bronze.erp_px_cat_g1v2) to Silver(bronze.erp_px_cat_g1v2) data transfer with cleaning and normalize 
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		Insert into silver.erp_px_cat_g1v2 
		(id, cat, subcat, maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';

		SET @batch_end_time = GETDATE();
		PRINT '========================================='
		PRINT 'Loading Silver Layer is Completed';
		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as nvarchar) + ' second';
		PRINT '=========================================';
	END TRY
	BEGIN CATCH 
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + error_message();
		PRINT 'Error Message' + cast (error_number() as nvarchar);
		PRINT 'Error Message' + cast (error_state() as nvarchar);
		PRINT '========================================='
	END CATCH
END