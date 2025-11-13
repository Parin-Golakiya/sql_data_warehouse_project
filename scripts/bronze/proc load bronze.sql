/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure automates the process of loading raw data from external 
    CSV files into the 'bronze' schema of the Data Warehouse. 

    The procedure ensures that the bronze tables always contain the latest 
    extracted data from source systems by performing the following actions:
      1. Truncates existing data from all bronze tables to prepare for fresh load.
      2. Uses the `BULK INSERT` command to efficiently import CSV data 
         into the corresponding bronze tables.

Parameters:
    None — this stored procedure does not accept any input parameters 
    or return output values.

Data Flow:
    Source Files (CSV) -> Bronze Schema Tables (Raw Data)

Usage Example:
    EXEC bronze.load_bronze;

Warning:
    This procedure truncates all data from the bronze tables before loading new data. 
    Ensure that source files are complete and verified before execution.
===============================================================================
*/

use DataWarehouse;

EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		print '===================================';
		print 'Loading Bronze Layer';
		print '===================================';

		print '-----------------------------------'
		print 'Loading CRM Tables'
		print '-----------------------------------'

		SET @start_time = GETDATE();
		print '>> Truncating Tables: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		print'>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info 
		from 'C:\Users\parin\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		SET @start_time = GETDATE();
		print '>> Truncating Tables: crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		print '>> Inserting Data Into Tables: crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\parin\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		SET @start_time = GETDATE();
		print '>> Truncating Tables: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		print '>> Inserting Data Into: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\parin\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';

		print '-----------------------------------'
		print 'Loading CRM Tables'
		print '-----------------------------------'

		SET @start_time = GETDATE();
		print '>> Truncating Table: erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		print '>> Inserting Data Into: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\parin\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		SET @start_time = GETDATE();
		print '>> Truncating Table: erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		print '>> Inserting Data Into: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\parin\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';


		SET @start_time = GETDATE();
		print '>> Truncating Table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		print '>> Inserting Data Into: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\parin\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar) + ' second';
		print '------------------';

		SET @batch_end_time = GETDATE();
		PRINT '========================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as nvarchar) + ' second';
		PRINT '=========================================';
	END TRY
	BEGIN CATCH 
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + error_message();
		PRINT 'Error Message' + cast (error_number() as nvarchar);
		PRINT 'Error Message' + cast (error_state() as nvarchar);
		PRINT '========================================='
	END CATCH
END