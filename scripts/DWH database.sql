/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script initializes the core structure of the SQL Data Warehouse project.
    It checks if a database named 'DataWarehouse' exists — if it does, the database 
    is dropped and recreated to ensure a clean environment for data ingestion.
    
    Within the database, three schemas are created to represent the data processing layers:
    - bronze:  Raw data layer (data ingested directly from source systems)
    - silver:  Cleansed and transformed data layer
    - gold:    Curated, analytics-ready data layer

Warning:
    Executing this script will permanently drop the 'DataWarehouse' database if it exists. 
    All existing data will be deleted. Ensure backups are taken before running this script.

*/
	

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database if exist
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
create database DataWarehouse;
GO

use DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
