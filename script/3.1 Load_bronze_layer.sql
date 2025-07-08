/*
=====================================================================
Stored Procedure for loading bronze layer data
=====================================================================
Purpose: 
	This is a stored procedure that loads data into bronze schema
	It will execute two functions:
	- Empty all table in bronze schema
	- Refreshes data from source

Actions: Please run code below
EXEC bronze.load_bronze;
*/

-- EXEC bronze.load_bronze;
-- GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT'======================================================';
		PRINT'LOADING DATA in Bronze Layer';
		PRINT'======================================================';

		PRINT'------------------------------------------------------';
		PRINT'Refreshisng data from CRM';
		PRINT'------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>> Inserting data to bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Raymund\OneDrive\Documents\Portfolio Projects\SQL\Data Warehouse\datasets\source_crm\cust_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>> Inserting data to bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Raymund\OneDrive\Documents\Portfolio Projects\SQL\Data Warehouse\datasets\source_crm\prd_info.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
		);

		SET @start_time = GETDATE();
		PRINT'>> Truncating bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>> Inserting data to bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Raymund\OneDrive\Documents\Portfolio Projects\SQL\Data Warehouse\datasets\source_crm\sales_details.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		PRINT'------------------------------------------------------';
		PRINT'Refreshisng data from CRM';
		PRINT'------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT'>> Inserting data to bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\Raymund\OneDrive\Documents\Portfolio Projects\SQL\Data Warehouse\datasets\source_erp\CUST_AZ12.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT'>> Inserting data to bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\Raymund\OneDrive\Documents\Portfolio Projects\SQL\Data Warehouse\datasets\source_erp\LOC_A101.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT'>> Inserting data to bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\Raymund\OneDrive\Documents\Portfolio Projects\SQL\Data Warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'
	END TRY
	BEGIN CATCH
		PRINT'========================================================';
		PRINT'ERROR during loading of bronze layer';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'========================================================';
	END CATCH
	SET @batch_end_time = GETDATE();
	PRINT'>> Total time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT'End';
END