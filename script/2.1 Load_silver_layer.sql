/*
=====================================================================
Stored Procedure for loading silver layer data
=====================================================================
Purpose: 
	This is a stored procedure that loads data into silver schema
	It will execute two functions:
	- Empty all table in silver schema
	- Refreshes data from bronze layer tables

Actions: Please run code below;
EXEC silver.load_silver;
*/

-- EXEC silver.load_silver
-- GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT'======================================================';
		PRINT'LOADING DATA in Silver Layer';
		PRINT'======================================================';

		PRINT'------------------------------------------------------';
		PRINT'Refreshisng CRM Data';
		PRINT'------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT'>> Inserting data to silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					 ELSE 'n/a'
				END cst_marital_status,
				CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					 ELSE 'n/a'
				END cst_gndr,
				cst_create_date
				FROM (
					SELECT 
						*,
						ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
					FROM bronze.crm_cust_info
					) AS prelim_query
			WHERE flag = 1 AND cst_id IS NOT NULL;
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'>> Inserting data to silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
				SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
				prd_nm,
				ISNULL(prd_cost,0) AS prd_cost,
				CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
					 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					 ELSE 'n/a'
				END AS prd_line,
				prd_start_dt,
				DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
			FROM bronze.crm_prd_info
			;

		SET @start_time = GETDATE();
		PRINT'>> Truncating silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT'>> Inserting data to silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
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
				SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE WHEN sls_order_dt = 0 OR sls_order_dt < 19000000 
					OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
					END sls_order_dt,
				CASE WHEN sls_ship_dt = 0 OR sls_ship_dt < 19000000 
					OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
					END sls_ship_dt,
				CASE WHEN sls_due_dt = 0 OR sls_due_dt < 19000000 
					OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END sls_due_dt,
				CASE WHEN sls_sales IS NULL OR sls_sales <= 0 
					 THEN sls_quantity * ABS(sls_price)
					 ELSE sls_sales
					 END AS sls_sales,
				sls_quantity,
				CASE WHEN sls_price IS NULL OR sls_price <= 0 
					 THEN ABS(sls_sales) / sls_quantity
					 ELSE sls_price
					 END AS sls_price
				FROM bronze.crm_sales_details
				WHERE sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
				OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
			;
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		PRINT'------------------------------------------------------';
		PRINT'Refreshisng ERP Data';
		PRINT'------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating silver.erp_CUST_AZ12';
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		PRINT'>> Inserting data to silver.erp_CUST_AZ12';
		INSERT INTO silver.erp_CUST_AZ12 (
			CID,
			BDATE,
			GEN
		)
			SELECT
			CASE WHEN TRIM(CID) LIKE 'NAS%' THEN
				 SUBSTRING(CID, 4, LEN(CID))
				 ELSE CID
				 END CID,
			CASE WHEN BDATE > GETDATE() THEN NULL
				 ELSE BDATE
				 END BDATE,
			CASE WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
			     WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
				 ELSE 'n/a'
				 END GEN
			FROM bronze.erp_CUST_AZ12
		;
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating silver.erp_LOC_A101';
		TRUNCATE TABLE silver.erp_LOC_A101;
		PRINT'>> Inserting data to silver.erp_LOC_A101';
		INSERT INTO silver.erp_LOC_A101 (
			CID,
			CNTRY
		)
			SELECT
				REPLACE(CID,'-','') AS CID,
				CASE WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
				     WHEN UPPER(TRIM(CNTRY)) IN ('DE', 'GERMANY') THEN 'Germany'
					 WHEN TRIM(CNTRY) IS NULL OR CNTRY = '' THEN 'n/a'
					 ELSE TRIM(CNTRY)
					 END CNTRY
			FROM bronze.erp_LOC_A101
		;
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating silver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT'>> Inserting data to silver.erp_PX_CAT_G1V2';
		INSERT INTO silver.erp_PX_CAT_G1V2 (
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
			SELECT
				ID,
				CAT,
				SUBCAT,
				MAINTENANCE
			FROM bronze.erp_PX_CAT_G1V2
		;
		SET @end_time = GETDATE();
		PRINT'>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'----------------------'
	END TRY
	BEGIN CATCH
		PRINT'========================================================';
		PRINT'ERROR during loading of silver layer';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'========================================================';
	END CATCH
	SET @batch_end_time = GETDATE();
	PRINT'>> Total time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT'End';
END