-- STORED PROCEDURE
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	-- Track ETL Duration: helps identify bottlenecks, optimiaze performance, monitor trends, detect issues
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY -- RUN try block first and if fails, runs CATCH block to handle the error 
		SET @batch_start_time = GETDATE();
		PRINT '==========================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================';
		-- BULK INSERT & TRUNCATE
		PRINT '--------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------';

		SET @start_time = GETDATE(); -- gets exact time when we loaded this table
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		-- TRUNCATE = quickly delete all rows from table, this helps with rerunning scripts not to make duplicates
		--CUST_INFO
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\rogst\Desktop\Data Warehouse Project 1\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- when loading a csv, need to tell DB that the data starts at line 2, not at the field names
			FIELDTERMINATOR = ',', -- file delimiter identification, how the cells are separated in a csv file
			TABLOCK -- as file is loaded, it is locked so no edits are slowing down/breaking processing
		);
		SET @end_time = GETDATE(); -- gets exact time when we completed loading this table
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------';
		--SELECT * FROM bronze.crm_cust_info;
		--SELECT COUNT(*) FROM bronze.crm_cust_info;

		--PRD_INFO
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\rogst\Desktop\Data Warehouse Project 1\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- when loading a csv, need to tell DB that the data starts at line 2, not at the field names
			FIELDTERMINATOR = ',', -- file delimiter identification, how the cells are separated in a csv file
			TABLOCK -- as file is loaded, it is locked so no edits are slowing down/breaking processing
		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------';
		--SELECT * FROM bronze.crm_prd_info;
		--SELECT COUNT(*) FROM bronze.crm_prd_info;

		--SALES_DETAILS
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze_crm_sales_details';
		TRUNCATE TABLE bronze_crm_sales_details;
		PRINT '>> Inserting Data Into: bronze_crm_sales_details';
		BULK INSERT bronze_crm_sales_details
		FROM 'C:\Users\rogst\Desktop\Data Warehouse Project 1\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- when loading a csv, need to tell DB that the data starts at line 2, not at the field names
			FIELDTERMINATOR = ',', -- file delimiter identification, how the cells are separated in a csv file
			TABLOCK -- as file is loaded, it is locked so no edits are slowing down/breaking processing
		);
		--SELECT * FROM bronze_crm_sales_details;
		--SELECT COUNT(*) FROM bronze_crm_sales_details;
		PRINT '--------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------';
		--CUST_AZ12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\rogst\Desktop\Data Warehouse Project 1\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, -- when loading a csv, need to tell DB that the data starts at line 2, not at the field names
			FIELDTERMINATOR = ',', -- file delimiter identification, how the cells are separated in a csv file
			TABLOCK -- as file is loaded, it is locked so no edits are slowing down/breaking processing
		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------';
		--SELECT * FROM bronze.erp_cust_az12;
		--SELECT COUNT(*) FROM bronze.erp_cust_az12;

		--LOC_A101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\rogst\Desktop\Data Warehouse Project 1\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, -- when loading a csv, need to tell DB that the data starts at line 2, not at the field names
			FIELDTERMINATOR = ',', -- file delimiter identification, how the cells are separated in a csv file
			TABLOCK -- as file is loaded, it is locked so no edits are slowing down/breaking processing
		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------';
		--SELECT * FROM bronze.erp_loc_a101;
		--SELECT COUNT(*) FROM bronze.erp_loc_a101;

		--PX_CAT_G1V2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_glv2';
		TRUNCATE TABLE bronze.erp_px_cat_glv2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_glv2';
		BULK INSERT bronze.erp_px_cat_glv2
		FROM 'C:\Users\rogst\Desktop\Data Warehouse Project 1\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, -- when loading a csv, need to tell DB that the data starts at line 2, not at the field names
			FIELDTERMINATOR = ',', -- file delimiter identification, how the cells are separated in a csv file
			TABLOCK -- as file is loaded, it is locked so no edits are slowing down/breaking processing
		);
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------';
		--SELECT * FROM bronze.erp_px_cat_glv2;
		--SELECT COUNT(*) FROM bronze.erp_px_cat_glv2;
		SET @batch_end_time = GETDATE();
		PRINT '============================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '============================';	
	END TRY
		BEGIN CATCH
			PRINT '============================';
			PRINT 'ERROR OCCURED DURING LOADING BROZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '============================';
		END CATCH
END

--TEST STORED PROCEDURE:
EXEC DataWarehouse.bronze.load_bronze;
