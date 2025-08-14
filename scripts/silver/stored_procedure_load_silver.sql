--STORED PROCEDURE SILVER LAYER

EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						--UPPER catches if a lowercase f or m was included in the data
				ELSE 'n/a'
			END AS cst_marital_status, --normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						--UPPER catches if a lowercase f or m was included in the data
				ELSE 'n/a'
			END AS cst_gndr, --normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info) t
		WHERE flag_last = 1
		;

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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		--deriving the category id out of the product key
		--replacing "-" with "_" to link tables
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		--deriving the product key out of the prd_key (w/o cat_id), LEN covers the keys that are different in length
		--needed to link to sales details table
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		--replaces Null values with 0, if business deems this correct
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		--standardizing the values to show full description 
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		--LEAD accesses the values from the next row within a window
		--helps the end date become the value of the previous day of the next entry's start date so there is no overlap
	FROM bronze.crm_prd_info
	;

	INSERT INTO silver.crm_sales_details(
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
		CASE
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			--use same logic on this date field to have uniformity and not miss any issues in future data uploads
		CASE
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			--use same logic on this date field to have uniformity and not miss any issues in future data uploads
		CASE 
		WHEN 
			sls_sales IS NULL 
			OR sls_sales <= 0 
			OR sls_sales != sls_quantity * ABS(sls_price)
			-- ABS returns absolute value of a number
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
		WHEN
			sls_price IS NULL 
			OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
		--ensures that in future, the equation is never dividing by zero
		ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details
	;

	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	SELECT
	CASE	
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		-- extracts the rest of the cid starting at position 4 
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
		-- upper and trim allow for all future caps or unwanted spaces to be dealt with
	END AS gen
	FROM bronze.erp_cust_az12
	;

	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry)
	SELECT
	REPLACE(cid, '-', '') AS cid,
	-- removes "-" and replaces with nothing
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'United States', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101
	;

	INSERT INTO silver.erp_px_cat_glv2(
	id,
	cat,
	subcat,
	maintenance)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_glv2
	;
END

