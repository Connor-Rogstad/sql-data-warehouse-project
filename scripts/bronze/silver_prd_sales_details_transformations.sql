USE [DataWarehouse]
GO

--INSPECT THE BRONZE DATA
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
--WHERE sls_ord_num != TRIM(sls_ord_num)
-- do we have any unwanted spaces?^
--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
-- see if any key ids do not exist within the two tables 
--WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
-- see if any key ids do not exist within the two tables

--no issues found, meaning we can successfully connect the tables
;

--CHECK FOR INVALID DATES
	--check all date fields (sls_ship_dt, sls_order_dt, sls_due_dt)
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
-- yields many 0 dates that need to be replaced
;
--CHECK FOR INVALID DATE LENGTHS (8 characters)
SELECT NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
-- yields 2 dates that are not 8 characters long
;
--CHECK FOR OUTLIERS (not in data range boundaries)
SELECT NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101
-- yields no outliers
-- no issues with sls_ship_dt
-- no issues with sls_due_dt
;

--CHECK IF ORDER DATE IS ALWAYS SMALLER THAN SHIP DATE
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
--yields no issues
;


--CHECK DATA CONSISTENCY:
--BUSINESS RULES:
-- SALES = Quantity * Price
-- Negative, zeros, and nulls are not allowed
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
--yields many instances of incorrect business rules
-- in real life, consulting someone in the business about specific item price/quantity issues to solve problem
--issues could be fixed at the source (i.e. human error, data entry error, etc)
--IF Sales is negative, zero, or null, derive it using quantity and price
--IF Price is zero or null, calculate using sales and quantity
--IF Price is negative, convert to positive
/*
SELECT DISTINCT
sls_sales AS old_sls_sales,
--for record of errors
sls_quantity,
sls_price AS old_price,
--for record of errors
CASE 
	WHEN 
		sls_sales IS NULL 
		OR sls_sales <= 0 
		OR sls_sales != sls_quantity * ABS(sls_price)
		-- ABS returns absolute value of a number
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
	END AS sls_sales,
CASE
	WHEN
		sls_price IS NULL 
		OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity,0)
	--ensures that in future, the equation is never dividing by zero
	ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
*/

--**********************************************************************************************************************
--TRANSFORM crm_sales_details from bronze to silver
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

/*
--CHECK NEW SILVER Data
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_order_dt
OR sls_order_dt > sls_due_dt

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL 
OR sls_quantity IS NULL 
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details