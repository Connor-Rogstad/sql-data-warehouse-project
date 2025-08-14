 --             *********SILVER LAYER Transformations*********
USE [DataWarehouse]
GO

--				**********MAIN SCRIPT**********
	--SEE FIXES BELOW
	--DUPLICATE FIX: third duplicate is the latest entry according to cst_create_date so we can use that as sole value with a rank clause
	--EXTRA SPACE FIX: add trim clause to relative variables
	--CONSISTENCY FIX: make value names the same across project
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
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
					--UPPER catches if a lowercase f or m was included in the data
			ELSE 'n/a'
		END AS cst_material_status, --normalize marital status values to readable format
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
--THIS PORTION ADDS TRANSFORMED DATA INTO CORRECT TABLE UNDER SILVER SCHEMA
	;

/*	

		--CHECK FOR DUPLICATE PRIMARY KEY VALUES:

 SELECT TOP 1000 *
  FROM [DataWarehouse].[bronze].[crm_cust_info];
	--check for nulls or duplicates in primary key:
	SELECT cst_id, COUNT(*)
	FROM bronze.crm_cust_info
	GROUP BY cst_id
	HAVING COUNT(*) > 1 OR cst_id IS NULL;
	--yields 6 different IDs with duplicates, 3 are null


	SELECT * FROM bronze.crm_cust_info
	WHERE cst_id = 29466;


				--CHECK FOR UNWANTED SPACES:

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
-- if the original value is not equal to the same value after trimming then there are spaces
-- yields 15 names with a space at start or end
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
-- yields 17 names with a space at start or end
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)
-- yields 0 instances with a space at start or end

			--*****CHECK FOR CONSISTENT DATA VALUES******

--check the consistency of values in low cardinality columns
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
-- yields NULL, F, & M. Can choose to change to female, male if needed
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info
-- yields NULL, S, M


SELECT * 
FROM silver.crm_cust_info

*/

