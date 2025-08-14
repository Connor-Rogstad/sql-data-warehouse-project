USE [DataWarehouse]
GO


--INSPECT BRONZE LAYER
SELECT
cid,
cntry
FROM bronze.erp_loc_a101

--Check cid and cst_key for ability to link tables
SELECT * FROM silver.crm_cust_info
-- there is a "-" within cid which does not match the cst_key
SELECT
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '')
NOT IN (SELECT cst_key FROM silver.crm_cust_info)
-- checking to see if transformation worked

--CHECK DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry
-- yields 3 different options for US, and blanks

-- TRANSFORMATIONS
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