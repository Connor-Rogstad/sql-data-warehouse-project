USE [DataWarehouse]
GO

-- CHECK BRONZE DATA
SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12
--WHERE cid LIKE '%AW00011000%'

--Check cid and cst_key for ability to link tables
SELECT * FROM silver.crm_cust_info
-- some cids have 3 characters in front that do not allow for a match

-- CHECK FOR DATE RANGE MISTAKE
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
-- yields 15 instances of customers falling out of the date range, i.e. older than 100 years old
-- yields 16 instances of future dates 

--CHECK DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT gen
FROM bronze.erp_cust_az12
-- yields many conflicting values


--TRANSFORMATION
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