
--INSPECT THE BRONZE DATA
SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info ;

--CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY FROM BRONZE LAYER
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
-- No Nulls or Duplicates found

--CHECK FOR BLANK SPACES
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
-- no spaces found

--CHECK FOR NULLS or NEGATIVE NUMBERS
--EXPECTATION: NO RESULTS
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL
--yields no negative values but 2 Null values

--CHECK STANDARDIZATION & CONSISTENCY
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;
--yields R, S, M, T, Null, need to make letters into full answers

--CHECK FOR INVALID DATE ORDERS
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
--yields many instances of start date falling after end date
--start date for same product (with multiple entries) needs to not overlap with the end date of the previous entry
--each record must have a start date

--MAIN TRANSFORMATION SCRIPT
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

--WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
	--(SELECT sls_prd_key FROM bronze_crm_sales_details)
	--"NOT IN" clause yields many prd_keys without orders
	-- "IN" clause matches all prd_keys that have order

--WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
	--(SELECT DISTINCT id FROM bronze.erp_px_cat_glv2)
	--yields one cat_id that is not matching (CO_PE), this is ok
;

/*

SELECT DISTINCT id FROM bronze.erp_px_cat_glv2
-- check if we can link the cat_id to id on this table
-- in erp, the cat_id uses an "_" but the crm table uses a "-"

SELECT sls_prd_key
FROM bronze_crm_sales_details

SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
	--LEAD accesses the values from the next row within a window
	--helps the end date become the value of the previous day of the next entry's start date so there is no overlap

FROM bronze.crm_prd_info 
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

*/
