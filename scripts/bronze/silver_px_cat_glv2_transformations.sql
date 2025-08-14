USE [DataWarehouse]
GO


--INSPECT BRONZE LAYER
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_glv2

--Check id and cat_id for ability to link tables
SELECT * FROM silver.crm_prd_info
-- nothing to change, matching correctly

--CHECK UNWANTED SPACES
SELECT * FROM bronze.erp_px_cat_glv2
WHERE cat != TRIM(cat)
--checks where cat does not equal a trim cat to identify spaces
--yields no errors
SELECT * FROM bronze.erp_px_cat_glv2
WHERE subcat != TRIM(subcat)
--yields no errors
SELECT * FROM bronze.erp_px_cat_glv2
WHERE maintenance != TRIM(maintenance)
--yields no errors

--CHECK DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cat, subcat, maintenance
FROM bronze.erp_px_cat_glv2
--nothing to change here


-- TRANSFORMATIONS
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