-- SILVER LAYER CONSTRUCTION

-- Explore existing data for transformation needs
  SELECT TOP 1000 *
  FROM [DataWarehouse].[bronze].[crm_cust_info];

  SELECT TOP 1000 *
  FROM [DataWarehouse].[bronze].[crm_prd_info];

  SELECT TOP 1000 *
  FROM [DataWarehouse].[dbo].[bronze_crm_sales_details];

  SELECT TOP 1000 *
  FROM [DataWarehouse].[bronze].[erp_cust_az12];

  SELECT TOP 1000 *
  FROM [DataWarehouse].[bronze].[erp_loc_a101];

  SELECT TOP 1000 *
  FROM [DataWarehouse].[bronze].[erp_px_cat_glv2];

  -- CREATE SILVER SCHEMA TABLES USING SAME CODE FROM BRONZE
  -- Use Notepad for find "bronze" and replace with "silver"
  -- Add Metadata columns if needed

  IF OBJECT_ID ('silver.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR (50),
	cst_lastname NVARCHAR (50),
	cst_material_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE
);
IF OBJECT_ID ('silver.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
IF OBJECT_ID ('silver_crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE silver_crm_sales_details;
CREATE TABLE silver_crm_sales_details (
	sls_ord_num NVARCHAR (50),
	sls_prd_key NVARCHAR (50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
IF OBJECT_ID ('silver.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR (50),
	cntry NVARCHAR (50)
);
IF OBJECT_ID ('silver.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR (50),
	bdate DATE,
	gen NVARCHAR (50)
);
IF OBJECT_ID ('silver.erp_px_cat_glv2' , 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_glv2;
CREATE TABLE silver.erp_px_cat_glv2 (
	id NVARCHAR (50),
	cat NVARCHAR (50),
	subcat NVARCHAR (50),
	maintenance NVARCHAR (50)
);










  -- Cleaning, Standardizing, and Normalizing Data

