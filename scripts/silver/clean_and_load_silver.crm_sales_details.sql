USE DataWarehouse;
SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT * FROM bronze.erp_loc_a101;


    SELECT * FROM bronze.crm_sales_details
    WHERE sls_ord_num != TRIM(sls_ord_num) OR sls_ord_num IS NULL;

    SELECT * FROM bronze.crm_sales_details
    WHERE sls_prd_key != TRIM(sls_prd_key) OR sls_ord_num IS NULL;
    --check prd_key in crm_prd_info--
    SELECT * FROM bronze.crm_sales_details
    WHERE sls_prd_key NOT IN (
    SELECT prd_key FROM silver.crm_prd_info
    );
    --check sls_cust_id in cst_id--
    SELECT * FROM bronze.crm_sales_details
    WHERE sls_cust_id NOT IN (
    SELECT cst_id FROM silver.crm_cust_info
    );
    --check sls_order_dt is negative number or zeros --
    SELECT NULLIF(sls_order_dt, 0) sls_order_dt FROM bronze.crm_sales_details
    WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

    SELECT NULLIF(sls_ship_dt, 0) sls_ship_dt FROM bronze.crm_sales_details
    WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8;

    SELECT NULLIF(sls_due_dt, 0) sls_due_dt FROM bronze.crm_sales_details
    WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8;

    -- check invalid date order_dt> due_dt or order_dt > ship_dt
    SELECT *  FROM bronze.crm_sales_details
    WHERE sls_order_dt> sls_due_dt OR sls_order_dt > sls_ship_dt;

    
    SELECT * FROM bronze.crm_sales_details
    WHERE sls_quantity < 0;

    SELECT * FROM bronze.crm_sales_details
    WHERE sls_price < 0;

    -- INTEGER TO DATE --
    SELECT sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END sls_order_dt,
    CASE 
        WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END sls_ship_dt,
    CASE 
        WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END sls_due_dt,
    CASE 
    --sales data is incorrect --
        WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END sls_sales,
    sls_quantity,
    CASE 
    --price data is incorrect --
        WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END sls_price
    FROM bronze.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price) 
    SELECT sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END sls_order_dt,
    CASE 
        WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END sls_ship_dt,
    CASE 
        WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END sls_due_dt,
    CASE 
    --sales data is incorrect --
        WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END sls_sales,
    sls_quantity,
    CASE 
    --price data is incorrect --
        WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END sls_price
    FROM bronze.crm_sales_details;

    -- SILVER QUALITY CHECK --
    -- check invalid date order_dt> due_dt or order_dt > ship_dt
    SELECT *  FROM silver.crm_sales_details
    WHERE sls_order_dt> sls_due_dt OR sls_order_dt > sls_ship_dt;

SELECT * FROM silver.crm_sales_details;