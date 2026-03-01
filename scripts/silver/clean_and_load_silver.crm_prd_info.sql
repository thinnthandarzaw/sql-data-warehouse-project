USE DataWarehouse;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT * FROM bronze.erp_loc_a101;

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt FROM bronze.crm_prd_info
    WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN (
    SELECT id FROM bronze.erp_px_cat_g1v2);

SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt FROM bronze.crm_prd_info;

    SELECT prd_nm FROM bronze.crm_prd_info
    WHERE prd_nm != TRIM(prd_nm);

    -- check for nulls or negative numbers --
    SELECT prd_cost FROM bronze.crm_prd_info
    WHERE prd_cost IS NULL OR prd_cost < 0;

    -- replace null with 0 --
    SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Mountain'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END prd_line,
    prd_start_dt,
    prd_end_dt FROM bronze.crm_prd_info;

    SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

     -- check end date is earlier than start date --
    SELECT * FROM bronze.crm_prd_info
    WHERE prd_end_dt < prd_start_dt;

    SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Mountain'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_end_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;


    INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    )
    SELECT prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Mountain'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_end_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;

    SELECT * FROM silver.crm_prd_info;

    --quality check --
    SELECT prd_id, COUNT(*)
    FROM silver.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL;

    -- check for nulls or negative numbers --
    SELECT prd_cost FROM silver.crm_prd_info
    WHERE prd_cost IS NULL OR prd_cost < 0;