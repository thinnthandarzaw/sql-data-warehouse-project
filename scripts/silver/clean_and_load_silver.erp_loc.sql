SELECT cid,
cntry
FROM bronze.erp_loc_a101;
SELECT cst_key
FROM silver.crm_cust_info;

-- check unwanted spaces --
SELECT cid,
cntry
FROM bronze.erp_loc_a101
WHERE cid != TRIM(cid) OR  cntry != TRIM(cntry);

SELECT cid
FROM bronze.erp_loc_a101
WHERE cid NOT LIKE 'AW%';

-- change cid format because cid and cst_key formats are different --
SELECT REPLACE(cid,'-','') AS cid,
cntry
FROM bronze.erp_loc_a101;

-- check cid in cst_key --
SELECT REPLACE(cid,'-','') AS cid
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (
	SELECT cst_key FROM silver.crm_cust_info
);

SELECT DISTINCT
cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT
cntry,
CASE
	WHEN TRIM(cntry) IN ('USA','US','United States') THEN 'United States'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

-- complete version --
INSERT INTO silver.erp_loc_a101(cid,cntry)
SELECT REPLACE(cid,'-','') AS cid,
CASE
	WHEN TRIM(cntry) IN ('USA','US','United States') THEN 'United States'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;

-- DATA QUALITY CHECK --
SELECT DISTINCT
cntry
FROM silver.erp_loc_a101;