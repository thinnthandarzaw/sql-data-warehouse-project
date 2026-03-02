SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12;
-- check unwanted spaces --
SELECT *
FROM bronze.erp_cust_az12
WHERE cid != TRIM(cid);

SELECT *
FROM bronze.erp_cust_az12
WHERE gen != TRIM(gen);
 
-- Find different cid start with NAS--
SELECT *
FROM bronze.erp_cust_az12
WHERE cid NOT LIKE 'AW%';

-- CHECK cid in cust_key --
SELECT
CASE 
	WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE 
	CASE 
		WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);
--check invalid birthday date --
SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();
-- change invalid birthday to NULL --
SELECT
CASE 
	WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12;

-- data standardization and consistency --
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

SELECT
CASE 
	WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE
	WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- check how it is work --
SELECT DISTINCT gen,
CASE
	WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- complete version --
INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT
CASE 
	WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE
	WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

-- DATA QUALITY CHECK --
--check invalid birthday date --
SELECT
cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

-- data standardization and consistency --
SELECT DISTINCT gen
FROM silver.erp_cust_az12;