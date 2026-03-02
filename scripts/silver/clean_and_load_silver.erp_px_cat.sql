SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT * FROM silver.crm_prd_info;

SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info);

SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2
WHERE LEN(id) > 5;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

-- complete version --
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;