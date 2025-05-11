/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;


-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
--Quality Check: Check for Nulls/ Duplicates in Primary Key
--Expectation: NO results

SELECT prd_id, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id --based on primary key
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--CHECK FOR UNWANTED SPACES IN STRING VALUES
--Expectation: NO results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); -- TRIM value removes leading & trailing spaces from a string
--If it is not equal then that means that there are spaces

--Data Standardisation & Consistency (of Marital status & gender)
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


--Check for Nulls or Negative Numbers in Cost
--Expectation: NO results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

--Check for Invalid Date Orders (Start Date > End Date)
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt --End date MUST not be earlier than start date


-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT *
FROM silver.crm_prd_info

--Check for Invalid Dates: 
-- (If dates = 0)
SELECT
NULLIF(sls_due_dt,0) AS sls_order_dt --returns NULL if two given values are equal: otherwise, returns the first expression
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
	OR LEN(sls_due_dt) != 8 --depends on the format of the date and how many numbers it's meant to have
	OR sls_due_dt > 20500101 --max boundary
	OR sls_due_dt < 19000101; --min boundary/ before business was started

--Check for invalid date orders (with shipping-, order- and due-dates)
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
	OR sls_order_dt > sls_due_dt;

--Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values MUST NOT be NULL, zero or negative
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
--Identify Out-of-range Dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
	OR bdate > GETDATE(); -- GETDATE() is the current date --can't have dates that make no sense!
--Especially dates in the future

--Data Standardisation & Consistency
SELECT DISTINCT gen,
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
