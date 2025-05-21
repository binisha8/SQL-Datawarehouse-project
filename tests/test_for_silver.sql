--Rerunning the  query of SQL(SILVER_DATA_CLEANING)4.sql
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

SELECT
*
FROM(
SELECT 
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as selecting_last
FROM silver.crm_cust_info)t
WHERE selecting_last=1 


--QUATITY CHECK:Check for unwanted spaces in string values
--expected output id :no results
SELECT
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname !=TRIM(cst_firstname)
/*  
    --The TRIM() function in SQL is used to remove unwanted characters (usually spaces) from the beginning (leading) and/orend (trailing) 
       of a string.
	--in the above contex if the original value is not equall to the same value after trimming, it means there are spaces
*/

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info


SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info
 --We can see that all output are empty  in the silver layer


 
IF OBJECT_ID('silver.crm_prd_info','U')IS NOT NULL
	DROP  TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id            INT,
	cat_id			  NVARCHAR(50),
	prd_key           NVARCHAR(50),
	prd_nm            NVARCHAR(50),
	prd_cost          NVARCHAR(50),
	prd_line          NVARCHAR(50),
	prd_start_dt      DATE,
	prd_end_dt        DATE,
	dwh_create_date   DATETIME2 DEFAULT GETDATE()
);
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
	  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
	  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
	  prd_nm,
	  ISNULL(prd_cost, 0) AS prd_cost,
	  CASE 
		  WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		  WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		  WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		  WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		  ELSE 'n/a'
	   END AS prd_line, -- Map product line codes to descriptive values
	   CAST(prd_start_dt AS DATE) AS prd_start_dt,
	   CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt -- Calculate end date as one day before the next start date
	   FROM bronze.crm_prd_info;

SELECT *
FROM silver.crm_prd_info


--Data Standarization & Consistency

--to remove the original value
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END cid,
bdate,
gen
FROM silver.erp_cust_az12

--Identify out of range
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate>GETDATE()


SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
	END AS gen
FROM silver.erp_cust_az12

SELECT *FROM silver.erp_cust_az12


SELECT 
cid,cntry
FROM silver.erp_loc_a101;


SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

SELECT * FROM silver.erp_px_cat_g1v2

