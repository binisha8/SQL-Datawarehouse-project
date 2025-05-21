--CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
--EXPECTATION :NO RESULT

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

SELECT 
* FROM bronze.crm_cust_info
WHERE cst_id=29466

SELECT
*
FROM(
SELECT 
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as selecting_last
FROM bronze.crm_cust_info)t
WHERE selecting_last=1 


--QUATITY CHECK:Check for unwanted spaces in string values
--expected output id :no results
SELECT
cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname !=TRIM(cst_firstname)
/*  
    --The TRIM() function in SQL is used to remove unwanted characters (usually spaces) from the beginning (leading) and/orend (trailing) 
       of a string.
	--in the above contex if the original value is not equall to the same value after trimming, it means there are spaces
*/

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info


SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info
 




--Check for invalid date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt<prd_start_dt

--Since the in come part of the dataset the end date  is not appropriate as compare to the satrt date so we 
--are finding the solution
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt)OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')
--rule is that
--Solution 1:SWITCH END DATE AND START DATE(IN EXCEL)
   --THE ISSUE IS THET THE DATES ARE OVERLAPPING
   --THE ANOTHER START DATE OF SAME PRODUCT KEY SHOULD BE YOUNGER THAN PREVIOUS END DATE
   -- EACH PRODUCT MUST HAS A START DATE
--SOLUTION 2: DERIVE THE END DATE FROMM THE START DATE
            --END DATE: START DATE OF THE NEXT RECORD - 1
			--eg:startdate:2012-07-01 now end date will be 2012-06-30



SELECT
sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quqntity
      ,sls_price
FROM bronze.crm_sales_details


--CHECK for invalid Dates
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0 
OR LEN(sls_order_dt) <>8
OR sls_due_dt>20500101
OR sls_due_dt<19000101


--CHECK for invalid Date Orders
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt



SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
	CASE WHEN sls_order_dt= 0 OR LEN(sls_order_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	 END AS sls_order_dt,
	 CASE WHEN sls_order_dt= 0 OR LEN(sls_ship_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	 END AS sls_ship_dt,
	 CASE WHEN sls_order_dt= 0 OR LEN(sls_due_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	 END AS sls_due_dt,

sls_sales,
sls_quqntity,
sls_price
FROM bronze.crm_sales_details
--order date must always be earlier than the shipping date or due date


--CHECK DATA CONSISTENCY : Between Sales,Quantityy, and Price
--Sales=Quantity*Price
--Negative,zeros,Nullls are not allowed!

SELECT DISTINCT
sls_sales,
sls_quqntity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales!=sls_quqntity*sls_price
OR sls_sales IS NULL OR sls_quqntity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quqntity<=0 OR sls_price<=0
ORDER BY sls_sales,sls_quqntity,sls_price


--Solution for the bad datas is that
  --Solution1:Data issues will be fixed direct in source system
  --Solution2:Data issues has to be fixed in data warehouse
--RULES
 --IF sales is negative ,zero,or null, derive it using Quantity and Price.
 --IF Price is zero or null, calculate it usingg Sales and Quantity
 --IF Price is negative, convert it to a positive value

 SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quqntity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales<=0  OR sls_sales!=sls_quqntity * ABS(sls_price)
     THEN sls_quqntity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price is NULL OR sls_price <=0
     THEN sls_sales/NULLIF(sls_quqntity,0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales!=sls_quqntity*sls_price
OR sls_sales IS NULL OR sls_quqntity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quqntity<=0 OR sls_price<=0
ORDER BY sls_sales,sls_quqntity,sls_price


SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
END  NOT IN(SELECT DISTINCT cst_key FROM silver.crm_cust_info)


--to remove the original value
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12

--Identify out of range
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate>GETDATE()


SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12

SELECT 
cid,cntry
FROM bronze.erp_loc_a101;

SELECT 
REPLACE (cid,'-','')cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN 
(SELECT cst_key FROM silver.crm_cust_info)

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry


SELECT DISTINCT
cntry AS old_cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry


SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


--Checking unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat!=TRIM(cat) OR subcat!=TRIM(subcat) OR maintenance!=TRIM(maintenance)

--DAta standarization & consistency
SELECT DISTINCT
cat FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance FROM bronze.erp_px_cat_g1v2

