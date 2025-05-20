/*
==================================================
Create Database and Schemas
==================================================
Scripts Purpose:
 This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
within the database: 'bronze', 'silver', and 'gold'.

WARNING:
Running this script will drop the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution
and ensure you have proper backups before running this script.
*/
--CREATE Database 'DataWarehouse'

CREATE DATABASE Datawarehouse;
USE Datawarehouse;

CREATE SCHEMA bronze; 
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
IF OBJECT_ID('bronze.crm_cust_info','U')IS NOT NULL
	DROP TABLE bronze.crm_cust_info;--(means that is same table present earlier then drop it and create new one)


--Building Bronze Layer
CREATE TABLE bronze.crm_cust_info(
cst_id                  INT,
cst_key                 NVARCHAR(50),
cst_firstname           NVARCHAR(50),
cst_lastname            NVARCHAR(50),
cst_material_status     NVARCHAR(50),
cst_gndr                NVARCHAR(50),
cst_create_date         DATE
);

--CREATE SQL DDL scripts for ALL CSV files in the CRM and FRP systems
IF OBJECT_ID('bronze.crm_prd_info','U')IS NOT NULL
	DROP TABLE bronze.crm_prd_info;--(means that is same table present earlier then drop it and create new one)

CREATE TABLE bronze.crm_prd_info(
	prd_id            INT,
	prd_key           NVARCHAR(50),
	prd_nm            NVARCHAR(50),
	prd_cost          NVARCHAR(50),
	prd_line          NVARCHAR(50),
	prd_start_dt      NVARCHAR(50),
	prd_end_dt        DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details','U')IS NOT NULL
	DROP TABLE bronze.crm_sales_details;--(means that is same table present earlier then drop it and create new one)
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num         NVARCHAR(50),
	sls_prd_key         NVARCHAR(50),
	sls_cust_id         INT,
	sls_order_dt        INT,
	sls_ship_dt         INT,
	sls_due_dt          INT,
	sls_sales           INT,
	sls_quqntity        INT,
	sls_price           INT
);

IF OBJECT_ID('bronze.erp_loc_a101','U')IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;--(means that is same table present earlier then drop it and create new one)

CREATE TABLE bronze.erp_loc_a101(		
	cid     NVARCHAR(50),
	cntry   NVARCHAR(50),
);

IF OBJECT_ID('bronze.erp_cust_az12','U')IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;--(means that is same table present earlier then drop it and create new one)

CREATE TABLE bronze.erp_cust_az12(		
		cid     NVARCHAR(50),
		bdate   DATE,
		gen     NVARCHAR(50),
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U')IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;--(means that is same table present earlier then drop it and create new one)

CREATE TABLE bronze.erp_px_cat_g1v2(		
	id           NVARCHAR(50),
	cat          NVARCHAR(50),
	subcat       NVARCHAR(50),
	maintenance  NVARCHAR(50),
);










 




