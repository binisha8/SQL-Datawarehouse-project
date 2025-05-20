EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
	    SET @batch_start_time=GETDATE();
		PRINT '=========================================';
		PRINT 'lOADING BRONZE LAYER';
		PRINT '=========================================';

	
		PRINT '------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '------------------------------------------';
	
	    SET @start_time=GETDATE();

	    PRINT '**************************************************';
		PRINT 'TRUNCATING TABLE:bronze.crm_cust_info';
	    PRINT '**************************************************';

        TRUNCATE TABLE bronze.crm_cust_info;
		--TRUNCATE will help in not including the duplicate more tahn 1 time dataset)
        
		PRINT'>> INSERTING DATA INTO :bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ASUS\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	    SET @end_time=GETDATE();
		PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + 'SECONDS';

		SELECT COUNT(*) FROM bronze.crm_cust_info

     	PRINT '**************************************************';
		PRINT 'TRUNCATING TABLE:\bronze.crm_prd_info';
	    PRINT '**************************************************';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
	    
		PRINT'>> INSERTING DATA INTO : bronze.crm_prd_info';
		BULK INSERT  bronze.crm_prd_info
		FROM 'C:\Users\ASUS\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time=GETDATE();
		PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + 'SECONDS';
		SELECT COUNT(*) FROM bronze.crm_prd_info

		PRINT '**************************************************';
		PRINT 'TRUNCATING TABLE:bronze.crm_sales_details';
	    PRINT '**************************************************';

        SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
	
	    PRINT'>> INSERTING DATA INTO : bronze.crm_sales_details';

		BULK INSERT  bronze.crm_sales_details
		FROM 'C:\Users\ASUS\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + 'SECONDS';
		SELECT COUNT(*) FROM bronze.crm_sales_details

	
		PRINT '------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '------------------------------------------';


		PRINT '**************************************************';
		PRINT 'TRUNCATING TABLE:bronze.erp_cust_az12';
	    PRINT '**************************************************';
		TRUNCATE TABLE bronze.erp_cust_az12;
	    
		
	    PRINT'>> INSERTING DATA INTO :bronze.erp_cust_az12';
		BULK INSERT  bronze.erp_cust_az12
		FROM 'C:\Users\ASUS\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + 'SECONDS';
		SELECT COUNT(*) FROM bronze.erp_cust_az12

		PRINT '**************************************************';

		PRINT 'TRUNCATING TABLE:bronze.erp_loc_a101';

	    PRINT '**************************************************';
	    SET @start_time=GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

		
	    PRINT'>> INSERTING DATA INTO : bronze.erp_loc_a101';
		BULK INSERT  bronze.erp_loc_a101
		FROM 'C:\Users\ASUS\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		 SET @end_time=GETDATE();
		PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + 'SECONDS';
		SELECT COUNT(*) FROM bronze.erp_loc_a101


		
	    PRINT '**************************************************';
		PRINT 'TRUNCATING TABLE:bronze.erp_px_cat_g1v2';
	    PRINT '**************************************************';
	    SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		
	    PRINT'>> INSERTING DATA INTO : bronze.erp_px_cat_g1v2';
		BULK INSERT  bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ASUS\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		 SET @end_time=GETDATE();
		PRINT'>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + 'SECONDS';

		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2

		SET @batch_end_time =GETDATE();
		PRINT'============================================================================================================'
		PRINT'LOADING BRONZE LAYER IS COMPLETED';
		PRINT'   TOTAL LOAD DURATION :' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR) + 'seconds';
	    PRINT'============================================================================================================='

		END TRY
		BEGIN CATCH
			PRINT'================================================';
			PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
			PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT'================================================';
		END CATCH
END



 

 

 
