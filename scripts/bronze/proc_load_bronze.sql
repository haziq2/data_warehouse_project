/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as

begin
	declare @start_time datetime, @end_time datetime, @start_timebatch datetime, @end_timebatch datetime;
	begin try
		
		set @start_timebatch = getdate ();

		print '=========================================';
		print 'Loading Bronze Layer';
		print '=========================================';

		print '-----------------------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------------------';

		set @start_time = getdate ();
		print '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\faris2\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate ();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' second(s)';
		print '-------------------------------------------';

		set @start_time = getdate ();
		print '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\faris2\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate ();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' second(s)';
		print '-------------------------------------------';

		set @start_time = getdate ();
		print 'Truncating Data: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print 'Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\faris2\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate ();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' second(s)';
		print '-------------------------------------------';

		print '-----------------------------------------';
		print 'Loading ERP Tables';
		print '-----------------------------------------';

		set @start_time = getdate ();
		print '>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print '>> Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\faris2\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate ();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' second(s)';
		print '-------------------------------------------';

		set @start_time = getdate ();
		print '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print '>> Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\faris2\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate ();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' second(s)';
		print '-------------------------------------------';

		set @start_time = getdate ();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		print 'Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\faris2\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate ();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' second(s)';
		print '-------------------------------------------';

		print '===========================================';
		set @end_timebatch = getdate ();
		print '>> Batch Load Duration: ' + cast (datediff(second, @start_timebatch, @end_timebatch) as nvarchar) + ' second(s)';
		print '===========================================';

	end try

	begin catch
		print '============================================'
		print 'Error occurred during loading Bronze Layer'
		print 'Error Message' + error_message ();
		print 'Error Message' + cast (error_number () as nvarchar);
		print 'Error Message' + cast (error_state () as nvarchar);
		print '============================================'
	end catch
end
