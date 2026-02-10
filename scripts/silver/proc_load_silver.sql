/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure accepts no parameters and returns no values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as
begin
    declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    begin TRY
        set @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		select
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case 
				when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
				when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'
			end as cst_marital_status, -- Normalise marital status values to readable format
			case 
				when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
				when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
				else 'n/a'
			end as cst_gndr, -- Normalise gender values to readable format
			cst_create_date
		from (
			select
				*,
				ROW_NUMBER() OVER (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		) t
		where flag_last = 1; -- Select the most recent record per customer
		set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select
			prd_id,
			replace(substring(prd_key, 1, 5), '-', '_') as cat_id, -- Extract category ID
			substring(prd_key, 7, LEN(prd_key)) as prd_key,        -- Extract product key
			prd_nm,
			isnull(prd_cost, 0) as prd_cost,
			case 
				when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
				when UPPER(TRIM(prd_line)) = 'R' then 'Road'
				when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
				when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
				else 'n/a'
			end as prd_line, -- Map product line codes to descriptive values
			cast(prd_start_dt as DATE) as prd_start_dt,
			cast(
				lead(prd_start_dt) OVER (partition by prd_key order by prd_start_dt) - 1 
				as DATE
			) as prd_end_dt -- Calculate end date as one day before the next start date
		from bronze.crm_prd_info;
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		insert into silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as VARCHAR) as DATE)
			end as sls_order_dt,
			case 
				when sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as VARCHAR) as DATE)
			end as sls_ship_dt,
			case 
				when sls_due_dt = 0 OR LEN(sls_due_dt) != 8 then null
				else cast(CAST(sls_due_dt as VARCHAR) as DATE)
			end as sls_due_dt,
			case 
				when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) 
					then sls_quantity * abs(sls_price)
				else sls_sales
			end as sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			case 
				when sls_price is null or sls_price <= 0 
					then sls_sales / nullif(sls_quantity, 0)
				else sls_price  -- Derive price if original value is invalid
			end as sls_price
		from bronze.crm_sales_details;
        set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		select
			case
				when cid like 'NAS%' then substring(cid, 4, len(cid)) -- Remove 'NAS' prefix if present
				else cid
			end as cid, 
			case
				when bdate > GETDATE() then null
				else bdate
			end as bdate, -- Set future birthdates to NULL
			case
				when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
				when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
				else 'n/a'
			end as gen -- Normalise gender values and handle unknown cases
		from bronze.erp_cust_az12;
	    set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101 (
			cid,
			cntry
		)
		select
			REPLACE(cid, '-', '') as cid, 
			case
				when TRIM(cntry) = 'DE' then 'Germany'
				when TRIM(cntry) in ('US', 'USA') then 'United States'
				when TRIM(cntry) = '' or cntry is null then 'n/a'
				else TRIM(cntry)
			end as cntry -- Normalise and Handle missing or blank country codes
		from bronze.erp_loc_a101;
	    set @end_time = GETDATE();
        PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		set @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		select
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;
		set @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		set @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	end try
	begin catch
		PRINT '=========================================='
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + cast (ERROR_NUMBER() as NVARCHAR);
		PRINT 'Error Message' + cast (ERROR_STATE() as NVARCHAR);
		PRINT '=========================================='
	end catch
end
