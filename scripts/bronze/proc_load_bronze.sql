/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
*/

go
create OR alter procedure bronze.load_bronze as 
begin
    declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;

    begin try
        set @batch_start_time = getdate();
        print('===========================================================================================================');
        print('Loading Bronze Layer');
        print('===========================================================================================================');


        print('-----------------------------------------------------------------------------------------------------------');
        print('Loading CRM Tables');
        print('-----------------------------------------------------------------------------------------------------------');
    
        -- Source CRM , file cust_info
        -- Deleting any data that exists in table from file cust_info of source crm to bronze layer, to make the table empty 
        
        set @start_time = getdate();
        print(' >> Truncating table : bronze.crm_cust_info');
        truncate table bronze.crm_cust_info;

        -- Data Ingestion begins from file cust_info of source crm to bronze layer
        -- When executing the bulk insert we need to not have open relevant file from where we will take the data for ingestion

        print(' >> Inserting Data Into : bronze.crm_cust_info');
        bulk insert bronze.crm_cust_info
        -- Placing the path of the file from where we will load the data from & after also the file name (form like: sourcefilepath_filename)
        from 'C:\Users\roxan\Desktop\Personal\Learning & Development\8. SQL\Data with Baraa\Roxani querying\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

        with(
        -- Since in the file the 1st row is headers and we have already defined that in our table, SQL should take data from 1st data row (non headers)
            firstrow=2,
        -- Delimeter
            fieldterminator=',',
        -- Using tablock for better table perfomance
            tablock
        );
           
        -- Testing the quality of the 'bronze.crm_cust_info' table that was just ingested with data
        -- select * from bronze.crm_cust_info;
        -- Checks
        -- 1. if data are placed in all columns
        -- 2. if data have been placed in the correct columns
        -- select count(*) from bronze.crm_cust_info;
        -- Checks
        -- 3. counting rows for the table & check from our file how many rows we had there. Here we need to remember if in file we had also 
        -- a header for columns that will not appear of course here
        -- ALERT !!!
        -- if we run again the bulk insert we will refill the table with 1 more time same data, meaning having duplicate for each row record

        set @end_time = getdate();
        print(' >> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'); 
        print(' >> ----------------');

        -- Source CRM , file prd_info

        set @start_time = getdate();
        print(' >> Truncating table : bronze.crm_prd_info');
        truncate table bronze.crm_prd_info;

        print(' >> Inserting Data Into : bronze.crm_prd_info');
        bulk insert bronze.crm_prd_info
        from 'C:\Users\roxan\Desktop\Personal\Learning & Development\8. SQL\Data with Baraa\Roxani querying\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'

        with(
        -- Since in the file the 1st row is headers and we have already defined that in our table, SQL should take data from 1st data row (non headers)
            firstrow=2,
        -- Delimeter
            fieldterminator=',',
        -- Using tablock for better table perfomance
            tablock
        );

        -- Testing the quality of the 'bronze.crm_prd_info' table that was just ingested with data
        -- select * from bronze.crm_prd_info;
        -- Checks
        -- 1. if data are placed in all columns
        -- 2. if data have been placed in the correct columns
        -- select count(*) from bronze.crm_prd_info;
        -- 3. counting rows for the table & check from our file how many rows we had there. Here we need to remember if in file we had also 
        -- a header for columns that will not appear of course here

        set @end_time = getdate();
        print(' >> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'); 
        print(' >> ----------------');

        -- Source CRM , file sales_details

        set @start_time = getdate();
        print(' >> Truncating table : bronze.crm_sales_details');
        truncate table bronze.crm_sales_details;

        print(' >> Inserting Data Into : bronze.crm_sales_details');
        bulk insert bronze.crm_sales_details
        from 'C:\Users\roxan\Desktop\Personal\Learning & Development\8. SQL\Data with Baraa\Roxani querying\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'

        with(
        -- Since in the file the 1st row is headers and we have already defined that in our table, SQL should take data from 1st data row (non headers)
            firstrow=2,
        -- Delimeter
            fieldterminator=',',
        -- Using tablock for better table perfomance
            tablock
        );

        -- Testing the quality of the 'bronze.crm_prd_info' table that was just ingested with data
        -- select * from bronze.crm_sales_details;
        -- Checks
        -- 1. if data are placed in all columns
        -- 2. if data have been placed in the correct columns
        -- select count(*) from bronze.crm_sales_details;
        -- 3. counting rows for the table & check from our file how many rows we had there. Here we need to remember if in file we had also 
        -- a header for columns that will not appear of course here

        set @end_time = getdate();
        print(' >> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'); 
        print(' >> ----------------');

        print('-----------------------------------------------------------------------------------------------------------');
        print('Loading ERP Tables');
        print('-----------------------------------------------------------------------------------------------------------');

        -- Source ERP , file CUST_AZ12

        set @start_time = getdate();
        print(' >> Truncating table : bronze.erp_CUST_AZ12');
        truncate table bronze.erp_CUST_AZ12;

        print(' >> Inserting Data Into : bronze.erp_CUST_AZ12');
        bulk insert bronze.erp_CUST_AZ12
        from 'C:\Users\roxan\Desktop\Personal\Learning & Development\8. SQL\Data with Baraa\Roxani querying\Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'

        with(
        -- Since in the file the 1st row is headers and we have already defined that in our table, SQL should take data from 1st data row (non headers)
            firstrow=2,
        -- Delimeter
            fieldterminator=',',
        -- Using tablock for better table perfomance
            tablock
        );

        -- Testing the quality of the 'bronze.erp_CUST_AZ12' table that was just ingested with data
        -- select * from bronze.erp_CUST_AZ12;
        -- Checks
        -- 1. if data are placed in all columns
        -- 2. if data have been placed in the correct columns
        -- select count(*) from bronze.erp_CUST_AZ12;
        -- 3. counting rows for the table & check from our file how many rows we had there. Here we need to remember if in file we had also 
        -- a header for columns that will not appear of course here

        set @end_time = getdate();
        print(' >> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'); 
        print(' >> ----------------');


        -- Source ERP , file LOC_A101

        set @start_time = getdate();
        print(' >> Truncating table : bronze.erp_LOC_A101');
        truncate table bronze.erp_LOC_A101;

        print(' >> Inserting Data Into : bronze.erp_LOC_A101');
        bulk insert bronze.erp_LOC_A101
        from 'C:\Users\roxan\Desktop\Personal\Learning & Development\8. SQL\Data with Baraa\Roxani querying\Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'

        with(
        -- Since in the file the 1st row is headers and we have already defined that in our table, SQL should take data from 1st data row (non headers)
            firstrow=2,
        -- Delimeter
            fieldterminator=',',
        -- Using tablock for better table perfomance
            tablock
        );

        -- Testing the quality of the 'bronze.erp_LOC_A101' table that was just ingested with data
        -- select * from bronze.erp_LOC_A101;
        -- Checks
        -- 1. if data are placed in all columns
        -- 2. if data have been placed in the correct columns
        -- select count(*) from bronze.erp_LOC_A101;
        -- 3. counting rows for the table & check from our file how many rows we had there. Here we need to remember if in file we had also 
        -- a header for columns that will not appear of course here

        set @end_time = getdate();
        print(' >> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'); 
        print(' >> ----------------');


        -- Source ERP , file PX_CAT_G1V2

        set @start_time = getdate();
        print(' >> Truncating table : erp_PX_CAT_G1V2');
        truncate table bronze.erp_PX_CAT_G1V2;

        print(' >> Inserting Data Into : erp_PX_CAT_G1V2');
        bulk insert bronze.erp_PX_CAT_G1V2
        from 'C:\Users\roxan\Desktop\Personal\Learning & Development\8. SQL\Data with Baraa\Roxani querying\Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'

        with(
        -- Since in the file the 1st row is headers and we have already defined that in our table, SQL should take data from 1st data row (non headers)
            firstrow=2,
        -- Delimeter 
            fieldterminator=',',
        -- Using tablock for better table perfomance
            tablock
        );

        -- Testing the quality of the 'bronze.erp_PX_CAT_G1V2' table that was just ingested with data
        -- select * from bronze.erp_PX_CAT_G1V2;
        -- Checks
        -- 1. if data are placed in all columns
        -- 2. if data have been placed in the correct columns
        -- select count(*) from bronze.erp_PX_CAT_G1V2;
        -- 3. counting rows for the table & check from our file how many rows we had there. Here we need to remember if in file we had also 
        -- a header for columns that will not appear of course here

        set @end_time = getdate();
        print(' >> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'); 
        print(' >> ----------------');

        set @batch_end_time = getdate();
        print '========================================='
        print 'Loading Bronze Layer is Completed';
        print '  - Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
        print '========================================='

    end try
    begin catch
        print('==============================================')
        print('Error occured during Loading the Bronze Layer')
        print('Error Message' + error_message());
        print('Error Message' + cast(error_number() as nvarchar));
        print('Error Message' + cast(error_state() as nvarchar));
        print('==============================================')
    end catch
end;
