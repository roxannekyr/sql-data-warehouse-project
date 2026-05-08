/*
=============================================================================
DDL Script: Refining Tables in Silver Layer 
(Applied after cleaning Bronze Layer data)
=============================================================================
Script Purpose:
    This script recreates tables in the 'silver' schema to enforce 
    updated column names and proper data types, dropping existing 
    tables if they already exist.
=============================================================================
*/

if object_id('silver.crm_prd_info', 'U') is not null
    drop table silver.crm_prd_info;
create table silver.crm_prd_info(
    prd_id       int,
    category_id  nvarchar(50),
    product_key  nvarchar(50),
    prd_nm       nvarchar(50),
    prd_cost     int,
    prd_line     nvarchar(50),
    prd_start_dt date,
    prd_end_dt   date,
    dwh_create_date datetime2 default getdate() 
);

if object_id('silver.crm_sales_details', 'U') is not null
    drop table silver.crm_sales_details;
create table silver.crm_sales_details(
    sls_ord_num  nvarchar(50),
    sls_prd_key  nvarchar(50),
    sls_cust_id  int,
    sls_order_dt date,
    sls_ship_dt  date,
    sls_due_dt   date,
    sls_sales    int,
    sls_quantity int,
    sls_price    int,
    dwh_create_date datetime2 default getdate() 
);
