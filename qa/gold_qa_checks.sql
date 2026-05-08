/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'gold' layer. It includes checks for:
    - Dimension table content validation (customers, products)
    - Data standardization verification (e.g., gender field normalization)
    - Fact table content and completeness validation
    - Foreign key integrity between fact and dimension tables
    - Orphaned records detection (unmatched keys across joins)
===============================================================================
*/

-- =============================================================================
-- Checking Dimension : gold.dim_customers
-- =============================================================================
	
use DataWarehouse
select * from gold.dim_customers;

-- Checking column that we adjusted with intergrating data from both sources
select distinct gender from gold.dim_customers;

-- =============================================================================
-- Checking Dimension : gold.dim_products
-- =============================================================================

-- Checking data from view for products dimension
use DataWarehouse;
select * from gold.dim_products;

-- 1. Checking data in gold.fact_sales
use DataWarehouse;
select * from gold.fact_sales;

-- 2. Check if all dimension tables can successfully join to the fact table (Foreign key Integrity - dimensions )

-- 2a. Check with dimension customers. 
-- Expectation : If all is done right no result will be visible
use DataWarehouse;
select * 
from gold.fact_sales as fact
left join gold.dim_customers as cust
	on fact.customer_key=cust.customer_key
where cust.customer_key is null;

-- 2b. Check with dimension products. 
-- Expectation : If all is done right no result will be visible
use DataWarehouse;
select * 
from gold.fact_sales as fact
left join gold.dim_products as prd
	on fact.product_key=prd.product_key
where prd.product_key is null;
