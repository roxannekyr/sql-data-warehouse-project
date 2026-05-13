/*
High Level Business Metrics Report
==========================================================================================================
Purpose:
    - This report consolidates key high-level business KPIs into a single unified metrics view

Highlights:
    1. Queries core sales data from the gold.fact_sales table.
    2. Uses a UNION ALL pattern to stack all metrics into a single measure_name / measure_value output.
    3. Aggregates the following business metrics:
        - total sales            : Sum of all sales amounts
        - total quantity         : Total number of units sold
        - avg selling price      : Average price per transaction
        - total unique orders    : Count of distinct order numbers
        - total unique customers : Count of distinct customers
===========================================================================================================
*/

select 
		'total_sales' as measure_name,sum(sales_amount) as measure_value from gold.fact_sales
		union all
select	'total_qty' as measure_name,sum(quantity) as measure_value from gold.fact_sales
		union all
select	'avg_selling_price' as measure_name, avg(price) as measure_value from gold.fact_sales
		union all
select	'total_unique_orders' as measure_name, count(distinct order_number) as measure_value from gold.fact_sales
		union all
select	'total_unique_customers' as measure_name,count(distinct customer_key) as measure_value from gold.fact_sales;
