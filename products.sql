/*
	Product Report
-- ==========================================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total quantity sold
        - total customers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last sale)
        - average order revenue (AOR)
        - average monthly revenue
===========================================================================================
*/

go
create or alter view gold.report_products as 

	with base_query_products as 

		(select 
				sales.order_date,				
				sales.order_number,
				sales.product_key,
				sales.sales_amount,
				sales.quantity,
				sales.customer_key,
				products.product_name,
				products.category,
				products.subcategory,
				products.cost

		from gold.fact_sales as sales left join gold.dim_products as products
			on sales.product_key = products.product_key
		where order_date is not null							
	)

	  , product_aggregations as 
		( 
			select 
			product_key,
			product_name,
			category,
			subcategory,
			cost,
			count(distinct order_number) total_unique_orders,
			count(distinct customer_key) total_unique_customers,
			sum(sales_amount) as total_sales,
			sum(quantity) as total_quantity,
	  	min(order_date) as first_order_date,
			max(order_date) as last_order_date,
			datediff(month,min(order_date),max(order_date)) as product_lifespan,
			round(sum(sales_amount * 1.0) / nullif(sum(quantity), 0), 1) as avg_selling_price
			from base_query_products
			group by product_key,product_name,category,subcategory,cost
		)
  
	  , product_kpis as 
		(
		select
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		last_order_date,
		datediff(month, last_order_date, getdate()) AS recency_in_months,
		case
			when total_sales > 50000 then 'High-Performer'
			when total_sales >= 10000 then 'Mid-Range'
			else 'Low-Performer'
		end as product_segment,
		product_lifespan,
		total_unique_orders,
		total_sales,
		total_quantity,
		total_unique_customers,
		avg_selling_price,
		case
			when total_unique_orders  = 0 then 0
			else round(total_sales * 1.0 / total_unique_orders, 2)			
		end as avg_order_revenue, 
		case
			when product_lifespan = 0 then total_sales
			else round(total_sales * 1.0 / product_lifespan, 2)					
		end as avg_monthly_revenue
		from product_aggregations
		)

	select * from product_kpis;
