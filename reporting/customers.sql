/*
Customer Report
==========================================================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
    2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
        - total orders
        - total sales
        - total quantity purchased
        - total products
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last order)
        - average order value   : Calculation = (total sales=total customer spend)/(total number of orders)
        - average monthly spend : Calculation = (total sales=total customer spend)/(total number of months)
===========================================================================================================
*/
go
create or alter view gold.report_customers as 

	with base_query as(
	select 
		   sales.order_date,				
		   sales.order_number,
		   sales.product_key,
		   sales.sales_amount,
		   sales.quantity,
		   customers.customer_key,
		   customers.customer_number,
	--	   customers.first_name,			
	--	   customers.last_name,				
		   concat(customers.first_name, ' ',customers.last_name) as customer_name,
	--	   customers.birthdate,
		   datediff(year,birthdate,getdate()) as age

	from gold.fact_sales as sales left join gold.dim_customers as customers
	on sales.customer_key=customers.customer_key
	where order_date is not null	
	)

	  , customer_aggregation as ( 
	select 
				customer_key,
				customer_name,
				customer_number,
				age,
				count(distinct order_number) total_unique_orders,
				sum(sales_amount) as total_spending,
				sum(quantity) as total_quantity,
				count(distinct product_key) as total_products_ordered,
	  			min(order_date) as first_order,
				max(order_date) as last_order,
				datediff(month,min(order_date),max(order_date)) as lifespan
		from base_query
		group by customer_key,customer_name,customer_number,age
	)

	  , customer_segments as ( 
	select 
				customer_key,
				customer_name,
				customer_number,
				age,
				case when age < 20 then 'Under 20'
					 when age between 20 and 29  then '20-29'
					 when age between 30 and 39  then '30-39'
					 when age between 40 and 49  then '40-49'
					 else '50 and above'
				end as age_group,
				total_unique_orders,
				total_spending,
				total_quantity,
				total_products_ordered,
	  			first_order,
				last_order,
				datediff(month,last_order,getdate()) as recency,
				lifespan,
				case when lifespan >= 12 and total_spending > 5000 then 'VIP'
					 when lifespan >= 12 and total_spending <= 5000 then 'Regular'
					 else 'New'
				end as customer_segment,
				total_spending/nullif(total_unique_orders,0) as average_order_value,   
				case when lifespan = 0 then total_spending                            
					 else total_spending/ lifespan
				end as average_monthly_spend
				from customer_aggregation
	)

	select 
	*
	from customer_segments;
