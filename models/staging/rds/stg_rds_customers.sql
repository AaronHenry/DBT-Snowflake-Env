with source_customers as (
    select * from {{ source('rds', 'DIMCUSTOMER') }} 
),
source_sales as (
    select * from {{ source('rds', 'FACTSALES') }} 
),
sales_count as (
    select concat('rds-', customer_key) as customer_key,
    count(customer_key) as num_of_sales
    from source_sales
    group by customer_key
),
customers as (
    SELECT concat('rds-', customer_id) as customer_id, 
    FIRST_NAME || ' ' || LAST_NAME as full_name,  
    '(' || LEFT(PHONE, 3) || ') ' || SUBSTRING(PHONE, 4, 3) || '-' || SUBSTRING(PHONE,7,4) as formatted_number, 
    city, country, address, postal_code, email
    FROM source_customers 
    where len(phone) = 10
),
customers_sales as (
select sales_count.*, customers.* from customers
    JOIN sales_count
    ON customers.customer_id = sales_count.customer_key
)

select * from customers_sales
