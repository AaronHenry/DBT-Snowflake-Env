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
    SELECT concat('rds-', customer_id) as contact_id, 
    FIRST_NAME,
    LAST_NAME,  
    '(' || LEFT(PHONE, 3) || ') ' || SUBSTRING(PHONE, 4, 3) || '-' || SUBSTRING(PHONE,7,4) as phone_number,
    case when contact_id != NULL then NULL end as company_id
    FROM source_customers 
    where len(phone) = 10
)

select * from customers
