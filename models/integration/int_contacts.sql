with hubspot_contacts as (
select * from {{ ref('stg_hubspot_contacts') }}
),

rds_customers as (
    select * from {{ ref('stg_rds_customers') }}
),

merged_contacts as (
    select * from rds_customers
    union all
    select * from hubspot_contacts

)

select * from merged_contacts