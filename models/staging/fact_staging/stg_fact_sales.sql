{{ config(materialized='table') }}

with source_factsales as (
    select * from {{ source('fact_sales', 'FACTSALES') }} 
),

source_dimstore as (
    select * from {{ source('fact_sales', 'DIMSTORE') }} 
),

joined_hashed as (
select 
{{ dbt_utils.surrogate_key(['A.sales_key', 'A.customer_key', 'A.date_key', 'B.City']) }} as sales_pk,
{{dbt_utils.star(from=source('fact_sales', 'FACTSALES'), except=["_FIVETRAN_DELETED", "_FIVETRAN_SYNCED", "store_key"])}},
{{dbt_utils.star(from=source('fact_sales', 'DIMSTORE'), except=["_FIVETRAN_DELETED", "_FIVETRAN_SYNCED", "store_key"])}}
from source_factsales A
left join source_dimstore B on A.store_key = B.store_key
)

select * from joined_hashed