with source_companies as (
    select * from {{ source('hubspot', 'CONTACTS') }} 
),

companies as (
    SELECT concat('hubspot-', lower(TRANSLATE(business_name, ' ,', '-'))) as company_id,
    business_name
    FROM source_companies 
    GROUP BY business_name
)

select * from companies