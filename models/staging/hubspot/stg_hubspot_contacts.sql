with source_contacts as (
    select * from {{ source('hubspot', 'CONTACTS') }} 
),

companies as (

select * from {{ ref('stg_hubspot_companies') }}

),

contacts as (
    SELECT concat('hubspot-', hubspot_id) as contact_id,
    FIRST_NAME,
    LAST_NAME,
    '(' || LEFT(TRANSLATE(phone, '(,),-, ,.', ''), 3) || ') ' || SUBSTRING(TRANSLATE(phone, '(,),-, ,.', ''), 4, 3) || '-' || SUBSTRING(TRANSLATE(phone, '(,),-, ,.', ''),7,4) as phone_number,
    business_name 
    FROM source_contacts

),

contacts_companies as (
    select contacts.contact_id,
    FIRST_NAME,
    LAST_NAME,
    phone_number,
    companies.company_id
    from contacts
    join companies on contacts.business_name = companies.business_name
)

select * from contacts_companies
