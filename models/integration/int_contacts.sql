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

),

pk_contacts as (
select {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone_number']) }} as contact_pk, * from merged_contacts
)

{% set sources = ["stg_hubspot_contacts", "stg_rds_customers"] %}
{% for source in sources %}

   select {{ 'contact_id' }}, upper({{ 'first_name' }}) as first_name, upper({{ 'last_name' }}) as last_name, {{ 'phone_number' }}
   from {{ ref(source) }}

   {% if not loop.last %}union all{% endif %}
{% endfor %}

