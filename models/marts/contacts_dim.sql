{{ config(materialized='table') }}

with integration_contacts as (
    select * from {{ ref('int_contacts') }}
)

select * from integration_contacts