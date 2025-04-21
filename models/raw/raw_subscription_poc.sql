{{ config(
    materialized = 'view',
    schema = 'raw'
) }}

-- Simple pass-through model to ensure the source is visible in lineage
select * from {{ source('dynamodb', 'SUBSCRIPTION_POC') }}