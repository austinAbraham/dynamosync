with source as (
    select * from {{ ref('raw_subscription_poc') }}
),

renamed as (
    select
        -- IDs
        CUSTOMER_ID as customer_id,
        SUBSCRIPTION_ID as subscription_id,
        BILLING_SUBSCRIPTION_ID as billing_subscription_id,
        OFFER_ID as offer_id,
        
        -- Dates
        cast(START_DT as date) as start_date,
        cast(END_DT as date) as end_date,
        cast(CANCELLATION_DT as date) as cancellation_date,
        cast(CREATE_DT as date) as create_date,
        cast(UPDATE_DT as date) as update_date,
        cast(FREE_TRIAL_END_DT as date) as free_trial_end_date,
        cast(CANCELLATION_PROCESS_DT as date) as cancellation_process_date,
        cast(COOLING_OFF_PERIOD_END_DT as date) as cooling_off_period_end_date,
        
        -- Categorical fields
        BRAND,
        CHANNEL,
        FAMILY,
        RATE_PLAN_ID as rate_plan_id,
        
        -- Status fields
        AMENDMENT_STATUS as amendment_status,
        FULFILLMENT_STATUS as fulfillment_status,
        TERM_ACTION_STATUS as term_action_status,
        
        -- Additional metadata
        VERSION as version,
        UPC as upc,
        HEAL_OFFER_ID as heal_offer_id,
        PLACEMENT as placement,
        TAG_NAME as tag_name,
        ID_ASSURANCE_LEVEL as id_assurance_level,
        ORIGINATOR as originator,
        
        -- Text fields
        BENEFITS as benefits,
        TERMS as terms,
        TERMS_AND_CONDITIONS as terms_and_conditions,
        DESCRIPTORS as descriptors,
        LEGAL_DOCUMENTS as legal_documents,
        ACTION_INFO as action_info,
        DEFERMENT_INFO as deferment_info,
        CORRELATION_ID as correlation_id,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as fivetran_synced,
        _FIVETRAN_DELETED as fivetran_deleted,
        
        -- Add columns for data quality tracking
        current_timestamp() as dbt_loaded_at
    from source
)

select * from renamed