with source as (
    select * from {{ source('dynamodb', 'SUBSCRIPTION_POC') }}
),

renamed as (
    select
        -- IDs
        "customerId" as customer_id,
        "subscriptionId" as subscription_id,
        "billingSubscriptionId" as billing_subscription_id,
        "offerId" as offer_id,
        
        -- Dates
        cast("startDt" as date) as start_date,
        cast("endDt" as date) as end_date,
        cast("cancellationDt" as date) as cancellation_date,
        cast("createDt" as date) as create_date,
        cast("updateDt" as date) as update_date,
        cast("freeTrialEndDt" as date) as free_trial_end_date,
        
        -- Categorical fields
        "brand",
        "channel",
        "family",
        "ratePlanId" as rate_plan_id,
        
        -- Status fields
        "amendmentStatus" as amendment_status,
        "fulfillmentStatus" as fulfillment_status,
        "termActionStatus" as term_action_status,
        
        -- Additional metadata
        "version",
        "upc",
        
        -- Parse benefits as JSON if applicable
        "benefits",
        
        -- Add columns for data quality tracking
        current_timestamp() as dbt_loaded_at
    from source
)

select * from renamed