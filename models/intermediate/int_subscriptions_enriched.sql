with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

enriched as (
    select
        customer_id,
        subscription_id,
        start_date,
        end_date,
        cancellation_date,
        brand,
        channel,
        
        -- Derive subscription status
        case
            when cancellation_date is not null then 'Cancelled'
            when end_date < current_date() then 'Expired'
            when start_date > current_date() then 'Not Started'
            else 'Active'
        end as subscription_status,
        
        -- Calculate subscription age in days
        datediff(day, start_date, coalesce(cancellation_date, end_date, current_date())) as subscription_age_days,
        
        -- Calculate if subscription is in trial
        case
            when free_trial_end_date is not null and current_date() <= free_trial_end_date then true
            else false
        end as is_in_trial,
        
        -- Identify subscription type
        case
            when free_trial_end_date is not null then 'Trial Subscription'
            else 'Regular Subscription'
        end as subscription_type,
        
        -- Add more derived fields as needed
        offer_id,
        rate_plan_id,
        family,
        
        -- Source fields
        create_date,
        update_date,
        free_trial_end_date,
        amendment_status,
        fulfillment_status,
        
        -- Additional fields from source
        cancellation_process_date,
        cooling_off_period_end_date,
        heal_offer_id,
        placement,
        tag_name,
        originator,
        
        -- Flag for deleted records
        fivetran_deleted
    from subscriptions
    where fivetran_deleted = false  -- Only include non-deleted records
)

select * from enriched