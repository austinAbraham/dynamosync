with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
    where fivetran_deleted = false  -- Only include non-deleted records
),

customer_subscription_histories as (
    select
        customer_id,
        subscription_id,
        start_date,
        end_date,
        cancellation_date,
        create_date,
        
        -- Order subscriptions by start date for each customer
        row_number() over (partition by customer_id order by start_date) as subscription_sequence,
        
        -- Identify if this is the first subscription
        case when row_number() over (partition by customer_id order by start_date) = 1 
             then true else false end as is_first_subscription,
             
        -- Identify if this is the most recent subscription
        case when row_number() over (partition by customer_id order by start_date desc) = 1 
             then true else false end as is_most_recent_subscription,
             
        -- Time between subscription start and cancellation (if applicable)
        datediff(day, start_date, cancellation_date) as days_to_cancellation,
        
        -- Calculate subscription duration
        datediff(day, start_date, coalesce(end_date, current_date())) as subscription_duration_days,
        
        -- Identify churned subscriptions
        case when cancellation_date is not null then true else false end as is_churned,
        
        brand,
        channel,
        offer_id,
        
        -- Additional fields
        cancellation_process_date,
        cooling_off_period_end_date,
        family,
        rate_plan_id,
        free_trial_end_date
    from subscriptions
),

-- Get previous subscription info for calculating resubscription metrics
customer_resubscriptions as (
    select
        a.customer_id,
        a.subscription_id,
        a.subscription_sequence,
        a.start_date,
        a.end_date,
        a.cancellation_date,
        
        -- Get previous subscription end date
        lag(a.end_date) over (partition by a.customer_id order by a.start_date) as prev_subscription_end_date,
        
        -- Calculate days between subscriptions
        datediff(day, 
                lag(a.end_date) over (partition by a.customer_id order by a.start_date), 
                a.start_date) as days_between_subscriptions,
                
        -- Is this a resubscription?
        case when a.subscription_sequence > 1 then true else false end as is_resubscription,
        
        a.is_first_subscription,
        a.is_most_recent_subscription,
        a.days_to_cancellation,
        a.subscription_duration_days,
        a.is_churned,
        a.brand,
        a.channel,
        a.offer_id,
        a.create_date,
        
        -- Additional fields
        a.cancellation_process_date,
        a.cooling_off_period_end_date,
        a.family,
        a.rate_plan_id,
        a.free_trial_end_date
    from customer_subscription_histories a
)

select * from customer_resubscriptions