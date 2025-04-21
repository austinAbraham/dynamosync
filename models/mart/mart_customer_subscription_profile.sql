{{
  config(
    materialized = 'table',
    tags = ['mart', 'customer_profile']
  )
}}

with enriched_subscriptions as (
    select * from {{ ref('int_subscriptions_enriched') }}
),

lifecycle_data as (
    select * from {{ ref('int_subscription_lifecycles') }}
),

-- Customer-level aggregations
customer_metrics as (
    select
        es.customer_id,
        min(es.start_date) as first_subscription_date,
        max(case when lc.is_most_recent_subscription then es.start_date else null end) as most_recent_subscription_date,
        max(case when lc.is_most_recent_subscription then es.subscription_status else null end) as current_subscription_status,
        count(distinct es.subscription_id) as total_subscriptions,
        sum(case when lc.is_churned then 1 else 0 end) as total_churned_subscriptions,
        sum(case when es.is_in_trial then 1 else 0 end) as total_trial_subscriptions,
        array_agg(distinct es.brand) within group (order by es.brand) as brands,
        array_agg(distinct es.channel) within group (order by es.channel) as channels,
        -- Longest subscription duration
        max(lc.subscription_duration_days) as longest_subscription_days,
        -- Average subscription duration
        avg(lc.subscription_duration_days) as avg_subscription_duration_days,
        -- If they've resubscribed
        max(case when lc.is_resubscription then 1 else 0 end) as has_resubscribed,
        -- Average days between subscriptions (for customers with multiple subscriptions)
        case 
            when count(distinct es.subscription_id) > 1 
            then avg(case when lc.days_between_subscriptions is not null then lc.days_between_subscriptions else null end)
            else null
        end as avg_days_between_subscriptions,
        -- Is the customer currently active
        max(case 
                when es.subscription_status = 'Active' then true
                else false
            end) as is_currently_active,
        -- Loyalty classification
        case
            when count(distinct es.subscription_id) >= 3 then 'Loyal'
            when count(distinct es.subscription_id) = 2 then 'Returning'
            else 'New'
        end as customer_loyalty_segment,
        -- Churn risk (simple model - could be replaced with ML model)
        case
            when max(case when lc.is_most_recent_subscription then es.subscription_status else null end) = 'Active'
                and max(case when lc.is_most_recent_subscription then lc.subscription_duration_days else null end) > 90
                then 'Low'
            when max(case when lc.is_most_recent_subscription then es.subscription_status else null end) = 'Active'
                and max(case when lc.is_most_recent_subscription then lc.subscription_duration_days else null end) <= 90
                then 'Medium'
            when max(case when lc.is_most_recent_subscription then es.subscription_status else null end) = 'Cancelled'
                then 'High'
            else 'Medium'
        end as churn_risk
    from enriched_subscriptions es
    join lifecycle_data lc on es.subscription_id = lc.subscription_id
    group by 1
)

select * from customer_metrics