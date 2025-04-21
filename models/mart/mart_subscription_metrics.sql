{{
  config(
    materialized = 'table',
    tags = ['mart', 'subscription_metrics']
  )
}}

with enriched_subscriptions as (
    select * from {{ ref('int_subscriptions_enriched') }}
),

lifecycle_data as (
    select * from {{ ref('int_subscription_lifecycles') }}
),

-- Get all subscription start dates to use as snapshots for metrics
subscription_dates as (
    select distinct
        date_trunc('day', start_date) as metric_date
    from enriched_subscriptions
    
    union
    
    select distinct
        date_trunc('day', cancellation_date) as metric_date
    from enriched_subscriptions
    where cancellation_date is not null
    
    union
    
    select distinct
        date_trunc('day', end_date) as metric_date
    from enriched_subscriptions
    where end_date is not null
),

-- Get all brand/channel combinations
dimensions as (
    select distinct
        brand,
        channel
    from enriched_subscriptions
),

-- Create a base of dates and dimensions for complete metrics coverage
date_dimension_cross as (
    select
        sd.metric_date,
        d.brand,
        d.channel
    from subscription_dates sd
    cross join dimensions d
),

-- Calculate daily metrics (new subscriptions)
daily_metrics as (
    select
        date_trunc('day', start_date) as metric_date,
        brand,
        channel,
        count(distinct subscription_id) as new_subscriptions,
        sum(case when is_in_trial then 1 else 0 end) as new_trial_subscriptions,
        count(distinct customer_id) as new_customers
    from enriched_subscriptions
    group by 1, 2, 3
),

-- Calculate churn metrics
churn_metrics as (
    select
        date_trunc('day', cancellation_date) as metric_date,
        brand,
        channel,
        count(distinct subscription_id) as churned_subscriptions,
        count(distinct customer_id) as churned_customers
    from enriched_subscriptions
    where cancellation_date is not null
    group by 1, 2, 3
),

-- Active subscriptions by day
active_subscriptions as (
    select
        ddc.metric_date,
        ddc.brand,
        ddc.channel,
        count(distinct case 
            when es.start_date <= ddc.metric_date 
                and (es.end_date is null or es.end_date >= ddc.metric_date)
                and (es.cancellation_date is null or es.cancellation_date > ddc.metric_date)
            then es.subscription_id 
        end) as active_subscriptions,
        count(distinct case 
            when es.start_date <= ddc.metric_date 
                and (es.end_date is null or es.end_date >= ddc.metric_date)
                and (es.cancellation_date is null or es.cancellation_date > ddc.metric_date)
            then es.customer_id 
        end) as active_customers
    from date_dimension_cross ddc
    left join enriched_subscriptions es
        on ddc.brand = es.brand
        and ddc.channel = es.channel
    group by 1, 2, 3
),

-- Combine metrics
combined_metrics as (
    select
        coalesce(a.metric_date, d.metric_date, c.metric_date) as metric_date,
        coalesce(a.brand, d.brand, c.brand) as brand,
        coalesce(a.channel, d.channel, c.channel) as channel,
        coalesce(d.new_subscriptions, 0) as new_subscriptions,
        coalesce(d.new_trial_subscriptions, 0) as new_trial_subscriptions,
        coalesce(d.new_customers, 0) as new_customers,
        coalesce(c.churned_subscriptions, 0) as churned_subscriptions,
        coalesce(c.churned_customers, 0) as churned_customers,
        coalesce(a.active_subscriptions, 0) as active_subscriptions,
        coalesce(a.active_customers, 0) as active_customers
    from active_subscriptions a
    full outer join daily_metrics d
        on a.metric_date = d.metric_date
        and a.brand = d.brand
        and a.channel = d.channel
    full outer join churn_metrics c
        on a.metric_date = c.metric_date
        and a.brand = c.brand
        and a.channel = c.channel
),

final_metrics as (
    select
        metric_date,
        brand,
        channel,
        new_subscriptions,
        new_trial_subscriptions,
        new_customers,
        churned_subscriptions,
        churned_customers,
        active_subscriptions,
        active_customers,
        
        -- Calculate derived metrics
        case 
            when lag(active_subscriptions) over (partition by brand, channel order by metric_date) > 0 
            then round(churned_subscriptions::float / lag(active_subscriptions) over (partition by brand, channel order by metric_date) * 100, 2)
            else 0
        end as churn_rate_pct,
        
        -- Growth rate
        case 
            when lag(active_subscriptions) over (partition by brand, channel order by metric_date) > 0 
            then round((active_subscriptions - lag(active_subscriptions) over (partition by brand, channel order by metric_date))::float 
                / lag(active_subscriptions) over (partition by brand, channel order by metric_date) * 100, 2)
            else 0
        end as growth_rate_pct
        
    from combined_metrics
)

select * from final_metrics
order by brand, channel, metric_date