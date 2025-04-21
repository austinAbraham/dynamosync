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

-- Calculate daily metrics
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

-- Create a date dimension since we don't have the date spine yet
date_dimension as (
    select distinct
        date_trunc('day', d) as date
    from (
        select dateadd('day', seq4(), dateadd('year', -1, current_date())) as d
        from table(generator(rowcount => 730)) -- ~2 years of dates
    )
    where d <= dateadd('year', 1, current_date())
),

-- Cross join with brands and channels to ensure we have all combinations
dimensions as (
    select distinct
        date_dimension.date as metric_date,
        es.brand,
        es.channel
    from date_dimension
    cross join (select distinct brand, channel from enriched_subscriptions) es
),

-- Active subscriptions by day
active_subscriptions as (
    select
        date_trunc('day', d.metric_date) as metric_date,
        d.brand,
        d.channel,
        count(distinct case 
            when es.start_date <= d.metric_date 
                and (es.end_date is null or es.end_date >= d.metric_date)
                and (es.cancellation_date is null or es.cancellation_date > d.metric_date)
            then es.subscription_id 
        end) as active_subscriptions,
        count(distinct case 
            when es.start_date <= d.metric_date 
                and (es.end_date is null or es.end_date >= d.metric_date)
                and (es.cancellation_date is null or es.cancellation_date > d.metric_date)
            then es.customer_id 
        end) as active_customers
    from dimensions d
    left join enriched_subscriptions es
        on d.brand = es.brand
        and d.channel = es.channel
    group by 1, 2, 3
),

-- Combine metrics
combined_metrics as (
    select
        coalesce(d.metric_date, c.metric_date, a.metric_date) as metric_date,
        coalesce(d.brand, c.brand, a.brand) as brand,
        coalesce(d.channel, c.channel, a.channel) as channel,
        coalesce(d.new_subscriptions, 0) as new_subscriptions,
        coalesce(d.new_trial_subscriptions, 0) as new_trial_subscriptions,
        coalesce(d.new_customers, 0) as new_customers,
        coalesce(c.churned_subscriptions, 0) as churned_subscriptions,
        coalesce(c.churned_customers, 0) as churned_customers,
        coalesce(a.active_subscriptions, 0) as active_subscriptions,
        coalesce(a.active_customers, 0) as active_customers
    from daily_metrics d
    full outer join churn_metrics c
        on d.metric_date = c.metric_date
        and d.brand = c.brand
        and d.channel = c.channel
    full outer join active_subscriptions a
        on coalesce(d.metric_date, c.metric_date) = a.metric_date
        and coalesce(d.brand, c.brand) = a.brand
        and coalesce(d.channel, c.channel) = a.channel
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