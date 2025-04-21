{{
  config(
    materialized = 'table',
    tags = ['utils']
  )
}}

-- Date spine from 1 year ago to 1 year in the future
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="dateadd('year', -1, current_date())",
        end_date="dateadd('year', 1, current_date())"
    )
    }}
)

select
    date_day as date,
    date_trunc('week', date_day) as week,
    date_trunc('month', date_day) as month,
    date_trunc('quarter', date_day) as quarter,
    date_trunc('year', date_day) as year,
    dayname(date_day) as day_name,
    month(date_day) as month_num,
    monthname(date_day) as month_name,
    quarter(date_day) as quarter_num,
    year(date_day) as year_num,
    case 
        when date_day = current_date() then 'Today'
        when date_day > current_date() then 'Future'
        else 'Past'
    end as time_period
from date_spine