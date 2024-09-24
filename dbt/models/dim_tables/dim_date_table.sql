with date_range as (
    select
        min(start_date) as start_date,
        current_date + interval '2 year' as end_date
    from {{ ref('dim_employees') }}
),

dates as (
    select
        cast(generate_series(
            start_date, end_date, interval '1 day'
        ) as date) as date_day
    from date_range
)

select
    date_day as "date",
    cast(extract(isodow from date_day) as int) as day_of_week,
    cast(extract(week from date_day) as int) as "week",
    to_char(date_day, 'Day') as day_of_week_name,
    to_char(date_day, 'IW') || extract(year from date_day) as week_year
from dates
