with fill_weekend as (
    select
        troi_name as full_name,
        date,
        case
            when extract(isodow from date) > 5
            then 1
            else sick_leave_mandays
        end as sick_leave_mandays
    from {{ ref("cleaned_absences") }}
),

ranked_entries as (
    select
        full_name,
        date,
        rank() over (
            partition by full_name
            order by date
        ) as rnk
    from fill_weekend
    where sick_leave_mandays is not null
),

date_groups as (
    select
        full_name,
        date,
        date - interval '1' day * rnk as grp_date,
        case
            when extract(isodow from date) > 5
            then 1
            else 0
        end as weekend_day
    from
        ranked_entries
),

interval_consecutive as (
    select
        min(date) as interval_start_date,
        max(date) as interval_end_date,
        1 + date_part('day',
            cast(max(date) as timestamp) - cast(min(date)as timestamp)
            ) - sum(weekend_day) as consecutive_days,
        full_name
    from date_groups
    group by full_name, grp_date
    order by full_name, interval_start_date
),

correct_start_end as (
    select
        case
            when extract(isodow from interval_start_date) = 6
            then cast(interval_start_date + interval '2 days' as date)
            when extract(isodow from interval_start_date) = 7
            then cast(interval_start_date + interval '1 day' as date)
            else cast(interval_start_date as date)
        end as interval_start_date,
        case
            when extract(isodow from interval_end_date) = 6
            then cast(interval_end_date - interval '1 day' as date)
            when extract(isodow from interval_end_date) = 7
            then cast(interval_end_date - interval '2 days' as date)
            else cast(interval_end_date as date)
        end as interval_end_date,
        consecutive_days,
        full_name
    from interval_consecutive
    where consecutive_days > 0
)

select
    interval_start_date,
    interval_end_date,
    consecutive_days
from correct_start_end
