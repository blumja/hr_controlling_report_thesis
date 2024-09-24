with split_breaks as (
    select
        date,
        troi_name,
        regexp_split_to_table(break_time_period, E'\n') as "value"
    from {{ ref('industrial_working_hours') }}
),

single_break as (
    select
        date,
        troi_name,
        trim("value") as "break"
    from split_breaks
    where "value" != ''
),

start_end as (
    select
        date,
        troi_name,
        trim(split_part("break", '-', 1)) as break_start,
        trim(split_part("break", '-', 2)) as break_end
    from single_break
),

break_min as (
    select
        date,
        break_start,
        break_end,
        troi_name,
        extract(
            epoch from (
                to_timestamp(break_end, 'HH24:MI')
                - to_timestamp(break_start, 'HH24:MI')
            )
        )
        / 60 as break_time_min
    from start_end
),

break_time as (
    select
        date,
        break_time_min,
        troi_name,
        to_char(interval '1 minute' * break_time_min, 'HH24:MI') as break_hhmm
    from break_min
),

break_industrial as (
    select
        "date",
        troi_name,
        {{ convert_to_industrial('break_hhmm', 'industrial_break_time') }}
    from break_time
)

select
    "date",
    troi_name,
    sum(industrial_break_time) as break_amount
from break_industrial
group by "date", troi_name
