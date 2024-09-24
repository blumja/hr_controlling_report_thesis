with joined_tables as (
    select
        {{ ref("cleaned_absences") }}.date,
        {{ ref("cleaned_absences") }}.troi_name,
        {{ ref("cleaned_absences") }}.industrial_working_hours,
        {{ ref("cleaned_absences") }}.planned_amount,
        {{ ref("cleaned_absences") }}.wb,
        {{ ref("cleaned_absences") }}.nwb,
        {{ ref("cleaned_absences") }}.absence_time_hours,
        {{ ref('industrial_break_time') }}.break_amount
    from {{ ref("cleaned_absences") }}
    left join {{ ref('industrial_break_time') }}
        on
            {{ ref("cleaned_absences") }}.troi_name
            = {{ ref('industrial_break_time') }}.troi_name
            and {{ ref("cleaned_absences") }}.date
            = {{ ref('industrial_break_time') }}.date
),

filter_col as (
    select
        *,
        {{ dbt_utils.safe_add([
            "industrial_working_hours",
            "break_amount",
            "planned_amount",
            "wb",
            "nwb",
            "absence_time_hours"
        ]) }} as "filter_col"
    from joined_tables
),

total_ist_col as (
    select
        *,
        {{ dbt_utils.safe_add([
            "industrial_working_hours",
            "absence_time_hours"
        ]) }} as "total_sum"
    from filter_col
),

legal_break_time as (
    select
        *,
        case
            when industrial_working_hours <= 6 then 0
            when
                industrial_working_hours > 6 and industrial_working_hours <= 9
                then 0.5
            when industrial_working_hours > 9 then 0.75
        end as legal_break_time
    from total_ist_col
),

compliance_break_time as (
    select
        *,
        case
            when legal_break_time <= break_amount then 'O'
            when legal_break_time > break_amount then 'X'
            when break_amount is null and legal_break_time > 0 then 'X'
            when legal_break_time = 0 and break_amount is null then 'O'
        end as compliance_break_time
    from legal_break_time
)

select
    date,
    troi_name as "full_name",
    industrial_working_hours as "worked_amount",
    break_amount,
    planned_amount,
    round(
        cast(
            {{ dbt_utils.safe_subtract([
                "total_sum",
                "planned_amount"
            ]) }} as numeric
        ),
        2
    ) as "difference",
    wb,
    nwb,
    legal_break_time,
    compliance_break_time
from compliance_break_time
where "filter_col" > 0
