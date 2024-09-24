with join_termination_date as (
    select
        {{ ref('factorial_contracts') }}.*,
        {{ ref('dim_employees') }}.termination_date
    from {{ ref('factorial_contracts') }}
    left join {{ ref('dim_employees') }}
        on
            {{ ref('factorial_contracts') }}.employee_id
            = {{ ref('dim_employees') }}.factorial_id
),

fill_ends_on as (
    select
        *,
        cast(
            (
                lead(effective_on)
                    over (partition by employee_id order by effective_on)
            )
            - interval '1 day' as date
        ) as new_end_date
    from join_termination_date
)

select
    id as contract_id,
    employee_id,
    job_title,
    cast(effective_on as date) as "date",
    salary_amount,
    salary_frequency,
    working_hours,
    working_hours_frequency,
    case
        when ends_on is not null then ends_on
        when new_end_date is not null then new_end_date
        when termination_date is not null then termination_date
        when
            effective_on > current_date
            then cast(effective_on + interval '1 year' as date)
        else cast(current_date + interval '1 year' as date)
    end as end_date
from fill_ends_on
where salary_amount is not null
