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

cases_calc as (
    select
        *,
        case
            when
                salary_frequency = 'monthly'
                then round(((3.0 * salary_amount) / 13) / working_hours, 2)
            when salary_frequency = 'hourly' then salary_amount
        end as salary_hour,
        case
            when working_hours = 40 then 'Vollzeit'
            when working_hours is not null then 'Teilzeit'
            else 'Nicht definiert'
        end as working_hours_type,
        case
            when termination_date is not null then 0
            when
                effective_on
                = max(
                    case when effective_on <= current_date then effective_on end
                ) over (partition by employee_id)
                then 1
            else 0
        end as latest_contract

    from join_termination_date
),

distinct_jobrole as (
    select
        employee_id,
        job_title,
        min(effective_on) as start_date_jobrole
    from cases_calc
    group by employee_id, job_title
)

select
    cases_calc.id as contract_id,
    cases_calc.employee_id as factorial_id,
    cases_calc.job_title,
    cases_calc.effective_on as start_date,
    cases_calc.ends_on as end_date,
    cases_calc.salary_amount,
    cases_calc.salary_hour,
    cases_calc.salary_frequency,
    cases_calc.working_hours,
    cases_calc.working_hours_frequency,
    cases_calc.working_hours_type,
    cases_calc.latest_contract,
    distinct_jobrole.start_date_jobrole
from cases_calc
inner join distinct_jobrole
    on
        cases_calc.employee_id = distinct_jobrole.employee_id
        and cases_calc.job_title = distinct_jobrole.job_title
