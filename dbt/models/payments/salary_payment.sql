with date_range as (
    select
        contract_id,
        employee_id,
        job_title,
        cast(
            generate_series(date, end_date, interval '1 month') as date
        ) as "date",
        salary_amount,
        salary_frequency,
        working_hours,
        working_hours_frequency
    from {{ ref('fill_contract_dates') }}
)

select distinct on (contract_id, date)
    employee_id as factorial_id,
    contract_id,
    salary_amount as amount,
    date,
    'Salary' as "type",
    'Salary' as "name"
from date_range
