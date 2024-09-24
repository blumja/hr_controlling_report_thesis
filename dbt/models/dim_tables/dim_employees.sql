with union_employees as (
    {{ dbt_utils.union_relations(
        relations=[ref('factorial_terminated_employees'), ref('factorial_active_employees')],
        source_column_name = None
    ) }}
),

min_start_date as (
    select
        employee_id,
        min(starts_on) as start_date
    from {{ ref('factorial_contracts') }}
    group by employee_id
),

join_start_date as (
    select
        union_employees.*,
        min_start_date.start_date
    from union_employees
    inner join min_start_date
        on union_employees.id = min_start_date.employee_id
)

select
    id as factorial_id,
    company_identifier as employee_id,
    email,
    first_name,
    last_name,
    full_name,
    birthday_on as birthdate,
    gender,
    terminated_on as termination_date,
    start_date
from join_start_date
