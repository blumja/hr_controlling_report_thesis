with source as (
    select * from {{ source('public', 'factorial_compensation') }}
),

casted as (
    select
        id,
        contract_version_id,
        description,
        compensation_type,
        recurrence,
        first_payment_on,
        sync_with_supplements,
        contracts_taxonomy_id,
        payroll_policy_id,
        recurrence_count,
        starts_on,
        unit,
        calculation,
        amount / 100 as amount
    from source
)

select *
from casted
