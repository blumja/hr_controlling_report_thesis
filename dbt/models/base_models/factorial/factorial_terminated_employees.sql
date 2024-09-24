with source as (
    select * from {{ source('public', 'factorial_employees') }}
),

casted as (
    select
        id,
        terminated_on,
        termination_reason,
        termination_reason_type,
        termination_observations
    from source
    where terminated_on is not null
)

select *
from casted
