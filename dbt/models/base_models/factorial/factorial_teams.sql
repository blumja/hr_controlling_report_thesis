with source as (
    select * from {{ source('public', 'factorial_teams') }}
),

casted as (
    select
        company_id,
        id,
        name,
        employee_ids,
        lead_ids,
        description,
        avatar
    from source
)

select *
from casted
