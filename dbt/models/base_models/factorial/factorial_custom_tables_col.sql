with source as (
    select * from {{ source('public', 'factorial_custom_tables_col') }}
),

casted as (
    select
        id,
        label,
        position
    from source
)

select *
from casted
