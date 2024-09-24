with source as (
    select * from {{ source('public', 'factorial_custom_tables_val') }}
),

casted as (
    select
        id,
        employee_id,
        col_val,
        table_id
    from source
)

select *
from casted
