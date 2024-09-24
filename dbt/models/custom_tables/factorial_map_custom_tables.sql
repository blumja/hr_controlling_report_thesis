with mapped_custom_tables as (
    select
        {{ ref('factorial_unpack_custom_table_val') }}.*,
        {{ ref('factorial_custom_tables') }}.name as table_name,
        {{ ref('factorial_custom_tables_col') }}.label as col_name
    from {{ ref('factorial_unpack_custom_table_val') }}
    inner join {{ ref('factorial_custom_tables') }}
        on
            {{ ref('factorial_custom_tables') }}.id
            = {{ ref('factorial_unpack_custom_table_val') }}.table_id
    inner join {{ ref('factorial_custom_tables_col') }}
        on
            {{ ref('factorial_custom_tables_col') }}.id
            = {{ ref('factorial_unpack_custom_table_val') }}.col_id
)

select
    id,
    employee_id as factorial_id,
    table_id,
    table_name,
    col_id,
    col_name,
    val
from mapped_custom_tables
