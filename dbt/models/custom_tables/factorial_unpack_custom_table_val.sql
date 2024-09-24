select
    id,
    employee_id,
    table_id,
    cast((jsonb_each_text(col_val)).key as int) as col_id,
    (jsonb_each_text(col_val)).value as val
from {{ ref('factorial_custom_tables_val') }}
