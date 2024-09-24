with pivot_inflation as (
    select
        factorial_id,
        {{ no_agg_pivot(
            ref('factorial_map_custom_tables'),
            'col_name',
            'val'
        ) }}
    from {{ ref('factorial_map_custom_tables') }}
    where table_name like '%Inflation%'
    group by factorial_id, id
)

select
    factorial_id,
    cast("Datum" as date) as "date",
    'Inflation' as "type",
    'Inflation' as "name",
    cast("Betrag" as int) / 100 as amount
from pivot_inflation
