with pivot_training as (
    select
        factorial_id,
        {{ no_agg_pivot(
            ref('factorial_map_custom_tables'),
            'col_name',
            'val'
        ) }}
    from {{ ref('factorial_map_custom_tables') }}
    where table_name like '%Weiterbildung%'
    group by factorial_id, id
)

select
    factorial_id,
    "Titel" as title,
    cast("Datum" as date) as "date",
    "Typ" as "type",
    case
        when cast("Enddatum" as date) is null then cast("Datum" as date)
        else cast("Enddatum" as date)
    end as end_date,
    cast("Kosten" as int) / 100 as cost
from pivot_training
