with distinct_ids as (
    select
        distinct(id) as id,
        troi_name
    from {{ ref('name_mapping_pto') }}
    where troi_name is not null
)

select
    {{ ref('processed_troi') }}.*,
    distinct_ids.troi_name
from {{ ref('processed_troi') }}
left join distinct_ids
    on {{ ref('processed_troi') }}.id = distinct_ids.id
