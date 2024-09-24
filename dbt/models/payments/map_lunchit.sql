with distinct_ids as (
    select
        distinct(id) as id,
        email
    from {{ ref('name_mapping_pto') }}
    where email is not null
),

map_emails as (
    select
        {{ ref("processed_lunchit")}}.*,
        distinct_ids.email
    from {{ ref("processed_lunchit")}}
    left join distinct_ids
        on {{ ref("processed_lunchit")}}.id = distinct_ids.id
)

select *
from map_emails
