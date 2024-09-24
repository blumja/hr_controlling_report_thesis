with source as (
    select * from {{ source('public', 'factorial_custom_tables') }}
),

casted as (
    select
        id,
        name,
        created_at,
        topic_name,
        custom_resources_topic_id,
        reportable,
        hidden
    from source
)

select *
from casted
