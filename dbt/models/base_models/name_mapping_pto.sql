with source as (
    select * from {{ source('public', 'name_mapping') }}
),

renamed as (
    select
        cast(id as int) as id,
        email,
        troi_name,
        cast(jahr as date) as jahr,
        name,
        urlaubsanspruch_pt,
        resturlaub_vorjahr
    from source
)

select *
from renamed
