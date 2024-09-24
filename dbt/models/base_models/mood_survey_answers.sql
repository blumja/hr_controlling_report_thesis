with source as (
    select * from {{ source('public', 'mood_survey_answers') }}
),

renamed as (
    select
        response_id,
        cast(date as date) as "date",
        rating,
        feedback,
        feedback_bad_rating,
        "__PowerAppsId__"
    from source
)

select *
from renamed
