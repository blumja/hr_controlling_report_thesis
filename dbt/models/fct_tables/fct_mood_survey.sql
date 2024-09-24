select
    "date",
    cast(left(rating, 1) as int) as rating,
    feedback,
    feedback_bad_rating,
    case
        when feedback is null and feedback_bad_rating is null then 0
        else 1
    end as feedback_exists
from {{ ref('mood_survey_answers') }}
