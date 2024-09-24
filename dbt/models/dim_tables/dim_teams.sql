select
    name as team_name,
    cast(jsonb_array_elements(employee_ids) as int) as factorial_id
from {{ ref('factorial_teams') }}
