select
    email,
    troi_name
from {{ ref("name_mapping_pto") }}
where troi_name is not null
