with name_join as (
    select
        cast(pto.jahr as date) as "year",
        nm.troi_name,
        pto.urlaubsanspruch_pt as pto_entitlement,
        pto.resturlaub_vorjahr as rest_pto_previous_year
    from {{ ref('name_mapping_pto') }} as nm
    inner join {{ ref('name_mapping_pto') }} as pto
        on nm.id = pto.id
),

taken_pto as (
    select
        troi_name,
        sum(paid_absence_mandays) as taken_pto,
        date_trunc('year', date) as "year"
    from {{ ref('cleaned_absences') }}
    group by troi_name, date_trunc('year', date)
)

select
    name_join.year,
    name_join.troi_name as full_name,
    name_join.pto_entitlement as amount,
    name_join.rest_pto_previous_year as rest_amount_previous_year,
    coalesce(taken_pto.taken_pto, 0) as taken_pto
from name_join
full join taken_pto
    on
        name_join.troi_name = taken_pto.troi_name
        and name_join.year = taken_pto.year
where name_join.year is not null and name_join.troi_name is not null
