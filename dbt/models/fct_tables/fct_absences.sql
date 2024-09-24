with named_absences as (
    select
        *,
        {{ dbt_utils.safe_add([
            "paid_absence_hours",
            "sick_leave_hours",
            "short_working_hours"
        ]) }} as named_absences_hours,
        {{ dbt_utils.safe_add([
            "paid_absence_mandays",
            "sick_leave_mandays",
            "short_working_mandays"
        ]) }} as named_absences_mandays
    from {{ ref('cleaned_absences') }}
),

other_absences as (
    select
        *,
        {{ dbt_utils.safe_subtract([
            "absence_time_hours",
            "named_absences_hours"
        ]) }} as other_absences_hours,
        {{ dbt_utils.safe_subtract([
            "absence_time_mandays",
            "named_absences_mandays"
        ]) }} as other_absences_mandays
    from named_absences
),

unpivot as (
    select
        date,
        troi_name as full_name,
        'Urlaub' as absence_type,
        paid_absence_hours as duration_hours,
        paid_absence_mandays as duration_mandays
    from other_absences
    union all
    select
        date,
        troi_name as full_name,
        'Krank' as absence_type,
        sick_leave_hours as duration_hours,
        sick_leave_mandays as duration_mandays
    from other_absences
    union all
    select
        date,
        troi_name as full_name,
        'Kurzarbeit' as absence_type,
        short_working_hours as duration_hours,
        short_working_mandays as duration_mandays
    from other_absences
    union all
    select
        date,
        troi_name as full_name,
        'Anderes' as absence_type,
        other_absences_hours as duration_hours,
        other_absences_mandays as duration_mandays
    from other_absences
)

select
    date,
    full_name,
    absence_type,
    duration_hours,
    duration_mandays
from unpivot
where duration_hours is not null and duration_hours > 0
