select
    pausenzeiten as break_time_period,
    urlaub as paid_absence,
    krank as sick_leave,
    kurzarbeit as short_working,
    abwesenheiten as absence_time,
    wb,
    nwb,
    soll as planned_amount,
    troi_name,
    to_date(monat, 'YYYY-MM-DD') as "date",
    {{ convert_to_industrial('arbeitszeit', 'industrial_working_hours') }}
from {{ ref('map_names_troi') }}
