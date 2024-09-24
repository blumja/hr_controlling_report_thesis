with source as (
    select * from {{ source('public', 'processed_troi') }}
),

renamed as (
    select
        "Monat" as "monat",
        "Checked_In" as "checked_in",
        "Checked_Out" as "checked_out",
        "Pausenzeiten" as "pausenzeiten",
        "Arbeitszeit" as "arbeitszeit",
        {{ clean_cast("erfasste_Stunden", "erfasste_stunden") }},
        "Urlaub__PT___Std__" as "urlaub",
        "krank__PT___Std__" as "krank",
        "Kurzarbeit__PT___Std__" as "kurzarbeit",
        "Abwesenheiten__PT___Std__" as "abwesenheiten",
        {{ clean_cast("Gesamt", "gesamt") }},
        {{ clean_cast("WB", "wb") }},
        {{ clean_cast("NWB", "nwb") }},
        {{ clean_cast("wb_nwb", "wb_nwb") }},
        {{ clean_cast("an_ab", "an_ab") }},
        {{ clean_cast("Soll", "soll") }},
        {{ clean_cast("Saldo", "saldo") }},
        cast(split_part(id, '.', 1) as int) as id
    from source
),

cleaned_day_entries as (
    select *
    from renamed
    where
        monat != ''
        and coalesce(monat ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', false)
)

select *
from cleaned_day_entries
