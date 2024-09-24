with source as (
    select * from {{ source('public', 'processed_lunchit') }}
),

clean_cast as (
    select
        {{ clean_cast('id', 'id') }},
        "Lunchit_Beleg_ID" as "lunchit_beleg_id",
        nullif("Abrechnungsmonat", '') as "abrechnungsmonat",
        nullif("Datum", '') as datum,
        {{ clean_cast('Lunchit_1_2', 'lunchit_1_2') }},
        {{ clean_cast('Lunchit_2_2', 'lunchit_2_2') }},
        {{ clean_cast('Eingereichter_Betrag', 'eingereichter_betrag') }},
        {{ clean_cast('Erstattungsbetrag', 'erstattungsbetrag') }},
        {{ clean_cast('Zuzahlung', 'zuzahlung') }}
    from source
),

clean_date as (
    select
        lunchit_beleg_id,
        lunchit_1_2,
        lunchit_2_2,
        eingereichter_betrag,
        erstattungsbetrag,
        zuzahlung,
        cast(id as int) as id,
        to_date("abrechnungsmonat", 'DD.MM.YYYY') as "abrechnungsmonat",
        to_date(datum, 'DD.MM.YYYY') as "datum"
    from clean_cast
)

select *
from clean_date
