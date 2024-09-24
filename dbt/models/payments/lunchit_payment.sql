with add_payment as (
    select
        email,
        abrechnungsmonat,
        date_trunc(
            'month', cast(abrechnungsmonat as timestamp) - interval '1 month'
        ) as "date",
        {{ dbt_utils.safe_add(['lunchit_1_2', 'lunchit_2_2']) }} as zuzahlung
    from {{ ref('map_lunchit') }}
    where abrechnungsmonat is not null
),

add_id as (
    select
        add_payment.*,
        {{ ref('factorial_active_employees') }}.id as factorial_id
    from add_payment
    inner join {{ ref('factorial_active_employees') }}
        on add_payment.email = {{ ref('factorial_active_employees') }}.email
)

select
    cast(date as date) as "date",
    zuzahlung as amount,
    'Benefit' as "type",
    'Lunchit' as "name",
    factorial_id
from add_id
where zuzahlung > 0
