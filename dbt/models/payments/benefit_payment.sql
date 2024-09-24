with join_contracts_data as (
    select
        {{ ref('factorial_compensation') }}.*,
        {{ ref('fill_contract_dates') }}.end_date,
        {{ ref('fill_contract_dates') }}.employee_id,
        {{ ref('fill_contract_dates') }}.date as contract_start
    from {{ ref('factorial_compensation') }}
    inner join {{ ref('fill_contract_dates') }}
        on
            {{ ref('factorial_compensation') }}.contract_version_id
            = {{ ref('fill_contract_dates') }}.contract_id
    where
        {{ ref('factorial_compensation') }}.amount is not null
        and {{ ref('factorial_compensation') }}.amount != 0
),

date_range as (
    select
        *,
        cast(generate_series(
            coalesce("starts_on", "contract_start"),
            "end_date",
            interval '1 month'
        ) as date) as "date"
    from join_contracts_data
)

select distinct on (
    "date", "contracts_taxonomy_id", "employee_id", "amount"
)
    employee_id as factorial_id,
    amount,
    date,
    end_date,
    'Benefit' as "type",
    "description" as "name",
    contract_version_id,
    contracts_taxonomy_id,
    id as compensation_id
from date_range
