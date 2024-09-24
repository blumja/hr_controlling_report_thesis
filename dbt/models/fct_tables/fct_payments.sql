with union_payments as (
    {{ dbt_utils.union_relations(
    relations=[ref('salary_payment'), ref("lunchit_payment"), ref('inflation_payment'), ref('benefit_payment')]
    ) }}
),

grouped_payments as (
    select
        type,
        date_trunc('month', date) as "date",
        sum(amount) as amount
    from union_payments
    group by date_trunc('month', date), type, factorial_id
),

average_payments as (
    select
        date,
        type,
        avg(amount) as average_amount
    from grouped_payments
    group by date, type
),

actual_payments as (
    select
        factorial_id,
        amount,
        date,
        type,
        name,
        date_trunc('month', date) as "month"
    from union_payments
)

select
    actual_payments.factorial_id,
    actual_payments.amount,
    actual_payments.date,
    actual_payments.type,
    actual_payments.name,
    round(average_payments.average_amount, 2) as average_amount
from actual_payments
left join average_payments
    on
        actual_payments.month = average_payments.date
        and actual_payments.type = average_payments.type
