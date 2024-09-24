with source as (
    select * from {{ source('public', 'factorial_contracts') }}
),

casted as (
    select
        id,
        employee_id,
        country,
        job_title,
        role,
        level,
        effective_on,
        starts_on,
        ends_on,
        has_payroll,
        salary_frequency,
        working_week_days,
        working_hours_frequency,
        trial_period_ends_on,
        es_has_teleworking_contract,
        es_cotization_group,
        es_contract_observations,
        es_job_description,
        es_contract_type_id,
        es_working_day_type_id,
        es_education_level_id,
        es_professional_category_id,
        fr_employee_type,
        fr_forfait_jours,
        fr_jours_par_an,
        fr_coefficient,
        fr_contract_type_id,
        fr_level_id,
        fr_step_id,
        fr_mutual_id,
        fr_professional_category_id,
        fr_work_type_id,
        de_contract_type_id,
        pt_contract_type_id,
        compensation_ids,
        created_at,
        updated_at,
        salary_amount / 100 as salary_amount,
        working_hours / 100 as working_hours
    from source
)

select *
from casted
