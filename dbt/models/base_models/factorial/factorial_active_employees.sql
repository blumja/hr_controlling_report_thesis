with source as (
    select * from {{ source('public', 'factorial_active_employees') }}
),

casted as (
    select
        id,
        first_name,
        last_name,
        full_name,
        email,
        birthday_on,
        terminated_on,
        termination_reason,
        termination_reason_type,
        termination_observations,
        termination_type_description,
        identifier,
        identifier_type,
        gender,
        nationality,
        bank_number,
        swift_bic,
        bank_number_format,
        country,
        city,
        state,
        postal_code,
        address_line_1,
        address_line_2,
        company_id,
        legal_entity_id,
        created_at,
        updated_at,
        manager_id,
        location_id,
        timeoff_manager_id,
        social_security_number,
        tax_id,
        timeoff_policy_id,
        team_ids,
        phone_number,
        company_identifier,
        contact_name,
        contact_number
    from source
)

select *
from casted
