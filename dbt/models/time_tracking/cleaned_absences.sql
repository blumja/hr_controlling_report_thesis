{%- set absence_types = ["paid_absence", "sick_leave", "short_working", "absence_time"] -%}

with extract_absence as (
    select
        *,
        {% for type in absence_types %}
            right(
                {{ type }}, position('/' in {{ type }}) - 1
            ) as hours_{{ type }},
            left(
                {{ type }}, position('/' in {{ type }}) - 1
            ) as manday_{{ type }}
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ ref('industrial_working_hours') }}
),

clean_cast as (
    select
        *,
        {% for type in absence_types %}
            {{ clean_cast(
                column='hours_' + type, new_column= type + '_hours'
            ) }},
            {{ clean_cast(
                column='manday_' + type, new_column=type + '_mandays'
            ) }}
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    from extract_absence
)

select
    date,
    break_time_period,
    {% for type in absence_types %}
        {{ type }}_hours,
        {{ type }}_mandays
        {% if not loop.last %}, {% endif %}
    {% endfor %},
    wb,
    nwb,
    planned_amount,
    troi_name,
    industrial_working_hours
from clean_cast
