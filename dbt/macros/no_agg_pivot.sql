{% macro no_agg_pivot(ref, col, val) %}

{% set columns = dbt_utils.get_column_values(ref, col) %}

{% for col in columns %}
    MAX(CASE WHEN col_name = '{{ col }}' THEN {{ val }} END) AS "{{ col }}" {% if not loop.last %},{% endif %}
{% endfor %}

{% endmacro %}
