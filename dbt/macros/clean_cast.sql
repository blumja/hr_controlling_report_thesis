{% macro clean_cast(column, new_column) %}
cast(replace(nullif("{{ column }}", ''), ',', '.') as float) as {{ new_column }}
{% endmacro %}
