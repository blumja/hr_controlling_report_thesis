{% macro convert_to_industrial(column, new_column) %}
case
	when length({{ column }}) = 5
		then
			cast(
				extract(
					hour from to_timestamp({{ column }}, 'HH24:MI')
				) as decimal(5, 2)
			)
			+ cast(extract(minute from to_timestamp({{ column }}, 'HH24:MI')) / 60 as decimal(5, 2))
end as {{ new_column }}
{% endmacro %}
