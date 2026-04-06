{% macro generate_cast(columns) %}
    {% for col_name, col_type in columns.items() %}
        cast({{ col_name }} as {{ col_type }}) as {{ col_name }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
