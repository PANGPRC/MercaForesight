{% macro safe_cast(column_name, column_type) %}
    {{ dbt.safe_cast(column_name, api.Column.translate_type(column_type)) }}
{% endmacro %}