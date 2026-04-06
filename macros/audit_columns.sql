{% macro audit_columns() %}
    current_timestamp()                                 as _loaded_at,
    '{{ this.database }}'                               as _source_database,
    '{{ this.schema }}'                                 as _source_schema,
    '{{ this.name }}'                                   as _source_table,
    '{{ invocation_id }}'                               as _dbt_invocation_id,
    '{{ env_var("DBT_ENVIRONMENT", "development") }}'   as _environment
{% endmacro %}
