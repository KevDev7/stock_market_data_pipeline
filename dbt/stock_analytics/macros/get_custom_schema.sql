-- Resolves the schema name: use custom schema when provided, otherwise fall back to target.schema.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ var('schema_prefix', '') }}{{ target.schema }}
    {%- else -%}
        {{ var('schema_prefix', '') }}{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
