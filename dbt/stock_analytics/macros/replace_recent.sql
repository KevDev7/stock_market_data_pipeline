{% macro recent_rebuild_start(relation, date_column='trade_date') -%}
    (SELECT COALESCE(
        DATEADD(day, -{{ var('correction_lookback_days', 4) }}, MAX({{ date_column }})),
        TO_DATE('1900-01-01')
    ) FROM {{ relation }})
{%- endmacro %}

{% macro get_incremental_replace_recent_sql(arg_dict) %}
    {# A frozen temp table is required: deletions must not change the model inputs.
       Replace the whole output window, including keys now absent from the source.
       A normal merge cannot remove a record that has become invalid or ineligible. #}
    {% set target = arg_dict['target_relation'] %}
    {% set incoming = arg_dict['temp_relation'] %}
    {% set columns = get_quoted_csv(arg_dict['dest_columns'] | map(attribute='name')) %}
    {% set dml %}
        DELETE FROM {{ target }}
        WHERE trade_date >= {{ recent_rebuild_start(target) }};

        INSERT INTO {{ target }} ({{ columns }})
        SELECT {{ columns }} FROM {{ incoming }}
    {% endset %}
    {{ return(snowflake_dml_explicit_transaction(dml)) }}
{% endmacro %}
