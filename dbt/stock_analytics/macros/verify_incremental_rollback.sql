{# Test-only operation: deliberately fail INSERT after the real replacement DELETE.
   Refuses every database except one owned by the pre-push harness. #}
{% macro verify_incremental_rollback() %}
    {% if not modules.re.fullmatch('PREPUSH_TEST_[0-9A-F]{12}', target.database) %}
        {{ exceptions.raise_compiler_error('Rollback probe requires an isolated pre-push database') }}
    {% endif %}
    {% set databases = run_query("SHOW DATABASES LIKE '" ~ target.database ~ "'") %}
    {% if databases | length != 1 or databases.rows[0]['comment'] != 'stock_market_pipeline_pre_push_incremental_test' %}
        {{ exceptions.raise_compiler_error('Rollback probe database ownership mismatch') }}
    {% endif %}
    {% set destination = api.Relation.create(database=target.database, schema='CHECKS', identifier='ROLLBACK_TARGET') %}
    {% set incoming = api.Relation.create(database=target.database, schema='CHECKS', identifier='ROLLBACK_INCOMING') %}
    {% set session = run_query('SELECT CURRENT_SESSION()') %}
    {% do log('ROLLBACK_TEST_SESSION=' ~ session.rows[0][0], info=true) %}
    {% set args = {'target_relation':destination, 'temp_relation':incoming,
                   'dest_columns':adapter.get_columns_in_relation(destination)} %}
    {% do run_query(get_incremental_replace_recent_sql(args)) %}
    {{ exceptions.raise_compiler_error('Expected INSERT failure did not occur') }}
{% endmacro %}
