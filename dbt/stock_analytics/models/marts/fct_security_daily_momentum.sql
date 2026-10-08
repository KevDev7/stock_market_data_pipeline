-- Security-day fact table with trading and technical momentum measures.
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'replace_recent',
    tmp_relation_type = 'table',
    unique_key = ['security_key', 'date_key'],
    cluster_by = ['security_key'],
    on_schema_change = 'fail'
) }}

SELECT
    security_key,
    MD5(COALESCE(sector, 'Unknown')) AS sector_key,
    TO_NUMBER(TO_CHAR(trade_date, 'YYYYMMDD')) AS date_key,
    trade_date,

    volume,
    open,
    close,
    yesterday_close,
    high,
    low,
    index_weight,
    is_first_observation,

    sma_20,
    sma_50,
    sma_200,
    high_52week,
    low_52week,
    avg_gain_14,
    avg_loss_14,
    bullish_crossover,
    golden_cross,
    death_cross,
    rel_vol,
    rsi
FROM {{ ref('calc_security_daily_momentum') }}
WHERE trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
{% if is_incremental() %}
AND trade_date >= {{ recent_rebuild_start(this) }}
{% endif %}
