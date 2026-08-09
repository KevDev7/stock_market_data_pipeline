-- Security-day fact table with trading and technical momentum measures.
{{ config(
    materialized = 'incremental',
    unique_key = ['security_key', 'date_key'],
    cluster_by = ['security_key'],
    on_schema_change = 'fail'
) }}

SELECT
    MD5(ticker) AS security_key,
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
    is_new_to_index,

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
FROM {{ ref('prep_security_daily_momentum') }}
{% if is_incremental() %}
WHERE trade_date >= (
    SELECT COALESCE(DATEADD(day, -4, MAX(trade_date)), TO_DATE('1900-01-01'))
    FROM {{ this }}
)
{% endif %}
