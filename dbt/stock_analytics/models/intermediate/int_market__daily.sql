-- Accepted prices enriched with same-day reference identity and observed industry.
{{ config(materialized='incremental', incremental_strategy='replace_recent',
          tmp_relation_type='table', unique_key=['security_key','trade_date'], on_schema_change='fail') }}
WITH prices AS (
    SELECT * FROM {{ ref('stg_daily_stocks') }}
    WHERE ticker IS NOT NULL AND is_valid_record=1 AND has_volume=1
      AND trade_date BETWEEN TO_DATE('{{ var("warmup_start", "2022-12-29") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
    {% if is_incremental() %}
      AND trade_date >= {{ recent_rebuild_start(this) }}
    {% endif %}
), joined AS (
    SELECT r.security_key, p.*, r.sector, r.company, r.asset_class,
           r.location, r.exchange, r.currency, r.market_currency,
           CAST(NULL AS DOUBLE) AS index_weight
    FROM prices p INNER JOIN {{ ref('int_market__reference_daily') }} r
        ON p.ticker=r.ticker AND p.trade_date=r.trade_date
), ordered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY security_key ORDER BY trade_date) AS slice_position
    FROM joined
)
{% if is_incremental() %}
, previous AS (
    SELECT s.security_key, p.close AS previous_close, p.observation_count AS previous_count
    FROM (SELECT DISTINCT security_key FROM ordered) s
    LEFT JOIN {{ this }} p ON s.security_key=p.security_key
                        AND p.trade_date < {{ recent_rebuild_start(this) }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.security_key ORDER BY p.trade_date DESC NULLS LAST)=1
)
{% endif %}
SELECT
    j.* EXCLUDE (slice_position),
    CAST(j.slice_position {% if is_incremental() %} + COALESCE(p.previous_count,0) {% endif %} AS BIGINT) AS observation_count,
    {% if is_incremental() %}
    COALESCE(LAG(j.close) OVER(PARTITION BY j.security_key ORDER BY j.trade_date),p.previous_close) AS yesterday_close,
    IFF(j.slice_position=1 AND p.previous_count IS NULL,1,0) AS is_first_observation
    {% else %}
    LAG(j.close) OVER(PARTITION BY j.security_key ORDER BY j.trade_date) AS yesterday_close,
    IFF(j.slice_position=1,1,0) AS is_first_observation
    {% endif %}
FROM ordered j
{% if is_incremental() %} LEFT JOIN previous p ON j.security_key=p.security_key {% endif %}
