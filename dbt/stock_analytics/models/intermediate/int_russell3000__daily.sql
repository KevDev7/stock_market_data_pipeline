-- Builds daily market data enriched with Russell 3000 attributes (incremental model).

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'replace_recent',
    tmp_relation_type = 'table',
    unique_key = ['ticker', 'trade_date'],
    on_schema_change = 'fail'
) }}

WITH russell_3000 AS (
    -- Time-aware dimension: defines when a ticker is considered part of the Russell 3000
    SELECT *
    FROM {{ ref('int_russell3000__membership') }}
),

full_market AS (
    -- Daily market fact data at ticker × trade_date grain
    SELECT *
    FROM {{ ref('stg_daily_stocks') }}
    -- Define the accepted analytical population before LAG or rolling measures.
    -- Rejected records remain available in STAGING and RAW for inspection.
    WHERE ticker IS NOT NULL
      AND is_valid_record = 1
      AND has_volume = 1
    {% if is_incremental() %}
        -- On incremental runs, only reprocess recent days
        -- Handles late data, corrections, and retries
        AND trade_date >= {{ recent_rebuild_start(this) }}
    {% endif %}
),

joined AS (
    -- Enrich daily prices with Russell 3000 attributes
    -- Use the shared snapshot-derived windows, including the documented backcast.
    SELECT 
        f.ticker,
        f.trade_date,
        f.volume,
        f.volume_weighted_avg,
        f.open,
        f.close,
        f.high,
        f.low,
        f.num_transactions,
        f.ingested_at,
        f.has_volume,
        f.is_valid_record,
        r.sector,
        r.company,
        r.asset_class,
        r.location,
        r.exchange,
        r.currency,
        r.market_currency,
        r.market_weight AS index_weight
    FROM full_market AS f
    INNER JOIN russell_3000 AS r
        ON f.ticker = r.ticker
        AND f.trade_date BETWEEN r.valid_from AND r.valid_to
),

slice_ordered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS slice_position
    FROM joined
),

{% if is_incremental() %}
-- Carry state only from the untouched prefix, not from old rows inside the
-- replacement window that may now be rejected or absent.
previous_state AS (
    SELECT
        s.ticker,
        p.close AS prev_close,
        p.trade_date AS prev_trade_date,
        p.observation_count AS prev_observation_count
    FROM (
        SELECT DISTINCT ticker
        FROM slice_ordered
    ) AS s
    LEFT JOIN {{ this }} AS p
        ON s.ticker = p.ticker
       AND p.trade_date < {{ recent_rebuild_start(this) }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.ticker
        ORDER BY p.trade_date DESC NULLS LAST
    ) = 1
),
{% endif %}

final AS (
    SELECT
        j.ticker,
        j.trade_date,
        j.volume,
        j.volume_weighted_avg,
        j.open,
        j.close,
        j.high,
        j.low,
        j.num_transactions,
        j.ingested_at,
        j.has_volume,
        j.is_valid_record,
        j.sector,
        j.company,
        j.asset_class,
        j.location,
        j.exchange,
        j.currency,
        j.market_currency,
        j.index_weight,

        -- Counts how many times this ticker has appeared, carrying state across incremental runs
        CAST(
            j.slice_position
            {% if is_incremental() %}
            + COALESCE(p.prev_observation_count, 0)
            {% endif %}
            AS NUMBER(38, 0)
        ) AS observation_count,

        {% if is_incremental() %}
        -- Get yesterday's close:
        -- 1) Use LAG if yesterday is in the current slice
        -- 2) Otherwise fall back to the historical table
        COALESCE(
            LAG(j.close) OVER (
                PARTITION BY j.ticker
                ORDER BY j.trade_date
            ),
            p.prev_close
        ) AS yesterday_close,
        {% else %}
        -- On full builds, all history is present
        -- LAG alone is sufficient
        LAG(j.close) OVER (
            PARTITION BY j.ticker
            ORDER BY j.trade_date
        ) AS yesterday_close,
        {% endif %}

        -- Flags the first day a ticker appears in the dataset
        -- If there is no previous row for this ticker, it is new
        CASE 
            WHEN LAG(j.ticker) OVER (
                    PARTITION BY j.ticker
                    ORDER BY j.trade_date
                ) IS NULL
                {% if is_incremental() %}
                AND p.prev_trade_date IS NULL
                {% endif %}
            THEN 1 
            ELSE 0 
        END AS is_first_observation

    FROM slice_ordered AS j

    {% if is_incremental() %}
    -- Join each ticker's rebuilt rows to their starting historical state
    LEFT JOIN previous_state AS p
        ON j.ticker = p.ticker
    {% endif %}
)

SELECT * FROM final
