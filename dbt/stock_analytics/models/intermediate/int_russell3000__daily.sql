-- Builds daily market data enriched with Russell 3000 attributes (incremental model).

{{ config(
    materialized = 'incremental',
    unique_key = ['ticker', 'trade_date'],
    on_schema_change = 'fail'
) }}

WITH russell_3000 AS (
    SELECT *
    FROM {{ ref('stg_russell3000__constituents') }}
),

full_market AS (
    SELECT DISTINCT *
    FROM {{ ref('stg_daily_stocks') }}
    {% if is_incremental() %}
        WHERE trade_date >= (
            SELECT DATEADD(day, -4, MAX(trade_date))
            FROM {{ this }}
        )
    {% endif %}
),

joined AS (
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
        r.market_weight AS index_weight
    FROM full_market AS f
    INNER JOIN russell_3000 AS r
        ON f.ticker = r.ticker
        AND f.trade_date BETWEEN r.valid_from AND r.valid_to
),

{% if is_incremental() %}
prev_day_close AS (
    SELECT
        ticker,
        trade_date,
        close AS prev_close
    FROM {{ this }}
),
{% endif %}

final AS (
    SELECT
        j.*,

        ROW_NUMBER() OVER (
            PARTITION BY j.ticker
            ORDER BY j.trade_date
        ) AS consecutive_trading_days,

        {% if is_incremental() %}
        COALESCE(
            LAG(j.close) OVER (
                PARTITION BY j.ticker
                ORDER BY j.trade_date
            ),
            p.prev_close
        ) AS yesterday_close,
        {% else %}
        LAG(j.close) OVER (
            PARTITION BY j.ticker
            ORDER BY j.trade_date
        ) AS yesterday_close,
        {% endif %}

        CASE 
            WHEN LAG(j.ticker) OVER (
                PARTITION BY j.ticker
                ORDER BY j.trade_date
            ) IS NULL 
            THEN 1 
            ELSE 0 
        END AS is_new_to_index

    FROM joined AS j

    {% if is_incremental() %}
    LEFT JOIN prev_day_close AS p
        ON j.ticker = p.ticker
       AND j.trade_date = DATEADD(day, 1, p.trade_date)
    {% endif %}
)

SELECT * FROM final
