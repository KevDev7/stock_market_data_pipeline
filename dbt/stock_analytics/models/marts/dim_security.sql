-- Conformed security dimension at one row per ticker.
{{ config(materialized = 'table') }}

WITH trading_history AS (
    SELECT
        ticker,
        MIN(trade_date) AS first_trade_date,
        MAX(trade_date) AS latest_trade_date,
        COUNT(DISTINCT trade_date) AS total_trading_days
    FROM {{ ref('prep_security_daily_momentum') }}
    GROUP BY ticker
),

latest_security AS (
    SELECT
        ticker,
        company,
        sector,
        asset_class,
        location,
        exchange,
        currency,
        market_currency,
        index_weight AS current_index_weight
    FROM {{ ref('prep_security_daily_momentum') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticker
        ORDER BY trade_date DESC
    ) = 1
)

SELECT
    MD5(l.ticker) AS security_key,
    MD5(COALESCE(l.sector, 'Unknown')) AS sector_key,
    l.ticker,
    l.company,
    COALESCE(l.sector, 'Unknown') AS sector_name,
    l.asset_class,
    l.location,
    l.exchange,
    l.currency,
    l.market_currency,
    l.current_index_weight,
    h.first_trade_date,
    h.latest_trade_date,
    h.total_trading_days,
    1 AS is_current_snapshot
FROM latest_security AS l
LEFT JOIN trading_history AS h
    ON h.ticker = l.ticker
ORDER BY l.ticker
