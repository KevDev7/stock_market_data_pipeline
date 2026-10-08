-- Conformed security dimension at one row per security identity.
{{ config(materialized = 'table') }}

WITH trading_history AS (
    SELECT
        security_key,
        MIN(trade_date) AS first_trade_date,
        MAX(trade_date) AS latest_trade_date,
        COUNT(DISTINCT trade_date) AS total_trading_days
    FROM {{ ref('calc_security_daily_momentum') }}
    WHERE trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
    GROUP BY security_key
),

latest_security AS (
    SELECT
        security_key,
        ticker,
        company,
        sector,
        asset_class,
        location,
        exchange,
        currency,
        market_currency,
        index_weight AS current_index_weight
    FROM {{ ref('calc_security_daily_momentum') }}
    WHERE trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY security_key
        ORDER BY trade_date DESC
    ) = 1
)

SELECT
    l.security_key,
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
    IFF(h.latest_trade_date=(SELECT MAX(trade_date) FROM {{ ref('calc_security_daily_momentum') }}),1,0) AS is_current_snapshot
FROM latest_security AS l
LEFT JOIN trading_history AS h
    ON h.security_key = l.security_key
ORDER BY l.ticker
