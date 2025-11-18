-- Aggregates daily breadth and technical signals across the Russell 3000 universe.
{{ config(
    materialized = 'table'
) }}

WITH base_aggregates AS (
    SELECT 
        trade_date,
        COUNT(DISTINCT ticker) AS stocks_traded,
        SUM(IFF(close = yesterday_close OR yesterday_close IS NULL, 1, 0)) AS unchanged_stocks,
        SUM(IFF(close > yesterday_close AND yesterday_close IS NOT NULL, 1, 0)) AS advances,
        SUM(IFF(close < yesterday_close AND yesterday_close IS NOT NULL, 1, 0)) AS declines,
        SUM(IFF(close > yesterday_close AND yesterday_close IS NOT NULL, volume, 0)) AS up_volume,
        SUM(IFF(close < yesterday_close AND yesterday_close IS NOT NULL, volume, 0)) AS down_volume
    FROM {{ ref('int_russell3000__daily') }}
    GROUP BY trade_date
),

rolling_high_low AS (
    SELECT
        *,
        IFF(
            COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252,
            MAX(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ),
            NULL
        ) AS high_52week,
        IFF(
            COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252,
            MIN(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ),
            NULL
        ) AS low_52week
    FROM {{ ref('int_russell3000__daily') }}
),

high_low_aggs AS (
    SELECT 
        trade_date,
        SUM(IFF(close = high_52week, 1, 0)) AS new_highs,
        SUM(IFF(close = low_52week, 1, 0))  AS new_lows
    FROM rolling_high_low
    GROUP BY trade_date
),

sma_aggs AS (
    SELECT
        trade_date,
        AVG(IFF(close > sma_20, 1, 0)) AS pct_market_over_sma20,
        AVG(IFF(close > sma_50, 1, 0)) AS pct_market_over_sma50,
        AVG(IFF(close > sma_200, 1, 0)) AS pct_market_over_sma200,
        AVG(rsi) AS market_rsi
    FROM {{ ref('fct_trading_momentum') }}
    GROUP BY trade_date
)

SELECT 
    b.trade_date,
    b.stocks_traded,
    b.unchanged_stocks,
    b.advances,
    b.declines,
    b.up_volume,
    b.down_volume,
    s.pct_market_over_sma20,
    s.pct_market_over_sma50,
    s.pct_market_over_sma200,
    s.market_rsi,
    SUM(b.advances - b.declines) OVER (ORDER BY b.trade_date) AS ad_line,
    IFF(
        (b.advances + b.declines + b.unchanged_stocks) > 0,
        (b.advances - b.declines) / (b.advances + b.declines + b.unchanged_stocks),
        NULL
    ) AS ad_percentage,
    IFF(b.declines > 0, b.advances / b.declines, NULL) AS ad_ratio,
    IFF(b.down_volume > 0, b.up_volume / b.down_volume, NULL) AS up_down_volume_ratio,
    IFF(s.market_rsi > 70, 'overbought',
        IFF(s.market_rsi < 30, 'oversold', 'normal')
    ) AS market_momentum,
    IFF(b.stocks_traded > 0, h.new_highs / b.stocks_traded, NULL) AS record_high_pct,
    AVG(
        IFF((h.new_highs + h.new_lows) > 0, h.new_highs / (h.new_highs + h.new_lows), NULL)
    ) OVER (ORDER BY h.trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS high_low_index
FROM base_aggregates b
LEFT JOIN sma_aggs s ON s.trade_date = b.trade_date
LEFT JOIN high_low_aggs h ON h.trade_date = b.trade_date
ORDER BY b.trade_date
