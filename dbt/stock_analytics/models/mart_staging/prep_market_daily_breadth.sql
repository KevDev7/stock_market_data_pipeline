-- Prepares daily market-wide breadth measures before dimensional publication.
{{ config(materialized = 'table') }}

WITH base_aggregates AS (
    SELECT
        trade_date,
        COUNT(DISTINCT ticker) AS stocks_traded,
        SUM(IFF(close = yesterday_close OR yesterday_close IS NULL, 1, 0)) AS unchanged_stocks,
        SUM(IFF(close > yesterday_close AND yesterday_close IS NOT NULL, 1, 0)) AS advances,
        SUM(IFF(close < yesterday_close AND yesterday_close IS NOT NULL, 1, 0)) AS declines,
        SUM(IFF(close > yesterday_close AND yesterday_close IS NOT NULL, volume, 0)) AS up_volume,
        SUM(IFF(close < yesterday_close AND yesterday_close IS NOT NULL, volume, 0)) AS down_volume
    FROM {{ ref('prep_security_daily_momentum') }}
    GROUP BY trade_date
),

technical_aggregates AS (
    SELECT
        trade_date,
        SUM(IFF(close = high_52week, 1, 0)) AS new_highs,
        SUM(IFF(close = low_52week, 1, 0)) AS new_lows,
        SUM(IFF(close > sma_20, 1, 0)) / COUNT(close) AS pct_market_over_sma20,
        SUM(IFF(close > sma_50, 1, 0)) / COUNT(close) AS pct_market_over_sma50,
        SUM(IFF(close > sma_200, 1, 0)) / COUNT(close) AS pct_market_over_sma200,
        AVG(rsi) AS market_rsi
    FROM {{ ref('prep_security_daily_momentum') }}
    GROUP BY trade_date
),

final AS (
    SELECT
        b.trade_date,
        b.stocks_traded,
        b.unchanged_stocks,
        b.advances,
        b.declines,
        b.up_volume,
        b.down_volume,

        t.pct_market_over_sma20,
        t.pct_market_over_sma50,
        t.pct_market_over_sma200,
        t.market_rsi,

        SUM(b.advances - b.declines) OVER (
            ORDER BY b.trade_date
        ) AS ad_line,

        IFF(
            (b.advances + b.declines + b.unchanged_stocks) > 0,
            (b.advances - b.declines)
            / (b.advances + b.declines + b.unchanged_stocks),
            NULL
        ) AS ad_percentage,

        IFF(
            b.declines IS NOT NULL AND b.declines != 0,
            b.advances / b.declines,
            NULL
        ) AS ad_ratio,

        IFF(
            b.down_volume IS NOT NULL AND b.down_volume != 0,
            b.up_volume / b.down_volume,
            NULL
        ) AS up_down_volume_ratio,

        IFF(
            t.market_rsi > 70, 'overbought',
            IFF(t.market_rsi < 30, 'oversold', 'normal')
        ) AS market_momentum,

        t.new_highs,
        t.new_lows,

        IFF(
            b.stocks_traded > 0,
            t.new_highs / b.stocks_traded,
            NULL
        ) AS record_high_pct,

        AVG(
            IFF(
                (t.new_highs + t.new_lows) > 0,
                t.new_highs / (t.new_highs + t.new_lows),
                NULL
            )
        ) OVER (
            ORDER BY t.trade_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS high_low_index

    FROM base_aggregates AS b
    LEFT JOIN technical_aggregates AS t
        ON t.trade_date = b.trade_date
)

SELECT *
FROM final
ORDER BY trade_date
