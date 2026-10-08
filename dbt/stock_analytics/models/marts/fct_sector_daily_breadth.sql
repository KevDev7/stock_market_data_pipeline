-- Industry-group/day fact; legacy SECTOR identifiers preserve SQL compatibility.
{{ config(materialized = 'table') }}

WITH base_aggregates AS (
    SELECT
        trade_date,
        sector,
        COUNT(DISTINCT ticker) AS stocks_traded,
        SUM(IFF(close = yesterday_close OR yesterday_close IS NULL, 1, 0)) AS unchanged_stocks,
        SUM(IFF(close > yesterday_close AND yesterday_close IS NOT NULL, 1, 0)) AS advances,
        SUM(IFF(close < yesterday_close AND yesterday_close IS NOT NULL, 1, 0)) AS declines,
        SUM(IFF(close > yesterday_close AND yesterday_close IS NOT NULL, volume, 0)) AS up_volume,
        SUM(IFF(close < yesterday_close AND yesterday_close IS NOT NULL, volume, 0)) AS down_volume
    FROM {{ ref('calc_security_daily_momentum') }}
    WHERE trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
    GROUP BY trade_date, sector
),

technical_aggregates AS (
    SELECT
        trade_date,
        sector,
        SUM(IFF(close = high_52week, 1, 0)) AS new_highs,
        SUM(IFF(close = low_52week, 1, 0)) AS new_lows,
        SUM(IFF(close > sma_20, 1, 0)) / COUNT(close) AS pct_sector_over_sma20,
        SUM(IFF(close > sma_50, 1, 0)) / COUNT(close) AS pct_sector_over_sma50,
        SUM(IFF(close > sma_200, 1, 0)) / COUNT(close) AS pct_sector_over_sma200,
        AVG(rsi) AS sector_rsi
    FROM {{ ref('calc_security_daily_momentum') }}
    WHERE trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
    GROUP BY trade_date, sector
),

final AS (
    SELECT
        b.trade_date,
        b.sector,
        b.stocks_traded,
        b.unchanged_stocks,
        b.advances,
        b.declines,
        b.up_volume,
        b.down_volume,

        t.pct_sector_over_sma20,
        t.pct_sector_over_sma50,
        t.pct_sector_over_sma200,
        t.sector_rsi,

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
            t.sector_rsi > 70, 'overbought',
            IFF(t.sector_rsi < 30, 'oversold', 'normal')
        ) AS sector_momentum,

        t.new_highs,
        t.new_lows,

        IFF(
            b.stocks_traded > 0,
            t.new_highs / b.stocks_traded,
            NULL
        ) AS record_high_pct

    FROM base_aggregates AS b
    LEFT JOIN technical_aggregates AS t
        ON t.trade_date = b.trade_date
       AND t.sector = b.sector
)


SELECT
    TO_NUMBER(TO_CHAR(trade_date, 'YYYYMMDD')) AS date_key,
    MD5(COALESCE(sector, 'Unknown')) AS sector_key,
    trade_date,
    stocks_traded,
    unchanged_stocks,
    advances,
    declines,
    up_volume,
    down_volume,
    pct_sector_over_sma20,
    pct_sector_over_sma50,
    pct_sector_over_sma200,
    sector_rsi,
    ad_percentage,
    ad_ratio,
    up_down_volume_ratio,
    sector_momentum,
    new_highs,
    new_lows,
    record_high_pct
FROM final
ORDER BY trade_date, sector_key
