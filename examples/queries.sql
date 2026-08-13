-- Most recent golden crosses.
SELECT
    s.ticker,
    s.company,
    s.sector_name
FROM MARKET.MARTS.FCT_SECURITY_DAILY_MOMENTUM AS f
INNER JOIN MARKET.MARTS.DIM_SECURITY AS s
    ON s.security_key = f.security_key
WHERE f.trade_date = (
    SELECT MAX(trade_date)
    FROM MARKET.MARTS.FCT_SECURITY_DAILY_MOMENTUM
)
  AND f.golden_cross = 1;

-- Market breadth and sentiment over the last 30 trading days.
SELECT
    trade_date,
    ad_ratio,
    pct_market_over_sma50,
    market_rsi,
    CASE
        WHEN pct_market_over_sma50 > 0.8 THEN 'Strong Bullish'
        WHEN pct_market_over_sma50 < 0.2 THEN 'Strong Bearish'
        ELSE 'Neutral'
    END AS market_sentiment
FROM MARKET.MARTS.FCT_MARKET_DAILY_BREADTH
ORDER BY trade_date DESC
LIMIT 30;

-- Top performers by sector in the latest snapshot.
SELECT
    s.sector_name,
    s.ticker,
    f.latest_close,
    f.return_1m,
    f.performance_percentile
FROM MARKET.MARTS.FCT_SECURITY_CURRENT_SNAPSHOT AS f
INNER JOIN MARKET.MARTS.DIM_SECURITY AS s
    ON s.security_key = f.security_key
WHERE f.performance_percentile > 0.9
ORDER BY s.sector_name, f.return_1m DESC;
