-- Builds the current-day securities dimension with returns, sector metrics, volatility, and technical signals.
{% macro calculate_return(lookback_days) %}
    MAX(
        CASE 
            WHEN days_back = {{ lookback_days }} THEN 
                (close - yesterday_close) / NULLIF(yesterday_close, 0)
        END
    ) OVER (PARTITION BY ticker) AS return_{{ lookback_days }}d
{% endmacro %}

{{ config(
    materialized='table'
) }}

WITH latest_snapshot AS (
    SELECT
        ticker,
        company,
        sector, 
        trade_date AS latest_trade_date,
        volume AS latest_volume,
        open AS latest_open,
        close AS latest_close,
        yesterday_close AS latest_prev_close,
        high AS latest_high,
        low AS latest_low,
        sma_20 AS latest_sma20,
        sma_50 AS latest_sma50,
        sma_200 AS latest_sma200,
        rsi AS latest_rsi,
        high_52week AS latest_52week_high,
        low_52week AS latest_52week_low,
        (close - yesterday_close) AS price_change_1d,
        (close - yesterday_close) / NULLIF(yesterday_close, 0) AS return_1d
    FROM {{ ref('fct_trading_momentum') }}
    WHERE trade_date = (SELECT MAX(trade_date) FROM {{ ref('fct_trading_momentum') }})
),

returns_lookback AS (
    SELECT 
        ticker,
        {{ calculate_return(5) }} AS return_1w,
        {{ calculate_return(21) }} AS return_1m,
        {{ calculate_return(63) }} AS return_3m,
        {{ calculate_return(252) }} AS return_ytd
    FROM {{ ref('fct_trading_momentum') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) = 1
),

numbered_dates AS (
    SELECT 
        ticker,
        sector,
        trade_date,
        close, 
        yesterday_close,
        volume,
        ROW_NUMBER() OVER (
            PARTITION BY ticker
            ORDER BY trade_date DESC
        ) AS days_back
    FROM {{ ref('fct_trading_momentum') }}
    WHERE trade_date >= DATEADD(day, -33, (SELECT MAX(trade_date) FROM {{ ref('fct_trading_momentum') }}))
),

sector_lookback AS (
    SELECT 
        ticker,
        sector,
        trade_date,
        {{ calculate_return(21) }} AS return_1m
    FROM numbered_dates
),

sector_metrics AS (
    SELECT
        ticker,
        AVG(return_1m) OVER (PARTITION BY sector) AS sector_return_1m,
        PERCENT_RANK() OVER (ORDER BY return_1m) AS performance_percentile
    FROM sector_lookback
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) = 1
),

volatility_metrics AS (
    SELECT
        ticker,
        STDDEV(LN(close / NULLIF(yesterday_close, 0))) * SQRT(252) AS volatility_20d,
        AVG(volume) AS avg_volume_20d,
        COUNT(*) AS trading_days
    FROM numbered_dates
    WHERE days_back <= 20
    GROUP BY ticker
),

trading_days_count AS (
    SELECT
        ticker, 
        COUNT(DISTINCT trade_date) AS total_trading_days
    FROM {{ ref('fct_trading_momentum') }}
    GROUP BY ticker
),

signal_flags AS (
    SELECT
        ticker,
        CASE WHEN latest_sma50 > latest_sma200 THEN 1 ELSE 0 END AS has_golden_cross_active,
        CASE WHEN latest_close > latest_sma20 THEN 1 ELSE 0 END AS over_sma20,
        CASE WHEN latest_close > latest_sma50 THEN 1 ELSE 0 END AS over_sma50,
        CASE WHEN latest_close > latest_sma200 THEN 1 ELSE 0 END AS over_sma200
    FROM latest_snapshot
),

final AS (
    SELECT 
        l.*,
        r.return_1w,
        r.return_1m,
        r.return_3m,
        r.return_ytd,
        t.total_trading_days,
        s.has_golden_cross_active,
        s.over_sma20,
        s.over_sma50,
        s.over_sma200,
        v.volatility_20d,
        v.avg_volume_20d,
        sm.sector_return_1m,
        sm.performance_percentile,
        CASE
            WHEN latest_52week_high IS NOT NULL THEN 
                (latest_52week_high - latest_close) / latest_52week_high
        END AS pct_distance_from_52week_high,
        CASE 
            WHEN latest_52week_low IS NOT NULL THEN 
                (latest_close - latest_52week_low) / latest_52week_low
        END AS pct_distance_from_52week_low
    FROM latest_snapshot AS l
    LEFT JOIN returns_lookback AS r ON l.ticker = r.ticker
    LEFT JOIN trading_days_count AS t ON l.ticker = t.ticker
    LEFT JOIN volatility_metrics AS v ON l.ticker = v.ticker
    LEFT JOIN signal_flags AS s ON l.ticker = s.ticker
    LEFT JOIN sector_metrics AS sm ON l.ticker = sm.ticker
)

SELECT * FROM final

