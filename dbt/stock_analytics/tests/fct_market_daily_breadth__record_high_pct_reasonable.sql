-- Validate the defined proportion, without imposing a market-outcome threshold.
SELECT
    *
FROM {{ ref('fct_market_daily_breadth') }}
WHERE
    record_high_pct < 0 OR record_high_pct > 1
