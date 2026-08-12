-- Checks whether market breadth is fresh relative to the security-day fact.
WITH dates AS (
    SELECT
        (SELECT MAX(trade_date) FROM {{ ref('fct_market_daily_breadth') }}) AS model_latest_date,
        (SELECT MAX(trade_date) FROM {{ ref('fct_security_daily_momentum') }}) AS upstream_latest_date
)
SELECT *
FROM dates
WHERE upstream_latest_date IS NOT NULL
  AND (
      model_latest_date IS NULL
      OR model_latest_date < upstream_latest_date
  )
