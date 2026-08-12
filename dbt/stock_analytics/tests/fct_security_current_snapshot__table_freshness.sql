-- Checks whether the current snapshot is fresh relative to the security-day fact.
WITH dates AS (
    SELECT
        (SELECT MAX(latest_trade_date) FROM {{ ref('fct_security_current_snapshot') }}) AS model_latest_date,
        (SELECT MAX(trade_date) FROM {{ ref('fct_security_daily_momentum') }}) AS upstream_latest_date
)

SELECT *
FROM dates
WHERE upstream_latest_date IS NOT NULL
  AND (
      model_latest_date IS NULL
      OR model_latest_date < upstream_latest_date
  )
