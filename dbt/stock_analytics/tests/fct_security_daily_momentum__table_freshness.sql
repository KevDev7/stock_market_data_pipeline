-- Checks whether fct_security_daily_momentum is fresh relative to its upstream model.
WITH dates AS (
    SELECT
        (SELECT MAX(trade_date) FROM {{ ref('fct_security_daily_momentum') }}) AS model_latest_date,
        (SELECT MAX(trade_date) FROM {{ ref('int_russell3000__daily') }}) AS upstream_latest_date
)
SELECT *
FROM dates
WHERE upstream_latest_date IS NOT NULL
  AND (
      model_latest_date IS NULL
      OR model_latest_date < upstream_latest_date
  )
