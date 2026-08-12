-- Checks for sector breadth rows where component counts do not reconcile.
SELECT
    *
FROM {{ ref('fct_sector_daily_breadth') }}
WHERE
    (advances + declines + unchanged_stocks) != stocks_traded
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
