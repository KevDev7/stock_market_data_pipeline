-- Date dimension for trading-day facts.
{{ config(materialized = 'table') }}

WITH dates AS (
    SELECT DISTINCT trade_date
    FROM {{ ref('prep_security_daily_momentum') }}
)

SELECT
    TO_NUMBER(TO_CHAR(trade_date, 'YYYYMMDD')) AS date_key,
    trade_date,
    YEAR(trade_date) AS year,
    QUARTER(trade_date) AS quarter,
    MONTH(trade_date) AS month,
    MONTHNAME(trade_date) AS month_name,
    DAYOFMONTH(trade_date) AS day_of_month,
    DAYOFWEEKISO(trade_date) AS day_of_week,
    DAYNAME(trade_date) AS day_name,
    WEEKISO(trade_date) AS week_of_year,
    IFF(trade_date = LAST_DAY(trade_date, 'month'), 1, 0) AS is_month_end,
    IFF(trade_date = LAST_DAY(trade_date, 'quarter'), 1, 0) AS is_quarter_end,
    IFF(trade_date = LAST_DAY(trade_date, 'year'), 1, 0) AS is_year_end,
    1 AS is_trading_day
FROM dates
ORDER BY trade_date
