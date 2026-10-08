-- Include warmup history when verifying the first visible reporting-day close.
SELECT f.security_key, f.trade_date, f.yesterday_close, p.yesterday_close AS expected_close
FROM {{ ref('fct_security_daily_momentum') }} f
JOIN {{ ref('int_market__daily') }} p
    ON f.security_key=p.security_key AND f.trade_date=p.trade_date
WHERE NOT EQUAL_NULL(f.yesterday_close,p.yesterday_close)
