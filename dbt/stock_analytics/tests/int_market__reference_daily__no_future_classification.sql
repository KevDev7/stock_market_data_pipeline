SELECT * FROM {{ ref('int_market__reference_daily') }}
WHERE classification_observed_on>trade_date
