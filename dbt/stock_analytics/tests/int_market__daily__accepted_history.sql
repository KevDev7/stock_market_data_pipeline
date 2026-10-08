WITH ordered AS (
 SELECT *, LAG(close) OVER(PARTITION BY security_key ORDER BY trade_date) AS expected_previous,
 ROW_NUMBER() OVER(PARTITION BY security_key ORDER BY trade_date) AS expected_count
 FROM {{ ref('int_market__daily') }}
)
SELECT * FROM ordered
WHERE NOT EQUAL_NULL(yesterday_close,expected_previous)
 OR observation_count<>expected_count OR is_valid_record<>1 OR has_volume<>1
