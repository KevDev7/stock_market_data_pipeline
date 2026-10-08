WITH history AS (
    SELECT
        *,
        LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date) AS expected_previous_close,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date) AS expected_observation_count
    FROM {{ ref('int_russell3000__daily') }}
)
SELECT *
FROM history
WHERE is_valid_record <> 1
   OR has_volume <> 1
   OR observation_count <> expected_observation_count
   OR NOT EQUAL_NULL(yesterday_close, expected_previous_close)
   OR is_first_observation <> IFF(expected_observation_count = 1, 1, 0)
{{ config(enabled=false) }}
