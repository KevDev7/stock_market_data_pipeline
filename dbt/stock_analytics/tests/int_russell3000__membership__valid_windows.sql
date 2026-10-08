WITH windows AS (
    SELECT
        *,
        LAG(valid_to) OVER (
            PARTITION BY ticker ORDER BY valid_from
        ) AS previous_valid_to
    FROM {{ ref('int_russell3000__membership') }}
)
SELECT *
FROM windows
WHERE valid_from > valid_to
   OR valid_from <= previous_valid_to
{{ config(enabled=false) }}
