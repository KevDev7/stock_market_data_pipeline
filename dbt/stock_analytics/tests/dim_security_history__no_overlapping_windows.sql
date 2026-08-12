-- Flags Type 2 security history rows with overlapping validity windows.
WITH ordered_history AS (
    SELECT
        ticker,
        valid_from,
        valid_to,
        LAG(valid_to) OVER (
            PARTITION BY ticker
            ORDER BY valid_from, valid_to
        ) AS previous_valid_to
    FROM {{ ref('dim_security_history') }}
)

SELECT *
FROM ordered_history
WHERE previous_valid_to IS NOT NULL
  AND valid_from <= previous_valid_to
