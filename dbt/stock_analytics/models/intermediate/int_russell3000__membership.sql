-- One authoritative membership window per constituent snapshot.
{{ config(materialized = 'table') }}

WITH snapshot_dates AS (
    SELECT DISTINCT snapshot_date
    FROM {{ ref('stg_russell3000__constituents') }}
),

snapshot_windows AS (
    SELECT
        snapshot_date,
        -- Preserve the portfolio's historical approximation explicitly.
        -- The earliest snapshot is projected back to the configured history start.
        CASE
            WHEN snapshot_date = MIN(snapshot_date) OVER ()
            THEN LEAST(snapshot_date, TO_DATE('{{ var("membership_history_start", "2023-01-01") }}'))
            ELSE snapshot_date
        END AS valid_from,
        CAST(COALESCE(
            DATEADD(day, -1, LEAD(snapshot_date) OVER (ORDER BY snapshot_date)),
            TO_DATE('3000-01-01')
        ) AS DATE) AS valid_to
    FROM snapshot_dates
)

SELECT
    c.*,
    w.valid_from,
    w.valid_to
FROM {{ ref('stg_russell3000__constituents') }} AS c
INNER JOIN snapshot_windows AS w
    ON c.snapshot_date = w.snapshot_date
