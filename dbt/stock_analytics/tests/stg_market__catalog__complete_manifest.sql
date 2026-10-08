-- A complete archive must not lose catalog records during parsing/standardization.
WITH counts AS (
 SELECT trade_date,COUNT(*) AS parsed_rows FROM {{ ref('stg_market__catalog') }} GROUP BY 1
)
SELECT m.api_date,m.row_count,c.parsed_rows
FROM {{ source('raw_reference','REFERENCE_MANIFEST') }} m
LEFT JOIN counts c ON m.api_date=c.trade_date
WHERE m.source='massive_ticker_catalog' AND m.row_count<>COALESCE(c.parsed_rows,0)
