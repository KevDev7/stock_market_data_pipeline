-- Daily eligibility/identity comes from dated catalogs, not quarter-end membership.
{{ config(materialized='table') }}
WITH calendar AS (
    SELECT trade_date,LAG(trade_date) OVER(ORDER BY trade_date) AS previous_catalog_date
    FROM (SELECT DISTINCT trade_date FROM {{ ref('stg_market__catalog') }})
), catalog_base AS (
    SELECT c.*,
        COALESCE('SHARE:' || share_class_figi, 'COMPOSITE:' || composite_figi,
                 'CIK:' || cik || ':' || ticker,
                 'TICKER:' || COALESCE(exchange, 'UNKNOWN') || ':' || ticker) AS base_identity,
        COALESCE(cik, 'TICKER:' || ticker) AS issuer_identity,
        -- CQS: lowercase terminal w = when-issued; .WD = when-distributed.
        -- https://www.nasdaqtrader.com/Trader.aspx?id=CQSSymbolConvention
        -- Nasdaq may use a fifth-character V; use the explicit source name
        -- rather than guessing whether a final V belongs to the root symbol.
        -- https://www.nasdaq.com/glossary/v/v
        CASE WHEN RIGHT(ticker,1)='w' THEN 'when_issued'
             WHEN RIGHT(ticker,3)='.WD' THEN 'when_distributed'
             WHEN REGEXP_LIKE(company,'.*when[ -]issued.*','i') THEN 'when_issued'
             WHEN REGEXP_LIKE(company,'.*when[ -]distributed.*','i') THEN 'when_distributed'
             ELSE 'regular' END AS trading_line,
        cal.previous_catalog_date
    FROM {{ ref('stg_market__catalog') }} c JOIN calendar cal USING(trade_date)
    WHERE security_type = 'CS' AND locale = 'us' AND active
      AND trade_date BETWEEN TO_DATE('{{ var("warmup_start", "2022-12-29") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
), gaps AS (
    SELECT *, IFF(trading_line='regular',0,
        IFF(EQUAL_NULL(previous_catalog_date,
            LAG(trade_date) OVER(PARTITION BY base_identity,ticker ORDER BY trade_date)),0,1)) AS episode_start
    FROM catalog_base
), episodes AS (
    SELECT *, SUM(episode_start) OVER(PARTITION BY base_identity,ticker ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_number
    FROM gaps
), catalog AS (
    SELECT *, IFF(trading_line='regular',base_identity,
        base_identity || ':LINE:' || trading_line || ':' || ticker || ':' ||
        TO_CHAR(MIN(trade_date) OVER(PARTITION BY base_identity,ticker,episode_number),'YYYYMMDD')) AS security_identity
    FROM episodes
), observations AS (
    SELECT *, COALESCE(cik, 'TICKER:' || ticker) AS issuer_identity
    FROM {{ ref('stg_market__issuer_observations') }}
), joined AS (
    SELECT c.*, o.sic_code, o.sic_description, o.location,
           o.observation_date AS classification_observed_on
    FROM catalog c
    LEFT JOIN observations o ON c.issuer_identity = o.issuer_identity
                            AND o.observation_date <= c.trade_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.ticker, c.trade_date
        ORDER BY o.observation_date DESC NULLS LAST, o.ingested_at DESC NULLS LAST
    ) = 1
)
SELECT
    MD5(security_identity) AS security_key,
    security_identity, issuer_identity, trade_date, ticker, company, cik,
    share_class_figi, composite_figi, sic_code, sic_description,
    {{ sic_industry_group('sic_code') }} AS sector,
    IFF(trading_line='regular','Common stock (provider classification)',
        'Common stock (' || trading_line || ', provider CS)') AS asset_class,
    trading_line,
    location, exchange, currency, currency AS market_currency,
    classification_observed_on,
    IFF(share_class_figi IS NULL AND composite_figi IS NULL, 'issuer_ticker_fallback', 'figi') AS identity_quality,
    IFF(REGEXP_LIKE(company, '.*(preferred|notes due|debentures).*', 'i'), 1, 0) AS reference_type_review_required
FROM joined
