-- Snapshot of Russell 3000 constituents to track changes over time.
{% snapshot russell3000_constituents_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='ticker',
        strategy='check',
        check_cols=['company','sector','market_weight'],
        invalidate_hard_deletes=True
    )
}}

with latest as (
  select
    ticker,
    company,
    sector,
    market_weight,
    row_number() over (partition by ticker order by valid_from desc) as rn
  from {{ ref('stg_russell3000__constituents') }}
)
select
  ticker,
  company,
  sector,
  market_weight
from latest
where rn = 1

{% endsnapshot %}
