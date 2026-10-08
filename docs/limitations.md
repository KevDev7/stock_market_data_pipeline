# Limitations

- Source ingestion is paused, so the hosted dashboard represents a historical
  portfolio snapshot rather than a real-time service.
- This is provider-classified U.S.-listed common stock, not Russell membership.
  Source classification anomalies are flagged for review, not silently discarded.
- Industry labels use SIC major groups, not GICS sectors. Unknown is retained;
  index weights are NULL because this source supplies no index membership weights.
  Approximately 15.4% of priced reporting observations have no provider SIC code;
  15.3% use fallback identity keys rather than FIGI.
- Quarterly metadata approximates change dates and is not strict as-known-then:
  overview dates follow reporting periods, not historical filing availability.
- Missing FIGI uses an issuer/ticker fallback. Identifier changes can split
  series; complete corporate-action identity continuity is not claimed.
- Prices are split-adjusted, not total-return adjusted. The migration refreshes
  retained dates to match newly fetched warmup data. Future provider adjustments
  require another coordinated history refresh, not just appending a few dates.
- This is an educational portfolio deployment, not a production SLA-backed
  service; Snowflake and market-data usage can incur costs when resumed.
