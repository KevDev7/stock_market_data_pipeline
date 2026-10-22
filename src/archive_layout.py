"""Dataset-based S3 folders and their scoped Snowflake stages."""

PRICE_PREFIX = 'daily-stock-prices'
REFERENCE_ARCHIVES = {
    'polygon_ticker_catalog': ('ticker-catalog', 'POLYGON_TICKER_CATALOG_STAGE'),
    'polygon_ticker_overview': ('company-overview', 'POLYGON_COMPANY_OVERVIEW_STAGE'),
}
PREFIX_MOVES = {
    'raw/polygon/grouped-daily/': PRICE_PREFIX + '/',
    'raw/polygon/reference/polygon_ticker_catalog/': 'ticker-catalog/',
    'raw/polygon/reference/polygon_ticker_overview/': 'company-overview/',
}


def simplified_archive_key(key):
    """Map only recognized archive prefixes; retain the complete date/run suffix."""
    for old, new in PREFIX_MOVES.items():
        if key.startswith(old) and key[len(old):]:
            return new + key[len(old):]
    raise ValueError('Not a recognized source archive key: ' + key)


def normalized_key_sql(expression):
    cases = ' '.join(
        f"WHEN STARTSWITH({expression},'{old}') THEN '{new}' || "
        f"SUBSTR({expression},{len(old) + 1})"
        for old, new in PREFIX_MOVES.items())
    return f'CASE {cases} ELSE {expression} END'
