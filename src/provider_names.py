"""Canonical project names and replay compatibility for original archives."""

REFERENCE_PREFIX = 'raw/polygon/reference'
REFERENCE_STAGE = 'POLYGON_REFERENCE_STAGE'
LEGACY_REFERENCE_PREFIX = 'raw/massive/reference'
SOURCE_ALIASES = {
    'massive_ticker_catalog': 'polygon_ticker_catalog',
    'massive_ticker_overview': 'polygon_ticker_overview',
}


def canonical_source(value):
    return SOURCE_ALIASES.get(value, value)


def canonical_source_sql(expression):
    """SQL for internal COPY expressions; leave source payloads untouched."""
    cases = ' '.join(f"WHEN '{old}' THEN '{new}'" for old, new in SOURCE_ALIASES.items())
    return f'CASE {expression} {cases} ELSE {expression} END'


def polygon_archive_key(key):
    if not key.startswith(LEGACY_REFERENCE_PREFIX + '/'):
        raise ValueError('Not a legacy reference archive key')
    source, separator, suffix = key[len(LEGACY_REFERENCE_PREFIX) + 1:].partition('/')
    if source not in SOURCE_ALIASES or not separator or not suffix:
        raise ValueError('Unknown reference archive layout')
    return f'{REFERENCE_PREFIX}/{SOURCE_ALIASES[source]}/{suffix}'
