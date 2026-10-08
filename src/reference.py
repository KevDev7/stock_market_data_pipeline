"""Massive reference extraction. No analytical transformations belong here."""

import datetime as dt
import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
import pandas_market_calendars as mcal

from src.config import API_BASE_URL, POLYGON_API_KEY


def reference_dates(start_date, end_date):
    """Initial/end observations plus the last exchange session of each quarter."""
    sessions = mcal.get_calendar('NYSE').schedule(start_date=start_date, end_date=end_date).index
    if sessions.empty:
        return []
    last_by_quarter = {}
    for session in sessions:
        day = session.date()
        last_by_quarter[(day.year, (day.month-1)//3)] = day
    return sorted({sessions[0].date(), sessions[-1].date(), *last_by_quarter.values()})


class MassiveReferenceClient:
    """Bearer authentication, complete pagination, bounded retries and durable cache.

    Cache responses by endpoint and query date so interrupted paid backfills do
    not repeat successful company requests. Never cache an incomplete catalog.
    """

    def __init__(self, cache_path, base_url=None, api_key=None, interval=0.05, session=None):
        self.base = (base_url or API_BASE_URL or 'https://api.massive.com').rstrip('/')
        self.host = urlparse(self.base).hostname
        self.session = session or requests.Session()
        self.session.headers['Authorization'] = 'Bearer ' + (api_key or POLYGON_API_KEY or '')
        self.interval, self.last_request = interval, 0.0
        self.lock = threading.RLock()
        self.rate_lock = threading.Lock()
        self.thread_local = threading.local()
        self.worker_sessions = []
        self.injected_session = session is not None
        Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
        self.cache = sqlite3.connect(str(cache_path), check_same_thread=False)
        self.cache.execute('CREATE TABLE IF NOT EXISTS responses (cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        self.cache.commit()

    def close(self):
        for session in self.worker_sessions:
            session.close()
        self.cache.close()
        self.session.close()

    def _safe_url(self, url):
        parsed = urlparse(url)
        if parsed.scheme != 'https' or parsed.hostname not in {self.host, 'api.massive.com', 'api.polygon.io'}:
            raise ValueError('Unexpected reference pagination host')
        query = [(k,v) for k,v in parse_qsl(parsed.query) if k.lower() != 'apikey']
        return urlunparse(parsed._replace(query=urlencode(query)))

    def get(self, path, params=None, allow_missing=False):
        url = self._safe_url(path if path.startswith('https://') else self.base + path)
        cache_key = hashlib.sha256((url + json.dumps(params or {}, sort_keys=True)).encode()).hexdigest()
        with self.lock:
            cached = self.cache.execute('SELECT payload FROM responses WHERE cache_key=?', (cache_key,)).fetchone()
        if cached:
            return json.loads(cached[0])
        for attempt in range(4):
            with self.rate_lock:
                time.sleep(max(0.0, self.interval-(time.monotonic()-self.last_request)))
                self.last_request = time.monotonic()
            try:
                if not self.injected_session and not hasattr(self.thread_local, 'session'):
                    self.thread_local.session = requests.Session()
                    self.thread_local.session.headers.update(self.session.headers)
                    with self.lock:
                        self.worker_sessions.append(self.thread_local.session)
                session = self.session if self.injected_session else self.thread_local.session
                response = session.get(url, params=params, timeout=30)
            except requests.RequestException:
                if attempt == 3:
                    raise RuntimeError('Reference network request failed; credentials suppressed') from None
                time.sleep(2**attempt)
                continue
            if response.status_code == 200:
                result = response.json()
                if result.get('status') not in {'OK','DELAYED',None}:
                    raise RuntimeError('Reference response did not report success')
                if result.get('next_url'):
                    result['next_url'] = self._safe_url(result['next_url'])
                with self.lock:
                    self.cache.execute('INSERT OR REPLACE INTO responses VALUES (?,?)',
                                       (cache_key, json.dumps(result, separators=(',',':'))))
                    self.cache.commit()
                return result
            if response.status_code == 404 and allow_missing:
                with self.lock:
                    self.cache.execute('INSERT OR REPLACE INTO responses VALUES (?,?)', (cache_key,'null'))
                    self.cache.commit()
                return None
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(min(45, 15*(attempt+1)) if response.status_code == 429 else 2**attempt)
                continue
            raise RuntimeError('Reference request HTTP ' + str(response.status_code))
        raise RuntimeError('Reference retries exhausted')

    def catalog(self, date):
        params = dict(market='stocks', locale='us', active='true', type='CS',
                      date=str(date), limit=1000, sort='ticker', order='asc')
        url, rows, seen_pages = '/v3/reference/tickers', [], set()
        while url:
            if url in seen_pages:
                raise ValueError('Reference pagination cycle')
            seen_pages.add(url)
            page = self.get(url, params)
            rows.extend(page.get('results', []))
            url, params = page.get('next_url'), None
        tickers = [r.get('ticker') for r in rows]
        if not rows or None in tickers or len(tickers) != len(set(tickers)):
            raise ValueError('Empty or nonunique complete ticker catalog')
        return rows

    def overview(self, ticker, date):
        response = self.get('/v3/reference/tickers/' + ticker, {'date':str(date)}, allow_missing=True)
        return response.get('results') if response else None


def landing_rows(records, date, source, run_id):
    """Operational envelope only; all provider attributes remain untouched."""
    ingested = dt.datetime.utcnow().isoformat()
    return [dict(API_DATE=str(date), SOURCE=source, RUN_ID=run_id,
                 RAW_PAYLOAD=json.dumps(r, separators=(',',':'),allow_nan=False), INGESTED_AT=ingested)
            for r in records]
