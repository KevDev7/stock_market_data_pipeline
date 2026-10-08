"""Execute the real model SQL on local fixtures, without warehouse credentials.

Jinja renders the project's macros; SQLGlot translates Snowflake syntax for
DuckDB. Small dialect adapters preserve Snowflake scalar-JSON and date semantics.
This exercises model behavior, but does not replace a Snowflake dbt build.
"""

import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

import duckdb
from jinja2 import Environment, StrictUndefined
import sqlglot
from sqlglot import exp


PROJECT = Path(__file__).resolve().parents[1] / "dbt" / "stock_analytics"
SEEDS = (
    "russell3000_2024_1231",
    "russell3000_2025_0630",
    "russell3000_2025_0829",
    "russell3000_2025_0916",
)
SOURCE_COLUMNS = (
    "Ticker", "Name", "Sector", "Asset_Class", "Location", "Exchange",
    "Currency", "Market_Currency", "Market_Value", "Weight",
)
INCREMENTAL_MODELS = (
    "int_market__daily",
    "calc_security_daily_momentum",
    "fct_security_daily_momentum",
)


def duckdb_sql(sql):
    def adapt(node):
        if isinstance(node, exp.Cast) and isinstance(node.this, exp.JSONExtract):
            # Snowflake VARIANT::STRING unwraps strings; DuckDB JSON::TEXT does not.
            if node.args["to"].this == exp.DataType.Type.TEXT:
                return exp.JSONExtractScalar(
                    this=node.this.this.copy(),
                    expression=node.this.expression.copy(),
                )
        if isinstance(node, exp.Anonymous):
            name = node.name.upper()
            if name == 'WEEKISO':
                return exp.Anonymous(this='week', expressions=node.expressions)
            if name in ("TRY_TO_DOUBLE", "TRY_TO_NUMBER"):
                dtype = "DOUBLE" if name == "TRY_TO_DOUBLE" else "BIGINT"
                return exp.TryCast(
                    this=node.expressions[0].copy(), to=exp.DataType.build(dtype)
                )
            if name == "EQUAL_NULL":
                return exp.NullSafeEQ(
                    this=node.expressions[0].copy(), expression=node.expressions[1].copy()
                )
        if isinstance(node, exp.LastDay) and node.args.get('unit'):
            unit = node.args['unit'].name.lower()
            if unit in ('quarter', 'year'):
                months = 2 if unit == 'quarter' else 11
                expression = exp.Anonymous(this='date_trunc', expressions=[
                    exp.Literal.string(unit), node.this.copy()])
                expression = exp.Add(this=expression, expression=exp.Interval(
                    this=exp.Literal.string(str(months)), unit=exp.Var(this='MONTH')))
                return exp.LastDay(this=expression)
        if isinstance(node, exp.ToChar) and node.args.get("format"):
            formats = {"YYYYMMDD": "%Y%m%d"}
            pattern = node.args["format"].this
            if pattern not in formats:
                raise ValueError(f"Add an explicit date-format adapter for {pattern}")
            return exp.TimeToStr(
                this=node.this.copy(), format=exp.Literal.string(formats[pattern])
            )
        if isinstance(node, exp.ToNumber):
            return exp.Cast(this=node.this.copy().transform(adapt), to=exp.DataType.build("BIGINT"))
        return node

    return ";\n".join(
        tree.transform(adapt).sql(dialect="duckdb")
        for tree in sqlglot.parse(sql, read="snowflake") if tree is not None
    )


class LayerModelTest(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect(":memory:")
        self.variables = {'warmup_start':'2022-01-01', 'analysis_start':'2023-01-01', 'analysis_end':'2030-12-31'}
        self.environment = Environment(
            undefined=StrictUndefined, extensions=["jinja2.ext.do"]
        )
        self.macro_source = "\n".join(
            path.read_text() for path in sorted((PROJECT / "macros").glob("*.sql"))
        )
        self.db.execute("""
            CREATE TABLE raw_daily_stocks (
                API_DATE DATE, INGESTED_AT TIMESTAMP, RUN_ID VARCHAR, RAW_PAYLOAD VARCHAR
            )
        """)
        self.db.execute("""CREATE TABLE raw_reference (
            API_DATE DATE, SOURCE VARCHAR, INGESTED_AT TIMESTAMP, RAW_PAYLOAD VARCHAR,
            RUN_ID VARCHAR DEFAULT 'reference-test'
        )""")
        for seed in SEEDS:
            fields = ", ".join(f"{name} VARCHAR" for name in SOURCE_COLUMNS)
            self.db.execute(f"CREATE TABLE {seed} ({fields})")
            self.add_constituent(seed, "AAA")

    def tearDown(self):
        self.db.close()

    def context(self, model, incremental=False):
        return {
            "config": lambda **kwargs: "",
            "ref": lambda name: name,
            "source": lambda source, table: "raw_reference" if source == "raw_reference" else "raw_daily_stocks",
            "this": model,
            "is_incremental": lambda: incremental,
            "var": lambda name, default=None: self.variables.get(name, default),
            "return": lambda value: value,
            "get_quoted_csv": lambda columns: ", ".join(f'"{c}"' for c in columns),
            "snowflake_dml_explicit_transaction": lambda dml: f"BEGIN;\n{dml};\nCOMMIT;",
        }

    def render(self, model, incremental=False, target=None):
        matches = list((PROJECT / "models").glob(f"**/{model}.sql"))
        self.assertEqual(len(matches), 1, model)
        template = self.environment.from_string(self.macro_source + matches[0].read_text())
        return template.render(**self.context(target or model, incremental))

    def build(self, model, incremental=False, target=None):
        target = target or model
        query = duckdb_sql(self.render(model, incremental, target))
        if not incremental:
            self.db.execute(f"CREATE OR REPLACE TABLE {target} AS {query}")
            return
        # Match the Snowflake adapter: freeze query results before replacing rows.
        incoming = f"incoming_{target}"
        self.db.execute(f"CREATE OR REPLACE TEMP TABLE {incoming} AS {query}")
        self.db.execute(self.replacement_sql(target, incoming))

    def replacement_sql(self, target, incoming):
        columns = [SimpleNamespace(name=row[0]) for row in self.db.execute(
            f"DESCRIBE {target}"
        ).fetchall()]
        context = self.context(target, incremental=True)
        context["arguments"] = {
            "target_relation": target, "temp_relation": incoming, "dest_columns": columns
        }
        dml = self.environment.from_string(
            self.macro_source + "{{ get_incremental_replace_recent_sql(arguments) }}"
        ).render(**context)
        return duckdb_sql(dml)

    def add_constituent(self, seed, ticker, company="Company", value="100", sector="Technology"):
        self.db.execute(
            f"INSERT INTO {seed} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [ticker, company, sector, "Equity", "US", "NASDAQ", "USD", "USD", value, "1"],
        )

    def add_price(self, date, close, ticker="AAA", volume=100, high=None, ingested_at=None):
        self.add_reference(date, {"ticker":ticker,"name":"Company","type":"CS","locale":"us",
                                  "active":True,"primary_exchange":"XNAS","currency_name":"usd",
                                  "cik":"CIK-"+ticker, "share_class_figi":"FIGI-"+ticker})
        payload = {"T": ticker, "v": volume, "vw": close, "o": close, "c": close,
                   "h": close + 1 if high is None else high, "l": close - 1, "n": 10}
        self.db.execute("INSERT INTO raw_daily_stocks VALUES (?, ?, ?, ?)",
                        [date, ingested_at or dt.datetime(2026, 1, 1), "run-1", json.dumps(payload)])

    def add_reference(self, date, payload, source="massive_ticker_catalog"):
        self.db.execute("INSERT INTO raw_reference (API_DATE,SOURCE,INGESTED_AT,RAW_PAYLOAD) VALUES (?, ?, ?, ?)",
                        [date, source, dt.datetime(2026, 1, 1), json.dumps(payload)])

    def build_inputs(self):
        self.build("stg_daily_stocks")
        self.build("stg_market__catalog")
        self.build("stg_market__issuer_observations")
        self.build("int_market__reference_daily")

    def build_history(self):
        self.build("int_market__security_history")
        self.build("dim_security_history")

    def build_chain(self, incremental=False):
        for model in INCREMENTAL_MODELS:
            self.build(model, incremental)

    def assert_incremental_matches_full(self):
        for model in INCREMENTAL_MODELS:
            self.build(model, target=f"expected_{model}")
            columns = [row[0] for row in self.db.execute(f"DESCRIBE {model}").fetchall()]
            ordering = "ticker, trade_date" if "ticker" in columns else "security_key, date_key"
            actual = self.db.execute(f"SELECT * FROM {model} ORDER BY {ordering}").fetchall()
            expected = self.db.execute(f"SELECT * FROM expected_{model} ORDER BY {ordering}").fetchall()
            self.assertEqual(actual, expected, model)

    def assert_sql_test_passes(self, name):
        template = self.environment.from_string(
            self.macro_source + (PROJECT / "tests" / f"{name}.sql").read_text()
        )
        query = template.render(**self.context("unused"))
        self.assertEqual(self.db.execute(duckdb_sql(query)).fetchall(), [], name)

    def test_source_duplicates_choose_latest_delivery(self):
        date = dt.date(2025, 9, 20)
        self.add_price(date, 10)
        self.add_price(date, 12, ingested_at=dt.datetime(2026, 1, 2))
        self.add_price(date, 12, ingested_at=dt.datetime(2026, 1, 2))
        self.build("stg_daily_stocks")
        self.assertEqual(self.db.execute("SELECT ticker, close FROM stg_daily_stocks").fetchall(),
                         [("AAA", 12.0)])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM raw_daily_stocks").fetchone()[0], 3)

    def test_all_published_marts_build_from_one_shared_calculation(self):
        start = dt.date(2023, 7, 1)
        self.variables['analysis_start'] = '2024-01-01'
        for i in range(300):
            day = start + dt.timedelta(days=i)
            self.add_price(day, 100 + i % 17, ticker='AAA')
            self.add_price(day, 80 + i % 11, ticker='BBB')
        self.build_inputs()
        self.build_chain()
        self.build_history()
        for model in ('dim_date', 'dim_sector', 'dim_security',
                      'fct_market_daily_breadth', 'fct_sector_daily_breadth',
                      'fct_security_current_snapshot'):
            self.build(model)
        self.assertEqual(self.db.execute(
            'SELECT COUNT(*) FROM dim_security').fetchone()[0], 2)
        self.assertEqual(self.db.execute(
            'SELECT COUNT(*) FROM fct_security_current_snapshot').fetchone()[0], 2)
        self.assertEqual(self.db.execute('''SELECT COUNT(*) FROM fct_security_daily_momentum f
            LEFT JOIN dim_date d ON f.date_key=d.date_key
            LEFT JOIN dim_security s ON f.security_key=s.security_key
            LEFT JOIN dim_sector i ON f.sector_key=i.sector_key
            WHERE d.date_key IS NULL OR s.security_key IS NULL OR i.sector_key IS NULL
        ''').fetchone()[0], 0)

    def test_four_layer_responsibility_dependencies(self):
        import re
        self.assertEqual(list((PROJECT / 'models/mart_staging').glob('*.sql')), [])
        shared = PROJECT / 'models/marts/_shared/calc_security_daily_momentum.sql'
        self.assertTrue(shared.is_file())
        for path in (PROJECT / 'models/marts').glob('**/*.sql'):
            query = path.read_text()
            self.assertNotIn("source(", query)
            self.assertFalse(any(ref.startswith('stg_') for ref in
                                 re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", query)),
                             path.name)
        for path in (PROJECT / 'models/staging').glob('stg_market*.sql'):
            self.assertNotIn("ref(", path.read_text())

    def test_catalog_duplicates_do_not_duplicate_price_or_history(self):
        day = dt.date(2024,1,2)
        self.add_price(day,10)
        self.add_price(day,12,ingested_at=dt.datetime(2026,1,2))
        self.build_inputs()
        self.build("int_market__daily")
        self.build_history()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM stg_market__catalog").fetchone()[0],1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM dim_security_history").fetchone()[0],1)
        self.assertEqual(self.db.execute("SELECT close FROM int_market__daily").fetchone()[0],12)

    def test_daily_eligibility_handles_absence_without_waiting_for_quarter(self):
        start=dt.date(2024,1,2)
        for i in range(3):
            self.add_price(start+dt.timedelta(days=i),10+i)
        self.db.execute("DELETE FROM raw_reference WHERE API_DATE=?",[start+dt.timedelta(days=1)])
        self.build_inputs()
        self.build("int_market__daily")
        self.build_history()
        self.assertEqual(self.db.execute("SELECT close FROM int_market__daily ORDER BY trade_date").fetchall(),
                         [(10.0,),(12.0,)])
        self.assertEqual(self.db.execute("SELECT yesterday_close,observation_count FROM int_market__daily ORDER BY trade_date DESC LIMIT 1").fetchone(),
                         (10.0,2))
        self.assert_sql_test_passes("dim_security_history__no_overlapping_windows")

    def test_invalid_prices_and_zero_volume_do_not_enter_price_history(self):
        first = dt.date(2025, 9, 20)
        self.add_price(first, 10)
        self.add_price(first + dt.timedelta(days=1), 9999, high=100)
        self.add_price(first + dt.timedelta(days=2), 30, volume=0)
        self.add_price(first + dt.timedelta(days=3), 12)
        self.build_inputs()
        self.build("int_market__daily")
        self.assertEqual(self.db.execute("""
            SELECT close, yesterday_close, observation_count, is_first_observation
            FROM int_market__daily ORDER BY trade_date
        """).fetchall(), [(10.0, None, 1, 1), (12.0, 10.0, 2, 0)])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM stg_daily_stocks").fetchone()[0], 4)

    def test_industry_change_applies_only_on_or_after_observation(self):
        start=dt.date(2025,8,28)
        for i in range(2):
            self.add_price(start+dt.timedelta(days=i),10)
        self.add_reference(start,{"ticker":"AAA","cik":"CIK-AAA","sic_code":"3571"},
                           "massive_ticker_overview")
        self.add_reference(start+dt.timedelta(days=1),{"ticker":"AAA","cik":"CIK-AAA","sic_code":"6021"},
                           "massive_ticker_overview")
        self.build_inputs()
        self.build("int_market__daily")
        self.build_history()
        rows=self.db.execute("SELECT sector FROM int_market__daily ORDER BY trade_date").fetchall()
        self.assertTrue(rows[0][0].startswith("SIC 35"))
        self.assertTrue(rows[1][0].startswith("SIC 60"))
        self.assertEqual(self.db.execute("""SELECT COUNT(*) FROM int_market__daily f
            JOIN dim_security_history h ON f.security_key=h.security_key
            AND f.trade_date BETWEEN h.valid_from AND h.valid_to
            WHERE f.sector<>h.sector_name""").fetchone()[0],0)
        self.assert_sql_test_passes("int_market__reference_daily__no_future_classification")
        self.assert_sql_test_passes("dim_security_history__no_overlapping_windows")

    def test_ticker_reuse_does_not_share_previous_close_or_history(self):
        first=dt.date(2025,1,2)
        self.add_price(first,10)
        self.add_price(first+dt.timedelta(days=1),100)
        self.db.execute("DELETE FROM raw_reference WHERE API_DATE=?",[first+dt.timedelta(days=1)])
        self.add_reference(first+dt.timedelta(days=1),{"ticker":"AAA","name":"New issuer",
            "type":"CS","locale":"us","active":True,"cik":"OTHER","share_class_figi":"OTHER"})
        self.build_inputs()
        self.build("int_market__daily")
        self.build_history()
        self.assertEqual(self.db.execute("SELECT COUNT(DISTINCT security_key) FROM int_market__daily").fetchone()[0],2)
        self.assertEqual(self.db.execute("SELECT yesterday_close FROM int_market__daily ORDER BY trade_date").fetchall(),
                         [(None,),(None,)])
        self.assert_sql_test_passes("dim_security_history__no_overlapping_windows")

    def test_temporary_trading_lines_do_not_merge_with_regular_figi(self):
        first=dt.date(2024,1,2)
        for i in range(4):
            day=first+dt.timedelta(days=i)
            self.add_price(day,100+i,ticker='AAA')
            if i!=2:
                self.add_price(day,20+i,ticker='AAAw')
                self.db.execute("DELETE FROM raw_reference WHERE API_DATE=? AND RAW_PAYLOAD LIKE '%AAAw%'",[day])
                self.add_reference(day,{'ticker':'AAAw','name':'Company','type':'CS','locale':'us',
                    'active':True,'cik':'CIK-AAA','share_class_figi':'FIGI-AAA'})
        self.build_inputs()
        self.build('int_market__daily')
        self.build_history()
        # Regular shares remain one identity. Temporary contracts split after
        # disappearance from a complete daily catalog, even with the same FIGI.
        self.assertEqual(self.db.execute("SELECT ticker,COUNT(DISTINCT security_key) FROM int_market__daily GROUP BY ticker ORDER BY ticker").fetchall(),
                         [('AAA',1),('AAAw',2)])
        self.assertEqual(self.db.execute("SELECT yesterday_close FROM int_market__daily WHERE ticker='AAA' ORDER BY trade_date").fetchall(),
                         [(None,),(100.0,),(101.0,),(102.0,)])
        self.assert_sql_test_passes('dim_security_history__no_overlapping_windows')

    def test_explicit_nasdaq_when_issued_name_separates_shared_figi(self):
        first=dt.date(2025,3,25)
        for i in range(2):
            day=first+dt.timedelta(days=i)
            self.add_price(day,100+i,ticker='ANGI')
            self.add_price(day,20+i,ticker='ANGIV')
            self.db.execute("DELETE FROM raw_reference WHERE API_DATE=?",[day])
            for ticker,name in [('ANGI','Angi Inc. Class A Common Stock'),
                                ('ANGIV','Angi Inc. Class A Common Stock When Issued')]:
                self.add_reference(day,{'ticker':ticker,'name':name,'type':'CS','locale':'us',
                    'active':True,'cik':'ANGI-CIK','share_class_figi':'ANGI-FIGI','primary_exchange':'XNAS'})
        self.add_reference(first,{'ticker':'ROOTV','name':'Root Symbol Company','type':'CS',
            'locale':'us','active':True,'share_class_figi':'ROOT-FIGI','primary_exchange':'XNAS'})
        self.build_inputs()
        self.build('int_market__daily')
        self.build_history()
        self.assertEqual(self.db.execute("SELECT trading_line FROM int_market__reference_daily WHERE ticker='ROOTV'").fetchone()[0],'regular')
        self.assertEqual(self.db.execute('SELECT COUNT(DISTINCT security_key) FROM int_market__daily').fetchone()[0],2)
        self.assertEqual(self.db.execute("SELECT ticker,yesterday_close FROM int_market__daily WHERE trade_date=? ORDER BY ticker",[first+dt.timedelta(days=1)]).fetchall(),
                         [('ANGI',100.0),('ANGIV',20.0)])
        self.assert_sql_test_passes('dim_security_history__no_overlapping_windows')

    def test_first_reporting_day_uses_warmup_previous_close(self):
        self.variables["analysis_start"]="2024-01-01"
        self.add_price(dt.date(2023,12,29),10)
        self.add_price(dt.date(2024,1,2),12)
        self.build_inputs()
        self.build_chain()
        self.assertEqual(self.db.execute("SELECT trade_date,yesterday_close FROM fct_security_daily_momentum").fetchall(),
                         [(dt.date(2024,1,2),10.0)])
        self.assert_sql_test_passes("fct_security_daily_momentum__yesterday_close_equal_prev_date_close")

    def test_fourteen_period_rsi_requires_fourteen_price_changes(self):
        first=dt.date(2024,1,2)
        for i in range(15):
            self.add_price(first+dt.timedelta(days=i),100+i)
        self.build_inputs()
        self.build_chain()
        rows=self.db.execute("SELECT rsi,avg_gain_14 FROM calc_security_daily_momentum ORDER BY trade_date").fetchall()
        self.assertTrue(all(row==(None,None) for row in rows[:14]))
        self.assertEqual(rows[14],(100.0,1.0))

    def test_nonnegative_gains_and_tiny_positive_relative_volume(self):
        first=dt.date(2024,1,2)
        for i in range(20):
            self.add_price(first+dt.timedelta(days=i),100-i,
                           volume=1 if i==19 else 1_000_000_000)
        self.build_inputs()
        self.build_chain()
        gain,loss,ratio=self.db.execute('SELECT avg_gain_14,avg_loss_14,rel_vol FROM calc_security_daily_momentum ORDER BY trade_date DESC LIMIT 1').fetchone()
        self.assertEqual(gain,0)
        self.assertEqual(loss,1)
        self.assertGreater(ratio,0)
        self.assertLess(ratio,0.000001)

    def test_touching_average_is_not_a_crossover_but_a_real_move_is(self):
        first=dt.date(2024,1,2)
        for i in range(260):
            self.add_price(first+dt.timedelta(days=i),2.47)
        self.add_price(first+dt.timedelta(days=260),2.50)
        self.build_inputs()
        self.build_chain()
        self.assertEqual(self.db.execute('''SELECT COUNT(*) FROM calc_security_daily_momentum
            WHERE trade_date < ? AND (bullish_crossover<>0 OR golden_cross<>0 OR death_cross<>0)''',
            [first+dt.timedelta(days=260)]).fetchone()[0],0)
        self.assertEqual(self.db.execute('''SELECT bullish_crossover FROM calc_security_daily_momentum
            ORDER BY trade_date DESC LIMIT 1''').fetchone()[0],1)
        self.build_chain(incremental=True)
        self.assert_incremental_matches_full()

    def test_incremental_corrections_removals_new_tickers_and_retries_match_full_builds(self):
        first = dt.date(2025, 10, 1)
        self.add_constituent(SEEDS[-1], "BBB")
        for i in range(30):
            self.add_price(first + dt.timedelta(days=i), 10 + i)
        self.build_inputs()
        self.build_chain()
        corrected = first + dt.timedelta(days=28)
        self.db.execute("DELETE FROM raw_daily_stocks WHERE API_DATE = ?", [corrected])
        self.add_price(corrected, 9999, high=100)
        self.add_price(first + dt.timedelta(days=30), 42)
        self.add_price(first + dt.timedelta(days=30), 20, ticker="BBB")
        self.build_inputs()
        self.build_chain(incremental=True)
        self.assert_incremental_matches_full()
        self.assert_sql_test_passes("int_market__daily__accepted_history")
        self.assert_sql_test_passes("int_market__reference_daily__no_future_classification")
        self.assert_sql_test_passes("fct_security_daily_momentum__yesterday_close_equal_prev_date_close")
        self.assertEqual(self.db.execute("""
            SELECT COUNT(*) FROM fct_security_daily_momentum WHERE trade_date = ?
        """, [corrected]).fetchone()[0], 0)
        self.build_chain(incremental=True)
        self.assert_incremental_matches_full()

    def test_sparse_history_has_enough_warmup_for_full_incremental_equivalence(self):
        first = dt.date(2023, 1, 1)
        for i in range(260):
            self.add_price(first + dt.timedelta(days=i * 7), 100 + i)
        self.build_inputs()
        self.build_chain()
        self.add_price(first + dt.timedelta(days=260 * 7), 370)
        self.build_inputs()
        self.build_chain(incremental=True)
        self.assert_incremental_matches_full()
        self.assertIsNotNone(self.db.execute("""
            SELECT sma_200 FROM calc_security_daily_momentum ORDER BY trade_date DESC LIMIT 1
        """).fetchone()[0])

    def test_removed_first_slice_row_cannot_be_used_as_previous_state(self):
        first = dt.date(2025, 10, 1)
        self.add_constituent(SEEDS[-1], "BBB")
        for i in range(30):
            self.add_price(first + dt.timedelta(days=i), 100 + i)
        self.add_price(first + dt.timedelta(days=10), 10, ticker="BBB")
        removed = first + dt.timedelta(days=27)
        self.add_price(removed, 20, ticker="BBB")
        self.add_price(first + dt.timedelta(days=29), 30, ticker="BBB")
        self.build_inputs()
        self.build_chain()
        self.db.execute("""
            DELETE FROM raw_daily_stocks WHERE API_DATE = ? AND RAW_PAYLOAD LIKE '%BBB%'
        """, [removed])
        self.build_inputs()
        self.build_chain(incremental=True)
        self.assert_incremental_matches_full()
        self.assertEqual(self.db.execute("""
            SELECT yesterday_close, observation_count FROM int_market__daily
            WHERE ticker = 'BBB' ORDER BY trade_date DESC LIMIT 1
        """).fetchone(), (10.0, 2))

    def test_incremental_membership_removal_does_not_leave_old_fact_rows(self):
        self.add_price(dt.date(2025, 9, 18), 10)
        self.build_inputs()
        self.build_chain()
        self.db.execute("DELETE FROM raw_reference WHERE SOURCE='massive_ticker_catalog'")
        self.add_constituent(SEEDS[-1], "BBB")
        self.build_inputs()
        self.build_chain(incremental=True)
        self.assert_incremental_matches_full()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM fct_security_daily_momentum").fetchone()[0], 0)

    def test_empty_replacement_window_removes_old_records(self):
        first = dt.date(2025, 10, 1)
        for i in range(3):
            self.add_price(first + dt.timedelta(days=i), 10)
        self.build_inputs()
        self.build_chain()
        self.db.execute("DELETE FROM raw_daily_stocks")
        self.build_inputs()
        self.build_chain(incremental=True)
        for model in INCREMENTAL_MODELS:
            self.assertEqual(self.db.execute(f"SELECT COUNT(*) FROM {model}").fetchone()[0], 0)
        self.assert_incremental_matches_full()

    def test_failed_replacement_can_roll_back_the_deleted_window(self):
        self.db.execute("""
            CREATE TABLE guarded (trade_date DATE, close DOUBLE CHECK (close > 0));
            INSERT INTO guarded VALUES (DATE '2025-10-01', 10);
            CREATE TABLE rejected (trade_date DATE, close DOUBLE);
            INSERT INTO rejected VALUES (DATE '2025-10-01', -1)
        """)
        with self.assertRaises(duckdb.ConstraintException):
            self.db.execute(self.replacement_sql("guarded", "rejected"))
        self.db.execute("ROLLBACK")
        self.assertEqual(self.db.execute("SELECT close FROM guarded").fetchall(), [(10.0,)])


if __name__ == "__main__":
    unittest.main()
