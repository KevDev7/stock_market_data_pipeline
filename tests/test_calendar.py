import datetime as dt
import unittest

from src.extract_load_stocks import latest_completed_trading_day


class CalendarTest(unittest.TestCase):
    def test_weekend_uses_friday(self):
        self.assertEqual(latest_completed_trading_day(dt.date(2025,1,12)),dt.date(2025,1,10))

    def test_national_mourning_closure_is_not_a_source_date(self):
        self.assertEqual(latest_completed_trading_day(dt.date(2025,1,10)),dt.date(2025,1,8))

    def test_year_start_uses_previous_completed_year_end(self):
        self.assertEqual(latest_completed_trading_day(dt.date(2025,1,2)),dt.date(2024,12,31))


if __name__=='__main__':
    unittest.main()
