import unittest
from unittest.mock import Mock, patch

from requests import RequestException

from src.extraction import _make_request_with_retry


class ExtractionLoggingTest(unittest.TestCase):
    @patch("src.extraction.time.sleep")
    @patch("src.extraction.requests.get")
    def test_client_error_logs_once_without_retry(self, get, sleep):
        get.return_value = Mock(
            status_code=401,
            text="invalid credentials",
        )

        with self.assertLogs("src.extraction", level="ERROR") as captured:
            result = _make_request_with_retry(
                "https://example.test/stocks/2026-01-14",
                params={},
                api_date="2026-01-14",
            )

        self.assertIsNone(result)
        get.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(len(captured.records), 1)
        self.assertIn("status=401", captured.output[0])

    @patch("src.extraction.time.sleep")
    @patch("src.extraction.requests.get")
    def test_rate_limit_logs_warning_before_success(self, get, sleep):
        rate_limited = Mock(status_code=429)
        success = Mock(status_code=200)
        success.json.return_value = {"results": [{"T": "AAPL"}]}
        get.side_effect = [rate_limited, success]

        with self.assertLogs("src.extraction", level="WARNING") as captured:
            result = _make_request_with_retry(
                "https://example.test/stocks/2026-01-14",
                params={},
                api_date="2026-01-14",
                max_retries=2,
            )

        self.assertEqual(result, {"results": [{"T": "AAPL"}]})
        self.assertIn("API rate limited", captured.output[0])
        self.assertIn("api_date=2026-01-14", captured.output[0])
        sleep.assert_called_once_with(60)

    @patch("src.extraction.time.sleep")
    @patch("src.extraction.requests.get")
    def test_exhausted_requests_log_terminal_error(self, get, sleep):
        get.side_effect = RequestException("timed out")

        with self.assertLogs("src.extraction", level="WARNING") as captured:
            result = _make_request_with_retry(
                "https://example.test/stocks/2026-01-14",
                params={},
                api_date="2026-01-14",
                max_retries=2,
            )

        self.assertIsNone(result)
        self.assertEqual(get.call_count, 2)
        self.assertEqual(sleep.call_count, 2)
        self.assertTrue(any("WARNING" in entry for entry in captured.output))
        self.assertEqual(captured.records[-1].levelname, "ERROR")
        self.assertTrue(
            any("API request did not complete" in entry for entry in captured.output)
        )


if __name__ == "__main__":
    unittest.main()
