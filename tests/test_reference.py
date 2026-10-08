import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from src.reference import MassiveReferenceClient, reference_dates


class ReferenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.session = Mock(headers={})
        self.client = MassiveReferenceClient(Path(self.temp.name)/'cache.db',
            base_url='https://api.massive.com', api_key='secret-test-key', interval=0, session=self.session)
    def tearDown(self):
        self.client.close()
        self.temp.cleanup()
    def response(self, result):
        response = Mock(status_code=200)
        response.json.return_value = result
        return response
    def test_quarters_include_baseline_and_end(self):
        self.assertEqual([str(d) for d in reference_dates('2024-01-01','2025-12-31')],
            ['2024-01-02','2024-03-28','2024-06-28','2024-09-30','2024-12-31',
             '2025-03-31','2025-06-30','2025-09-30','2025-12-31'])
    def test_complete_catalog_and_resume_do_not_repeat_requests(self):
        self.session.get.side_effect = [self.response({'results':[{'ticker':'AAA'}],
            'next_url':'https://api.massive.com/v3/reference/tickers?cursor=next&apiKey=do-not-send'}),
            self.response({'results':[{'ticker':'BBB'}]})]
        self.assertEqual(len(self.client.catalog('2024-01-02')),2)
        self.assertEqual(len(self.client.catalog('2024-01-02')),2)
        self.assertEqual(self.session.get.call_count,2)
        self.assertNotIn('apiKey',self.session.get.call_args.args[0])
    def test_foreign_pagination_host_cannot_receive_credentials(self):
        self.session.get.return_value = self.response({'results':[{'ticker':'AAA'}],
                                                     'next_url':'https://evil.example/data'})
        with self.assertRaisesRegex(ValueError,'pagination host'):
            self.client.catalog('2024-01-02')
        self.assertEqual(self.session.get.call_count,1)
    def test_duplicate_catalog_fails_instead_of_publishing_partial_population(self):
        self.session.get.return_value = self.response({'results':[{'ticker':'AAA'},{'ticker':'AAA'}]})
        with self.assertRaisesRegex(ValueError,'nonunique'):
            self.client.catalog('2024-01-02')
