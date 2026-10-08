import datetime as dt
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from src.backfill_batches import land_batch
from src.config import AWS
from src.reference_load import REFERENCE_PREFIX


class BatchLandingTest(unittest.TestCase):
    def setUp(self):
        self.cursor=MagicMock()
        self.warehouse=SimpleNamespace(cursor=self.cursor)

    def archive(self,item):
        records,date,source,requested=item
        prefix=AWS['s3_prefix'].strip('/') if source=='polygon_grouped_daily' else REFERENCE_PREFIX+'/'+source
        return dict(date=date,source=source,requested=requested,row_count=len(records),
                    key=prefix+'/api_date='+date+'/run_id=test/reference_raw.ndjson.gz',
                    bucket='fixture',etag='etag',sha256='sha',run_id='test')

    def run_batch(self,items,prices=False):
        with patch('src.backfill_batches.archive_item',side_effect=self.archive):
            return land_batch(self.warehouse,items,prices=prices)

    def statements(self):
        return [call.args[0] for call in self.cursor.execute.call_args_list]

    def test_missing_prices_append_without_deleting_retained_dates(self):
        self.cursor.fetchall.return_value=[(dt.date(2023,1,3),'polygon_grouped_daily',1)]
        self.run_batch([([{'T':'AAA'}],'2023-01-03','polygon_grouped_daily',1)],prices=True)
        sql=self.statements()
        self.assertFalse(any('DELETE' in s for s in sql))
        self.assertTrue(any('WHERE NOT EXISTS' in s for s in sql))
        self.assertIn('COMMIT',sql)

    def test_reference_batch_replaces_only_manifest_source_dates(self):
        self.cursor.fetchall.return_value=[(dt.date(2024,1,2),'massive_ticker_catalog',1)]
        self.run_batch([([{'ticker':'AAA'}],'2024-01-02','massive_ticker_catalog',1)])
        deletes=[call for call in self.cursor.execute.call_args_list if call.args[0].startswith('DELETE')]
        self.assertEqual(len(deletes),2)
        self.assertTrue(all(call.args[1]==['2024-01-02','massive_ticker_catalog'] for call in deletes))
        self.assertIn('COMMIT',self.statements())

    def test_empty_successful_overview_still_records_manifest(self):
        self.cursor.fetchall.return_value=[]
        self.run_batch([([],'2024-01-02','massive_ticker_overview',1)])
        self.assertFalse(any(s.startswith('COPY') for s in self.statements()))
        self.assertTrue(any('REFERENCE_MANIFEST VALUES' in s for s in self.statements()))

    def test_archive_count_mismatch_cannot_change_raw_tables(self):
        self.cursor.fetchall.return_value=[]
        with self.assertRaisesRegex(ValueError,'count mismatch'):
            self.run_batch([([{'ticker':'AAA'}],'2024-01-02','massive_ticker_catalog',1)])
        self.assertNotIn('BEGIN',self.statements())
        self.assertFalse(any(s.startswith('DELETE') for s in self.statements()))

    def test_commit_failure_rolls_back(self):
        self.cursor.fetchall.return_value=[(dt.date(2024,1,2),'massive_ticker_catalog',1)]
        def execute(sql,*args):
            if sql=='COMMIT':
                raise RuntimeError('fixture commit failure')
        self.cursor.execute.side_effect=execute
        with self.assertRaises(RuntimeError):
            self.run_batch([([{'ticker':'AAA'}],'2024-01-02','massive_ticker_catalog',1)])
        self.assertIn('ROLLBACK',self.statements())

    def test_explicit_refresh_replaces_only_archived_dates_and_records_campaign(self):
        self.cursor.fetchall.return_value=[(dt.date(2024,1,2),'polygon_grouped_daily',1)]
        item=([{'T':'AAA'}],'2024-01-02','polygon_grouped_daily',1)
        with patch('src.backfill_batches.archive_item',side_effect=self.archive):
            land_batch(self.warehouse,[item],prices=True,refresh_campaign='fixture')
        deletes=[s for s in self.statements() if s.startswith('DELETE')]
        self.assertEqual(len(deletes),1)
        self.assertIn('API_DATE IN (SELECT DISTINCT API_DATE FROM',deletes[0])
        self.assertTrue(any('PRICE_REFRESH_MANIFEST' in s for s in self.statements()))


if __name__=='__main__':
    unittest.main()
