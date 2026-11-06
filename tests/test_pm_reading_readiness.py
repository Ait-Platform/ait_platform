"""Reading capture is independent of reconciliation schema."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
import test_pm_utilities_capture as fixture

class ReadingReadinessTests(fixture.UtilitiesCaptureTests):
    def test_missing_mar_table_does_not_block_reading_capture(self):
        self.session.execute(text('DROP TABLE bil_municipal_statement'))
        self.session.commit();self.conn.commit()
        page=self.client.get(self.readings_url,headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        response=self.capture_post(self.readings_url,meter_id=1,reading_date='2026-09-30',reading_value='100')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),2)
        with self.app.test_request_context():self.assertFalse(self.capture.schema_ready())

    def test_missing_audit_still_blocks_reading_capture(self):
        original=self.capture.available
        with patch.object(self.capture,'available',side_effect=lambda table,cols:False if table=='bil_utilities_capture_audit' else original(table,cols)):
            response=self.capture_post(self.readings_url,meter_id=1,reading_date='2026-09-30',reading_value='100')
        self.assertEqual(response.status_code,503)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReadingReadinessTests(n) for n in ReadingReadinessTests.__dict__ if n.startswith('test_'))
