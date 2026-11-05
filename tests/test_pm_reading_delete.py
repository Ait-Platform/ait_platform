"""Confirmed meter reading deletion on isolated PostgreSQL."""
import unittest
from lxml import html
from sqlalchemy import text
from test_pm_historical_reading_edits import HistoricalReadingEditTests

class ReadingDeleteTests(HistoricalReadingEditTests):
    def confirmation(self,data):
        response=self.capture_post(self.readings_url,**dict(data,action='delete_reading'))
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        self.assertIn('Are you sure you want to delete this meter reading?',tree.text_content())
        self.assertTrue(tree.xpath('//*[@id="reading-delete-confirmation"]'))
        return dict(data,action='confirm_delete_reading',reading_delete_token=tree.xpath('//input[@name="reading_delete_token"]/@value')[0])
    def test_delete_both_types_refresh_remaining_monthly_maxima_and_audit(self):
        for meter in (98,99):
            self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,'2025-09-20',180)"),dict(m=meter));self.session.commit();self.conn.commit()
            record,data=self.edit(meter);before=self.state();confirmed=self.confirmation(data)
            self.assertEqual(before,self.state())
            response=self.capture_post(self.readings_url,**confirmed)
            self.assertEqual(response.status_code,302,response.text);self.assertIn('workspace=register',response.location)
            self.assertIsNone(self.session.execute(text('SELECT id FROM bil_meter_reading WHERE id=:r'),dict(r=record)).scalar())
            self.assertEqual(list(self.session.execute(text("SELECT month,consumption FROM bil_consumption WHERE meter_id=:m ORDER BY month"),dict(m=meter))),[('2025-09',80),('2025-10',120),('2025-11',777)])
            event=self.session.execute(text("SELECT before_data,after_data FROM bil_utilities_capture_audit WHERE kind='reading' AND record_id=:r"),dict(r=record)).one()
            self.assertEqual(event[0]['reading_value'],200);self.assertTrue(event[1]['deleted']);self.assertEqual(event[1]['meter_id'],meter)
            self.assertEqual(before[1],self.state()[1])
            self.assertIn('Meter reading deleted.',self.client.get(response.location,headers=self.headers).text)
    def test_deleting_only_monthly_reading_invalidates_derived_cache_not_financials(self):
        record,data=self.edit();before=self.state()
        self.assertEqual(self.capture_post(self.readings_url,**self.confirmation(data)).status_code,302)
        self.assertEqual(list(self.session.execute(text("SELECT month,consumption FROM bil_consumption WHERE meter_id=98 ORDER BY month"))),[('2025-11',777)])
        self.assertEqual(before[1],self.state()[1])
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_utilities_capture_audit WHERE kind='reading_consumption' AND after_data->>'invalidated'='true'")).scalar(),2)
    def test_cancel_preserves_draft_and_never_writes(self):
        record,data=self.edit(reading_value='225');before=self.state();confirmed=self.confirmation(data)
        response=self.capture_post(self.readings_url,**dict(confirmed,action='cancel_delete_reading'))
        self.assertEqual(response.status_code,200);self.assertEqual(before,self.state())
        tree=html.fromstring(response.data)
        self.assertFalse(tree.xpath('//*[@id="reading-delete-confirmation"]'))
        self.assertEqual(tree.xpath('//input[@name="reading_value"]/@value'),['225'])
        self.assertIn('Delete Reading',tree.text_content())
    def test_forged_stale_foreign_and_missing_targets_cannot_delete(self):
        record,data=self.edit();confirmed=self.confirmation(data);before=self.state()
        response=self.capture_post(self.readings_url,**dict(confirmed,reading_delete_token='forged'))
        self.assertEqual(response.status_code,200);self.assertEqual(before,self.state())
        for changes,code in [(dict(meter_id='97'),403),(dict(record_id='999999'),404),(dict(version='stale'),409)]:
            response=self.capture_post(self.readings_url,**dict(confirmed,**changes))
            self.assertEqual(response.status_code,code,response.text);self.assertEqual(before,self.state())
        self.session.execute(text('UPDATE bil_meter_reading SET reading_value=210 WHERE id=:r'),dict(r=record));self.session.commit();self.conn.commit();before=self.state()
        self.assertEqual(self.capture_post(self.readings_url,**confirmed).status_code,409);self.assertEqual(before,self.state())
    def test_audit_failure_rolls_back_reading_and_consumption(self):
        _,data=self.edit();confirmed=self.confirmation(data)
        self.session.execute(text("ALTER TABLE bil_utilities_capture_audit ADD CONSTRAINT fail_deletion CHECK(kind<>'reading')"));self.session.commit();self.conn.commit();before=self.state()
        with self.assertLogs(self.app.logger,level='ERROR'):response=self.capture_post(self.readings_url,**confirmed)
        self.assertEqual(response.status_code,503);self.assertEqual(before,self.state())
    def test_delete_button_only_on_reading_edit_and_csrf_required(self):
        _,data=self.edit();tree=html.fromstring(self.client.get(self.readings_url+'?edit='+data['record_id'],headers=self.headers).data)
        self.assertEqual(tree.xpath('//*[@id="capture-actions"]/button[@value="delete_reading"]/text()'),['Delete Reading'])
        for url in (self.readings_url,self.statements_url):self.assertNotIn('Delete Reading',self.client.get(url,headers=self.headers).text)
        response=self.client.post(self.readings_url,data=dict(data,action='delete_reading'),headers=self.headers)
        self.assertEqual(response.status_code,400)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReadingDeleteTests(name) for name in ReadingDeleteTests.__dict__ if name.startswith('test_'))
