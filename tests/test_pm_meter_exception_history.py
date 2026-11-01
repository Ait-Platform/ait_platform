"""Dated case statuses, register isolation and append-only event updates."""
import unittest,uuid
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_meter_exception_charges as fixture
class ExceptionHistoryTests(fixture.ExceptionChargeTests):
    def status_fields(self,**changes):
        version=self.session.execute(text('SELECT version FROM bil_meter_exception WHERE id=:e'),dict(e=self.incident)).scalar()
        fields=dict(action='status',account_id='1',exception_id=str(self.incident),version=str(version),status='Reported',status_date='2026-09-05',notification_date='2026-09-02',municipal_reference='MUNI-123',status_remarks='Municipality notified')
        fields.update(changes);return fields
    def test_status_sequence_and_effective_vs_recorded_history(self):
        before=self.snapshot();bills=self.statements_snapshot()
        for status,day in [('Reported','2026-09-05'),('Resolved','2026-09-10'),('Closed','2026-09-20')]:
            response=self.post_exception(self.status_fields(status=status,status_date=day));self.assertEqual(response.status_code,302,response.text)
        row=self.session.execute(text('SELECT * FROM bil_meter_exception WHERE id=:e'),dict(e=self.incident)).mappings().one()
        self.assertEqual(row['status'],'Closed');self.assertEqual(row['version'],4)
        events=self.session.execute(text('SELECT * FROM bil_meter_exception_event WHERE exception_id=:e ORDER BY id'),dict(e=self.incident)).mappings().all()
        self.assertEqual([r['kind'] for r in events],['created','status_changed','status_changed','status_changed'])
        self.assertEqual([r['details']['effective_date'] for r in events[1:]],['2026-09-05','2026-09-10','2026-09-20'])
        self.assertTrue(all(r['actor_id']==7 and r['recorded_at'].tzinfo for r in events))
        detail=self.client.get(response.location,headers=self.headers);self.assertIn('SAST',detail.text);self.assertIn('Reported',detail.text);self.assertIn('Back to Exception Register',detail.text)
        self.assertEqual(before,self.snapshot());self.assertEqual(bills,self.statements_snapshot())
    def test_stale_invalid_and_cross_account_changes_are_blocked(self):
        fields=self.status_fields();self.assertEqual(self.post_exception(fields).status_code,302)
        stale=self.post_exception(dict(fields,status='Resolved',status_remarks='Preserve this entered note'))
        self.assertEqual(stale.status_code,409);self.assertIn('Preserve this entered note',stale.text)
        for changes in [dict(status='Invalid'),dict(status='Resolved',status_date='2026-09-04'),dict(status_date='2026-08-01'),dict(notification_date='')]:
            response=self.post_exception(self.status_fields(**changes));self.assertEqual(response.status_code,400,response.text)
        self.assertEqual(self.post_exception(self.status_fields(account_id='2')).status_code,403)
        self.assertEqual(self.client.get(self.exceptions_url+'?account_id=2&exception_id='+str(self.incident),headers=self.headers).status_code,404)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_meter_exception_event WHERE kind='status_changed'")).scalar(),1)
    def test_notification_changes_and_notes_keep_prior_values_in_audit(self):
        result=self.post_exception(self.status_fields(status='Open',municipal_reference='CORRECTED-REF',status_remarks='Reference corrected'))
        self.assertEqual(result.status_code,302)
        event=self.session.execute(text("SELECT details FROM bil_meter_exception_event WHERE kind='details_updated'")).scalar()
        self.assertEqual(event['before']['municipal_reference'],'MUNI-123');self.assertEqual(event['after']['municipal_reference'],'CORRECTED-REF')
        self.assertEqual(self.post_exception(self.status_fields(status='Open',municipal_reference='CORRECTED-REF',status_remarks='Additional incident information')).status_code,302)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_meter_exception_event WHERE kind='note_added'")).scalar(),1)
        self.assertEqual(self.session.execute(text('SELECT description FROM bil_meter_exception WHERE id=:e'),dict(e=self.incident)).scalar(),'Meter incident')
    def test_audit_failure_rolls_back_status_and_version(self):
        self.session.execute(text("ALTER TABLE bil_meter_exception_event ADD CONSTRAINT audit_failure CHECK(kind<>'status_changed')"));self.session.commit();self.conn.commit()
        with self.assertLogs(self.app.logger,level='ERROR'):result=self.post_exception(self.status_fields())
        self.assertEqual(result.status_code,503)
        row=self.session.execute(text('SELECT status,version FROM bil_meter_exception WHERE id=:e'),dict(e=self.incident)).one()
        self.assertEqual(tuple(row),('Open',1))
    def test_register_property_account_filtering_and_rendered_forms(self):
        other=self.fields(account_id='2',meter_id='90',description='Other account incident')
        self.assertEqual(self.post_exception(other).status_code,302)
        self.session.execute(text("INSERT INTO bil_meter_exception(property_id,account_id,meter_id,exception_type,incident_date,description,submission_key,created_by) VALUES(28,3,91,'Missing','2026-09-01','Foreign incident',:key,99)"),dict(key=uuid.uuid4()));self.session.commit();self.conn.commit()
        all_page=self.client.get(self.exceptions_url,headers=self.headers);tree=html.fromstring(all_page.data)
        self.assertFalse(tree.xpath('//table[@id="exception-register"]'));self.assertTrue(tree.xpath('//form[@id="exception-account-form"]'))
        self.assertNotIn('Foreign incident',all_page.text)
        selected=self.client.get(self.exceptions_url+'?account_id=1',headers=self.headers)
        self.assertEqual(len(html.fromstring(selected.data).xpath('//table[@id="exception-register"]/tbody/tr')),1)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort());tab.set_content(all_page.text,wait_until='domcontentloaded')
                self.assertEqual(tab.locator('form:has(input[name="action"][value="create"])').count(),0)
                registration=self.client.get(self.exceptions_url+'?account_id=1&view=register',headers=self.headers)
                tab.set_content(registration.text,wait_until='domcontentloaded')
                options=tab.locator('select[name="meter_id"] option').evaluate_all('(items)=>items.map(item=>item.value)')
                self.assertNotIn('90',options);self.assertNotIn('91',options)
                detail=self.client.get(self.exceptions_url+'?account_id=1&exception_id='+str(self.incident),headers=self.headers)
                tab.set_content(detail.text,wait_until='domcontentloaded')
                tab.select_option('#exception-status-form select[name="status"]','Resolved')
                tab.fill('#exception-status-form input[name="status_date"]','2026-09-10')
                posted=tab.locator('#exception-status-form').evaluate('(form)=>Object.fromEntries(new FormData(form))')
                posted.pop('csrf_token');result=self.post_exception(posted)
                self.assertEqual(result.status_code,302,result.text)
            finally:browser.close()

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ExceptionHistoryTests(name) for name in ExceptionHistoryTests.__dict__ if name.startswith('test_'))
