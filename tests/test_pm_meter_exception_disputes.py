"""Municipal allegations never become physical observations or financial adjustments."""
import unittest,importlib.util
from pathlib import Path
from unittest.mock import patch
from io import BytesIO
from sqlalchemy import text
from alembic.operations import Operations
from alembic.migration import MigrationContext
import test_pm_meter_exception_historical as fixture
class DisputeTests(fixture.HistoricalExceptionTests):
    def setUp(self):
        super().setUp()
        spec=importlib.util.spec_from_file_location('dispute_migration',Path('migrations/versions/pm_meter_exceptions_004.py'));migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        before=self.statements_snapshot()
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.session.commit();self.conn.commit();self.assertEqual(before,self.statements_snapshot())
    def test_disputed_observations_unknown_amount_and_idempotency(self):
        before=self.snapshot();bills=self.statements_snapshot()
        fields=self.charge_fields(amount='',municipal_reading='15043.25',disputed_consumption='184')
        result=self.post_exception(fields);self.assertEqual(result.status_code,302,result.text)
        self.assertEqual(self.post_exception(fields).status_code,302)
        row=self.session.execute(text('SELECT * FROM bil_meter_exception_charge')).mappings().one()
        self.assertIsNone(row['amount']);self.assertEqual(row['dispute_details'],dict(meter_number='WATER-26',municipal_reading='15043.25',disputed_consumption='184'))
        self.assertEqual(before,self.snapshot());self.assertEqual(bills,self.statements_snapshot())
        page=self.client.get(result.location,headers=self.headers);self.assertEqual(page.status_code,200,page.text)
        self.assertIn('Amount not yet known',page.text);self.assertIn('15043.25',page.text)
        self.assertEqual(self.post_exception(dict(fields,municipal_reading='15044')).status_code,409)
    def test_historical_evidence_and_status_audit_preserve_originals(self):
        from app.program_billing import meter_exception_evidence as storage
        objects={};before=self.snapshot();bills=self.statements_snapshot()
        def upload(key,data,**kw):objects[key]=data;return key
        with patch.object(storage.transport,'upload_bytes_to_r2',side_effect=upload),patch.object(storage.transport,'read_file_from_r2',side_effect=lambda key:objects[key]):
            fields=self.historical_fields()
            result=self.post_exception(dict(fields,evidence=(BytesIO(self.png()),'annexure.png')));self.assertEqual(result.status_code,302,result.text)
            incident=self.last_id()
            self.assertEqual(self.post_exception(dict(fields,evidence=(BytesIO(self.png()),'annexure.png'))).status_code,302)
            row=self.session.execute(text('SELECT * FROM bil_meter_exception WHERE id=:e'),dict(e=incident)).mappings().one()
            self.assertIsNone(row['replacement_meter_id']);self.assertIsNone(row['replacement_date'])
            status=dict(action='status',account_id='1',exception_id=str(incident),version='1',status='Reported',status_date='2026-09-05',notification_date='2026-09-02',municipal_reference='ARCHIVE-REF',status_remarks='Municipality correspondence retained')
            self.assertEqual(self.post_exception(status).status_code,302)
            events=self.session.execute(text('SELECT kind FROM bil_meter_exception_event WHERE exception_id=:e ORDER BY id'),dict(e=incident)).scalars().all()
            self.assertEqual(events,['created','evidence_added','status_changed'])
            self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_evidence WHERE exception_id=:e'),dict(e=incident)).scalar(),1)
        self.assertEqual(before,self.snapshot());self.assertEqual(bills,self.statements_snapshot())
    def test_historical_identity_and_incident_rollback_on_evidence_failure(self):
        from app.program_billing import meter_exception_evidence as storage
        before=self.snapshot()
        with patch.object(storage.transport,'upload_bytes_to_r2',side_effect=RuntimeError('test private storage failure')):
            with self.assertLogs(self.app.logger,level='ERROR'):
                result=self.post_exception(dict(self.historical_fields(),evidence=(BytesIO(self.png()),'annexure.png')))
        self.assertEqual(result.status_code,503)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_identity')).scalar(),0)
        self.assertEqual(before,self.snapshot())

    def test_disputed_observation_validation_and_account_isolation(self):
        for changes in [dict(municipal_reading='NaN'),dict(disputed_consumption='-1'),dict(disputed_consumption='infinity'),dict(municipal_reading='9999999999999999')]:
            result=self.post_exception(self.charge_fields(**changes));self.assertEqual(result.status_code,400,result.text)
        self.assertEqual(self.post_exception(self.charge_fields(account_id='2')).status_code,403)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_charge')).scalar(),0)
    def test_browser_disputed_charge_form_and_private_evidence(self):
        from playwright.sync_api import sync_playwright
        from app.program_billing import meter_exception_evidence as storage
        self.assertEqual(self.post_exception(self.historical_fields()).status_code,302);incident=self.last_id()
        detail=self.client.get(self.exceptions_url+'?account_id=1&exception_id='+str(incident),headers=self.headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort());tab.set_content(detail.text,wait_until='domcontentloaded')
                form=tab.locator('form:has(input[name="action"][value="charge"])')
                form.locator('[name="statement_id"]').select_option(str(self.bill_id))
                form.locator('[name="municipal_reading"]').fill('15043')
                form.locator('[name="disputed_consumption"]').fill('184')
                form.locator('[name="charge_description"]').fill('Disputed estimated municipal billing')
                self.assertTrue(form.evaluate('(form)=>form.checkValidity()'))
                posted=form.evaluate('(form)=>Object.fromEntries(new FormData(form))');posted.pop('csrf_token');posted.pop('evidence',None)
            finally:browser.close()
        before=self.snapshot();bills=self.statements_snapshot();objects={}
        def upload(key,data,**kw):objects[key]=data;return key
        with patch.object(storage.transport,'upload_bytes_to_r2',side_effect=upload),patch.object(storage.transport,'read_file_from_r2',side_effect=lambda key:objects[key]):
            posted['evidence']=(BytesIO(self.png()),'municipal-correspondence.png')
            result=self.post_exception(posted);self.assertEqual(result.status_code,302,result.text)
            eid=self.session.execute(text('SELECT evidence_id FROM bil_meter_exception_charge_evidence')).scalar()
            url='/meter_exceptions/26/evidence/'+str(eid)
            self.assertEqual(self.client.get(url,headers=self.headers).data,self.png())
            self.assertEqual(self.client.get(url).status_code,403)
            self.assertEqual(self.client.get(url.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.assertEqual(before,self.snapshot());self.assertEqual(bills,self.statements_snapshot())
        saved=self.session.execute(text('SELECT dispute_details FROM bil_meter_exception_charge')).scalar();self.assertEqual(saved['meter_number'],'OLD-WATER-123')

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(DisputeTests(name) for name in DisputeTests.__dict__ if name.startswith('test_'))
