"""Historical exception identities stay outside physical meter and billing records."""
import unittest,importlib.util
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import text
from alembic.operations import Operations
from alembic.migration import MigrationContext
from lxml import html
import test_pm_meter_exception_charges as fixture
class HistoricalExceptionTests(fixture.ExceptionChargeTests):
    def setUp(self):
        super().setUp()
        spec=importlib.util.spec_from_file_location('historical_migration',Path('migrations/versions/pm_meter_exceptions_003.py'));migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.session.commit();self.conn.commit()
    def historical_fields(self,**changes):
        data=self.fields(meter_id='historical:new',historical_number='OLD-WATER-123',historical_utility='Water',historical_basis='Archived municipal correspondence identifies the meter and this account.',historical_verified='yes')
        data.update(changes);return data
    def test_historical_identity_registration_preserves_allocations_and_replays(self):
        before=self.snapshot();fields=self.historical_fields()
        result=self.post_exception(fields);self.assertEqual(result.status_code,302,result.text)
        self.assertEqual(self.post_exception(fields).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_identity')).scalar(),1)
        row=self.session.execute(text('SELECT * FROM bil_meter_exception WHERE id=:e'),dict(e=self.last_id())).mappings().one()
        self.assertIsNone(row['meter_id']);self.assertIsNotNone(row['historical_identity_id'])
        self.assertEqual(before,self.snapshot())
        page=self.client.get(self.exceptions_url+'?account_id=1&view=register',headers=self.headers);doc=html.fromstring(page.data)
        self.assertTrue(doc.xpath('//select[@name="meter_id"]/option[starts-with(@value,"h:")]'))
        fields=self.fields(meter_id='h:'+str(row['historical_identity_id']))
        self.assertEqual(self.post_exception(fields).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_exception_identity')).scalar(),1)
        detail=self.client.get(result.location,headers=self.headers);self.assertIn('OLD-WATER-123',detail.text)
    def test_existing_identity_conflicts_and_verification(self):
        for changes in [dict(historical_number='WATER-26'),dict(historical_number='FOREIGN-W'),dict(historical_verified=''),dict(historical_basis='')]:
            result=self.post_exception(self.historical_fields(**changes));self.assertIn(result.status_code,(400,409),result.text)
        self.assertEqual(self.post_exception(self.historical_fields()).status_code,302)
        self.assertEqual(self.post_exception(self.historical_fields(account_id='2')).status_code,409)
        self.assertEqual(self.post_exception(self.historical_fields(account_id='3')).status_code,403)
        hid=self.session.execute(text('SELECT id FROM bil_meter_exception_identity')).scalar()
        self.assertEqual(self.post_exception(self.fields(account_id='2',meter_id='h:'+str(hid))).status_code,400)
        self.assertEqual(self.post_exception(self.fields(replacement_meter_id='h:'+str(hid),replacement_date='2026-09-03')).status_code,400)
    def test_browser_historical_registration_without_replacement(self):
        from playwright.sync_api import sync_playwright
        page=self.client.get(self.exceptions_url+'?account_id=1&view=register',headers=self.headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort());tab.set_content(page.text,wait_until='domcontentloaded')
                form=tab.locator('form:has(input[name="action"][value="create"])')
                form.locator('select[name="meter_id"]').select_option('historical:new')
                tab.locator('details summary').click()
                form.locator('[name="historical_number"]').fill('ARCHIVED-WATER')
                form.locator('[name="historical_basis"]').fill('Verified archived bill for the selected account')
                form.locator('[name="historical_verified"]').check()
                form.locator('[name="incident_date"]').fill('2026-09-01')
                form.locator('[name="description"]').fill('Historical theft; replacement unknown')
                self.assertTrue(form.evaluate('(form)=>form.checkValidity()'))
                posted=form.evaluate('(form)=>Object.fromEntries(new FormData(form))');posted.pop('csrf_token');posted.pop('evidence',None)
                result=self.post_exception(posted);self.assertEqual(result.status_code,302,result.text)
            finally:browser.close()

    def test_unassigned_legacy_meter_uses_verified_account_without_reassignment(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number,status) VALUES(96,NULL,'UNASSIGNED-OLD','Water','MET-26','stolen')"));self.session.commit();self.conn.commit()
        before=self.snapshot()
        page=self.client.get(self.exceptions_url+'?account_id=1&view=register',headers=self.headers)
        self.assertIn('UNASSIGNED-OLD',page.text)
        self.assertEqual(self.post_exception(self.fields(meter_id='96')).status_code,302)
        self.assertEqual(self.post_exception(self.fields(account_id='2',meter_id='96')).status_code,400)
        self.assertEqual(before,self.snapshot())

    def test_old_registered_meter_remains_selectable_by_verified_account_number(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number,status) VALUES(95,26,'OLD-REGISTERED','Water','MET-26','stolen')"));self.session.commit();self.conn.commit()
        before=self.snapshot()
        self.assertEqual(self.post_exception(self.fields(meter_id='95')).status_code,302)
        self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(HistoricalExceptionTests(name) for name in HistoricalExceptionTests.__dict__ if name.startswith('test_'))
