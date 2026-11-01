"""Explicit owner/tenant recipients and generation gating on isolated PostgreSQL."""
import unittest
from sqlalchemy import text
from lxml import html
from unittest.mock import patch
import test_pm_municipal_metsoa as fixture
class ExplicitSelectionTests(fixture.MunicipalMetsoaTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("INSERT INTO bil_owner_identity(id,display_name) VALUES(100,'Natural Lessee'),(101,'Unrelated Owner')"))
        self.session.execute(text('INSERT INTO bil_property_ownership(property_id,owner_id) VALUES(26,100),(28,101)'))
        self.session.commit();self.conn.commit()
    def test_owner_and_tenant_choices_are_separate_and_property_scoped(self):
        before=self.snapshot();landing=self.client.get('/billing/utilities/26/generate-metsoa',headers=self.headers)
        self.assertEqual(landing.status_code,200)
        tree=html.fromstring(landing.data)
        options=tree.xpath('//select[@name="recipient_id"]/option')
        choices={o.get('value'):o.text for o in options}
        self.assertIn('Owner',choices['owner:100']);self.assertIn('Tenant',choices['11'])
        self.assertNotIn('owner:101',choices)
        url=self.generation_url.replace('recipient_id=11','recipient_id=owner:100')
        generated=self.client.get(url,headers=self.headers);self.assertEqual(generated.status_code,200,generated.text)
        selected=html.fromstring(generated.data).xpath('//select[@name="recipient_id"]/option[@selected]/@value')
        self.assertEqual(selected,['owner:100'])
        from urllib.parse import urlsplit,parse_qs
        link=html.fromstring(generated.data).xpath('//a[contains(@href,"format=pdf")]/@href')[0]
        query=parse_qs(urlsplit(link).query)
        self.assertEqual(query['recipient_id'],['owner:100']);self.assertEqual(query['billing_month'],['2026-10']);self.assertEqual(query['generate'],['1'])
        pdf=self.client.get(link,headers=self.headers);self.assertEqual(pdf.status_code,200);self.assertEqual(pdf.mimetype,'application/pdf')
        self.assertEqual(self.client.get(url.replace('owner:100','owner:101'),headers=self.headers).status_code,403)
        self.assertEqual(before,self.snapshot())
    def test_browser_requires_both_explicit_values_and_does_not_generate_on_entry(self):
        from playwright.sync_api import sync_playwright
        with patch.object(self.metsoa,'build',side_effect=AssertionError('Unexpected generation')):
            page=self.client.get('/billing/utilities/26/generate-metsoa?billing_month=2026-10&recipient_id=11',headers=self.headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort());tab.set_content(page.text,wait_until='domcontentloaded')
                button=tab.locator('#metsoa-generate-button')
                self.assertEqual(tab.locator('#metsoa-billing-month').input_value(),'')
                self.assertEqual(tab.locator('#metsoa-recipient').input_value(),'')
                self.assertTrue(button.is_disabled())
                tab.fill('#metsoa-billing-month','2026-10');self.assertTrue(button.is_disabled())
                tab.select_option('#metsoa-recipient','owner:100');self.assertTrue(button.is_enabled())
                tab.fill('#metsoa-billing-month','');self.assertTrue(button.is_disabled())
            finally:browser.close()
    def test_unlinked_ang489_and_any_other_meter_keep_normal_consumption(self):
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=184 WHERE meter_id=5"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2026-09-25',0)"));self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertEqual(next(w['summary']['consumption'] for w in data['water']['meters'] if w['meter_number']=='ANG489'),184)
        self.assertEqual(data['pm_bulk_notes'],[])
        self.session.execute(text("UPDATE bil_meter SET meter_number='ORDINARY-WATER' WHERE id=5"));self.session.commit();self.conn.commit()
        with self.app.test_request_context():renamed=self.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertEqual(renamed['grand_total'],data['grand_total']);self.assertEqual(renamed['pm_bulk_notes'],[])

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ExplicitSelectionTests(name) for name in ExplicitSelectionTests.__dict__ if name.startswith('test_'))
