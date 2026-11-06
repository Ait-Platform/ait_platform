"""Accounts month context and metadata edits; property 26 on isolated PostgreSQL."""
import unittest
from urllib.parse import urlparse,parse_qs
from lxml import html
from sqlalchemy import text
from test_pm_utilities_capture import UtilitiesCaptureTests

class AccountCorrectionsTests(UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text('ALTER TABLE bil_meter ADD COLUMN municipal_reference varchar(100)'))
        self.session.execute(text("UPDATE bil_meter SET meter_number=' 000wR ' WHERE id=1"))
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(2,26,'000AbC-S','Electrical','MET-26')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(2,'2025-09-30',100)"))
        self.session.execute(text("UPDATE bil_muni_account SET water_meter_id=1,elec_meter_id=2,muni_water_meter_no=' 000wR ',muni_elec_meter_no='000AbC-S' WHERE id=1"))
        self.session.execute(text("INSERT INTO ref_muni_owner(id,name) VALUES(902,'Current Owner')"))
        self.session.execute(text('UPDATE bil_muni_account SET owner_id=902 WHERE id=1'))
        self.session.execute(text("INSERT INTO bil_owner_identity(id,display_name) VALUES(901,'Correct Owner')"))
        self.session.execute(text('INSERT INTO bil_property_ownership(property_id,owner_id) VALUES(26,901)'))
        self.session.execute(text('CREATE TEMP TABLE bil_muni_payment(id integer PRIMARY KEY,account_id integer,month text,amount numeric)'))
        self.session.execute(text('CREATE TEMP TABLE bil_muni_cycle_totals(id integer PRIMARY KEY,account_id integer,period text,paid numeric,due numeric)'))
        self.session.execute(text("INSERT INTO bil_muni_payment VALUES(1,1,'2025-09',10)"))
        self.session.execute(text("INSERT INTO bil_muni_cycle_totals VALUES(1,1,'2025-09',10,20)"))
        self.session.commit();self.conn.commit()
    def edit(self,**changes):
        fields=self.account_form(1);fields.pop('csrf_token');fields.update(changes)
        return self.capture_post(self.accounts_url,**fields)
    def immutable_history(self):
        return {t:list(self.session.execute(text('SELECT * FROM '+t+' ORDER BY id'))) for t in ('bil_meter_reading','bil_consumption','bil_muni_payment','bil_muni_cycle_totals','bil_architecture_draft')}
    def test_owner_account_number_and_references_are_metadata_not_meter_renames(self):
        original=self.immutable_history()
        meter_ids=list(self.session.execute(text('SELECT id,meter_number,property_id FROM bil_meter ORDER BY id')))
        pointers=self.session.execute(text('SELECT id,property_id,water_meter_id,elec_meter_id FROM bil_muni_account WHERE id=1')).one()
        for changes in (dict(owner_name='Correct Owner'),dict(account_number='000CORRECTED-ACCOUNT'),dict(water_municipal_reference='W-000REF'),dict(electricity_municipal_reference='E-000REF')):
            response=self.edit(**changes);self.assertEqual(response.status_code,302,response.text)
            self.assertEqual(original,self.immutable_history())
            self.assertEqual(meter_ids,list(self.session.execute(text('SELECT id,meter_number,property_id FROM bil_meter ORDER BY id'))))
            self.assertEqual(pointers,self.session.execute(text('SELECT id,property_id,water_meter_id,elec_meter_id FROM bil_muni_account WHERE id=1')).one())
        self.assertEqual(list(self.session.execute(text('SELECT municipal_bill_number FROM bil_meter ORDER BY id'))),[('000CORRECTED-ACCOUNT',),('000CORRECTED-ACCOUNT',)])
    def test_unconfirmed_meter_changes_are_blocked_and_preserve_entered_values(self):
        before=self.snapshot();history=self.immutable_history()
        for changes in (dict(water_number='GENUINE-NEW'),dict(electricity_number='GENUINE-NEW'),dict(water_number='00wR')):
            response=self.edit(**changes);self.assertEqual(response.status_code,409,response.text)
            self.assertIn('Select Identifier correction (same physical meter)',response.text)
            tree=html.fromstring(response.data)
            for name,value in changes.items():self.assertEqual(tree.xpath('//input[@name="'+name+'"]/@value'),[value])
            self.assertEqual(before,self.snapshot());self.assertEqual(history,self.immutable_history())
    def test_case_and_padding_preserve_saved_characters_and_zeroes(self):
        history=self.immutable_history()
        response=self.edit(water_number='000wr',electricity_number='000abc-s',water_municipal_reference='New reference')
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(list(self.session.execute(text('SELECT meter_number FROM bil_meter ORDER BY id'))),[(' 000wR ',),('000AbC-S',)])
        self.assertEqual(history,self.immutable_history())
    def test_register_has_no_month_controls_and_keeps_existing_month_context(self):
        response=self.client.get(self.accounts_url+'?view=list&billing_month=2025-09',headers=self.headers)
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.data)
        self.assertFalse(tree.xpath('//input[@type="month"]'))
        self.assertFalse(tree.xpath('//*[@id="municipal-account-period"]'))
        self.assertNotIn('Show Period',tree.text_content())
        readings=self.client.get('/billing/utilities/26/meter-readings?workspace=register&billing_month=2025-09',headers=self.headers)
        reading_tree=html.fromstring(readings.data)
        self.assertEqual(reading_tree.xpath('//*[@id="municipal-reading-month"]/@value'),['2025-09'])
        self.assertIn('Show Period',reading_tree.text_content())
        response=self.client.get(self.accounts_url+'?view=list&billing_month=2025-10',headers=self.headers)
        tree=html.fromstring(response.data)
        for href in tree.xpath('//a[contains(@href,"edit=")]/@href'):
            self.assertEqual(parse_qs(urlparse(href).query)['billing_month'],['2025-10'])
        response=self.client.get(self.accounts_url+'?edit=1&billing_month=2025-10',headers=self.headers)
        fields=html.fromstring(response.data).xpath('//form[@id="municipal-account-form"]//input[@name="billing_month"]/@value')
        self.assertEqual(fields,['2025-10'])
        self.assertEqual(self.client.get(self.accounts_url+'?view=list&billing_month=2025-13',headers=self.headers).status_code,400)
        self.assertEqual(self.client.get('/billing/utilities/28/accounts?view=list&billing_month=2025-10',headers=self.headers).status_code,403)

    def test_browser_register_edit_and_save_keep_existing_month_context(self):
        from playwright.sync_api import sync_playwright
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from threading import Thread
        case=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.respond(case.client.get(self.path,headers=case.headers))
            def do_POST(self):self.respond(case.client.post(self.path,data=self.rfile.read(int(self.headers['Content-Length'])),content_type=self.headers.get('Content-Type'),headers=case.headers))
            def respond(self,response):
                self.send_response(response.status_code);self.send_header('Content-Type',response.content_type)
                if response.location:self.send_header('Location',response.location)
                self.end_headers();self.wfile.write(response.data);response.close()
        server=HTTPServer(('127.0.0.1',0),Handler);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(timeout=2)))
        before=self.immutable_history()
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.goto('http://127.0.0.1:'+str(server.server_port)+self.accounts_url+'?billing_month=2025-09')
                self.assertFalse(page.locator('#account-register').is_visible())
                page.get_by_role('link',name='View Register',exact=True).click()
                self.assertTrue(page.get_by_role('heading',name='Recorded Municipal Accounts',exact=True).is_visible())
                self.assertEqual(page.locator('#municipal-account-month').count(),0)
                self.assertEqual(page.get_by_role('button',name='Show Period',exact=True).count(),0)
                self.assertIn('billing_month=2025-09',page.url);self.assertTrue(page.locator('#account-register').is_visible())
                page.get_by_role('link',name='Edit',exact=True).click()
                page.select_option('#owner_name','Correct Owner')
                page.get_by_role('button',name='Save',exact=True).click()
                self.assertIn('billing_month=2025-09',page.url)
                self.assertEqual(page.locator('#municipal-account-month').count(),0)
                self.assertTrue(page.locator('#account-register').is_visible())
                self.assertIn('Municipal account saved.',page.content())
                self.assertIn('Correct Owner',page.locator('#account-register').inner_text())
            finally:browser.close()
        self.assertEqual(before,self.immutable_history())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AccountCorrectionsTests(name) for name in AccountCorrectionsTests.__dict__ if name.startswith('test_'))
