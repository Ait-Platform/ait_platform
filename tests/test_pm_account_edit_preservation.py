"""Saved account drafts retain identifiers and values until explicit Save; local PostgreSQL only."""
import unittest
from lxml import html
from sqlalchemy import text
import test_pm_municipal_accounts as fixture

class AccountEditPreservationTests(fixture.MunicipalAccountsTests):
    expected=dict(account_number='00083192896040',owner_name='preetha McDonald',water_number='000WABC',water_municipal_reference='W000ABC',electricity_number='000EAbC',electricity_municipal_reference='E000xyz')
    def setUp(self):
        super().setUp()
        self.session.execute(text('ALTER TABLE bil_meter ADD COLUMN IF NOT EXISTS municipal_reference varchar(100)'))
        self.session.execute(text("INSERT INTO ref_muni_owner(id,name) VALUES(700,'preetha McDonald')"))
        self.session.execute(text("INSERT INTO bil_owner_identity(id,display_name) VALUES(700,'preetha McDonald')"))
        self.session.execute(text('INSERT INTO bil_property_ownership(property_id,owner_id) VALUES(26,700)'))
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_reference,municipal_bill_number) VALUES(98,26,'000WABC','Water','W000ABC','00083192896040'),(99,26,'000EAbC','Electrical','E000xyz','00083192896040')"))
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number,owner_id,water_meter_id,elec_meter_id,muni_water_meter_no,muni_elec_meter_no) VALUES(2,26,'00083192896040',700,98,99,'000WABC','000EAbC')"))
        self.session.commit();self.conn.commit()

    def test_edit_initial_values_labels_name_display_and_optional_validation_unchanged(self):
        before=self.snapshot()
        page=self.client.get(self.accounts_url+'?edit=2',headers=self.headers)
        tree=html.fromstring(page.data)
        fields={e.get('name'):e.get('value') for e in tree.xpath('//form[@id="municipal-account-form"]//input')}
        fields['owner_name']=tree.xpath('//select[@name="owner_name"]/option[@selected]/@value')[0]
        self.assertEqual({k:fields[k] for k in self.expected},self.expected)
        self.assertEqual(tree.xpath('//select[@name="owner_name"]/option[@selected]/text()'),['Preetha McDonald'])
        labels=tree.xpath('//label[@for="water_number" or @for="water_municipal_reference" or @for="electricity_number" or @for="electricity_municipal_reference"]/text()')
        self.assertEqual(labels,['Water meter number','Water Municipal Reference','Electricity meter number','Electricity Municipal Reference'])
        self.assertFalse(tree.xpath('//input[@name="water_number" or @name="water_municipal_reference" or @name="electricity_number" or @name="electricity_municipal_reference"]/@required'))
        self.assertEqual(before,self.snapshot())

    def test_browser_edit_number_keeps_other_values_leading_zeros_validation_and_cancel(self):
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
        origin='http://127.0.0.1:'+str(server.server_port)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();requests=[];page.on('request',lambda request:requests.append((request.method,request.url)))
                page.goto(origin+self.accounts_url+'?billing_month=2026-10')
                page.get_by_role('link',name='View Register',exact=True).click()
                page.locator('#account-register tbody tr').filter(has_text=self.expected['account_number']).get_by_role('link',name='Edit',exact=True).click()
                form=page.locator('#municipal-account-form')
                for key,value in self.expected.items():self.assertEqual(form.locator('[name="'+key+'"]').input_value(),value)
                before=self.snapshot();version=form.locator('[name="version"]').input_value();requests.clear()
                form.locator('[name="account_number"]').fill('00083192896041');form.locator('[name="account_number"]').press('Tab')
                page.wait_for_timeout(500) # Exceeds the former 350ms lookup debounce.
                for key,value in self.expected.items():
                    if key!='account_number':self.assertEqual(form.locator('[name="'+key+'"]').input_value(),value)
                self.assertEqual(form.locator('[name="account_id"]').input_value(),'2')
                self.assertEqual(form.locator('[name="version"]').input_value(),version)
                self.assertFalse(any('/lookup' in url or method=='POST' for method,url in requests))
                self.assertEqual(before,self.snapshot())
                page.get_by_role('button',name='Save',exact=True).click()
                self.assertIn('view=list',page.url)
                self.assertEqual(self.session.execute(text('SELECT account_number FROM bil_muni_account WHERE id=2')).scalar(),'00083192896041')
                meters=list(self.session.execute(text('SELECT meter_number,municipal_reference FROM bil_meter WHERE id IN (98,99) ORDER BY id')))
                self.assertEqual(meters,[('000WABC','W000ABC'),('000EAbC','E000xyz')])
                self.assertEqual(self.session.execute(text('SELECT owner_id FROM bil_muni_account WHERE id=2')).scalar(),700)
                for name in ('bil_meter_reading','bil_consumption','bil_architecture_draft'):self.assertEqual(before[name],self.snapshot()[name])
                page.locator('#account-register tbody tr').filter(has_text='00083192896041').get_by_role('link',name='Edit',exact=True).click()
                after_save=self.snapshot();form=page.locator('#municipal-account-form')
                draft=dict(self.expected,account_number='000BAD?',water_municipal_reference='W000DRAFT')
                form.locator('[name="account_number"]').fill(draft['account_number'])
                form.locator('[name="water_municipal_reference"]').fill(draft['water_municipal_reference'])
                page.get_by_role('button',name='Save',exact=True).click()
                self.assertTrue(page.locator('#account-form-area p[role="alert"]').first.is_visible())
                self.assertTrue(page.locator('#account-form-area').is_visible())
                for key,value in draft.items():self.assertEqual(page.locator('#municipal-account-form [name="'+key+'"]').input_value(),value)
                self.assertEqual(after_save,self.snapshot())
                page.get_by_role('link',name='Cancel',exact=True).click()
                self.assertFalse(page.locator('#account-workspaces').is_visible())
                self.assertEqual(after_save,self.snapshot())
                page.get_by_role('link',name='View Register',exact=True).click()
                page.locator('#account-register tbody tr').filter(has_text='00083192896041').get_by_role('link',name='Edit',exact=True).click()
                self.assertEqual(page.locator('[name="water_municipal_reference"]').input_value(),'W000ABC')
                self.assertEqual(page.locator('[name="electricity_municipal_reference"]').input_value(),'E000xyz')
            finally:browser.close()


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AccountEditPreservationTests(name) for name in AccountEditPreservationTests.__dict__ if name.startswith('test_'))
