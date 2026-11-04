"""Accounts hub presentation over real account routes and isolated PostgreSQL."""
import unittest
from lxml import html
from sqlalchemy import text
import test_pm_municipal_accounts as fixture

class AccountsHubTests(fixture.MunicipalAccountsTests):
    def test_hub_only_initial_state_existing_edit_and_list_destinations(self):
        before=self.snapshot()
        tree=html.fromstring(self.client.get(self.accounts_url+'?billing_month=2026-10',headers=self.headers).data)
        self.assertEqual(tree.xpath('//h1/text()'),['Accounts'])
        self.assertFalse(tree.xpath('//nav[@aria-label="Municipal Utilities"]'))
        self.assertEqual(tree.xpath('//nav[@id="account-menu"]/a/text()'),['Add Account','View Register'])
        self.assertTrue(tree.xpath('//*[@id="account-context-row"]/h2[normalize-space()]'))
        self.assertIn('property_id=26',tree.xpath('//h1/parent::div/a/@href')[0])
        self.assertIn('billing_month=2026-10',tree.xpath('//h1/parent::div/a/@href')[0])
        self.assertTrue(tree.xpath('//*[@id="account-workspaces"][@hidden]'))
        self.assertFalse(tree.xpath('//*[@data-account-section][not(@hidden)]'))
        self.assertEqual(tree.xpath('//form[@id="municipal-account-form"]//input[not(@type="hidden")]/@name'),['account_number','water_number','water_municipal_reference','electricity_number','electricity_municipal_reference'])
        for query,visible in [('workspace=add','form'),('view=list','register'),('edit=1','form')]:
            page=self.client.get(self.accounts_url+'?billing_month=2026-10&'+query,headers=self.headers)
            self.assertEqual(page.status_code,200)
            self.assertEqual(html.fromstring(page.data).xpath('//*[@data-account-section][not(@hidden)]/@data-account-section'),[visible])
        self.assertEqual(before,self.snapshot())

    def test_browser_switch_close_save_edit_cancel_and_validation(self):
        from playwright.sync_api import sync_playwright,expect
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from threading import Thread
        self.session.execute(text("INSERT INTO bil_owner_identity(id,display_name) VALUES(800,'Hub Owner')"))
        self.session.execute(text('INSERT INTO bil_property_ownership(property_id,owner_id) VALUES(26,800)'))
        self.session.commit();self.conn.commit();before=self.snapshot()
        case=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.respond(case.client.get(self.path,headers=case.headers))
            def do_POST(self):
                self.respond(case.client.post(self.path,data=self.rfile.read(int(self.headers['Content-Length'])),content_type=self.headers.get('Content-Type'),headers=case.headers))
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
                page=browser.new_page();page.goto(origin+self.accounts_url+'?billing_month=2026-10')
                self.assertFalse(page.locator('#account-workspaces').is_visible())
                page.get_by_role('link',name='Add Account',exact=True).click()
                self.assertTrue(page.locator('#account-form-area').is_visible())
                page.locator('#account_number').evaluate('(el)=>el.blur()')
                for field in ['#account_number','#owner_name','#water_number','#water_municipal_reference','#electricity_number','#electricity_municipal_reference']:
                    self.assertEqual(page.locator(field).evaluate('(el)=>getComputedStyle(el).borderTopColor'),'rgb(184, 205, 230)')
                    self.assertEqual(page.locator(field).evaluate('(el)=>getComputedStyle(el).height'),'42px')
                page.get_by_role('link',name='View Register',exact=True).click()
                self.assertEqual(page.locator('[data-account-section]:visible').count(),1)
                self.assertTrue(page.locator('#account-register').is_visible())
                page.get_by_role('link',name='Close',exact=True).click()
                self.assertFalse(page.locator('#account-workspaces').is_visible())
                page.reload();self.assertFalse(page.locator('#account-workspaces').is_visible())
                page.get_by_role('link',name='Add Account',exact=True).click()
                page.locator('#account_number').fill('NEW-HUB-26');page.locator('#account_number').press('Tab')
                save=page.get_by_role('button',name='Save',exact=True)
                expect(save).to_be_enabled()
                page.locator('#owner_name').select_option('Hub Owner')
                page.locator('#water_number').fill('HUB-W')
                save.click()
                self.assertIn('view=list',page.url,page.locator('#account-form-area').inner_text())
                self.assertTrue(page.locator('#account-register').is_visible())
                self.assertEqual(page.locator('[data-account-section]:visible').count(),1)
                self.assertIn('Municipal account saved.',page.content())
                page.locator('#account-register tbody tr').filter(has_text='NEW-HUB-26').get_by_role('link',name='Edit',exact=True).click()
                self.assertTrue(page.locator('#account-form-area').is_visible())
                self.assertEqual(page.locator('#account_number').input_value(),'NEW-HUB-26')
                self.assertEqual(page.locator('#water_number').input_value(),'HUB-W')
                page.get_by_role('link',name='Cancel',exact=True).click()
                self.assertFalse(page.locator('#account-workspaces').is_visible())
                page.get_by_role('link',name='View Register',exact=True).click()
                page.get_by_role('link',name='Add Account',exact=True).click()
                self.assertEqual(page.locator('#account_number').input_value(),'')
                page.locator('#account_number').fill('INVALID-HUB-26');page.locator('#account_number').press('Tab');expect(page.get_by_role('button',name='Save',exact=True)).to_be_enabled()
                page.locator('#owner_name').select_option('Hub Owner')
                page.get_by_role('button',name='Save',exact=True).click()
                self.assertTrue(page.locator('#account-form-area').is_visible())
                self.assertEqual(page.locator('#account_number').input_value(),'INVALID-HUB-26')
                self.assertTrue(page.locator('#account-form-area p[role="alert"]').first.is_visible())
                self.assertEqual(page.locator('[data-account-section]:visible').count(),1)
            finally:browser.close()
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_muni_account WHERE account_number='NEW-HUB-26'")).scalar(),1)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_muni_account WHERE account_number='INVALID-HUB-26'")).scalar(),0)
        after=self.snapshot()
        for name in ('bil_meter_reading','bil_consumption','bil_architecture_draft'):self.assertEqual(before[name],after[name])
        for name in ('bil_muni_account','bil_meter'):
            for row in before[name]:self.assertIn(row,after[name])


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(AccountsHubTests(name) for name in AccountsHubTests.__dict__ if name.startswith('test_'))
