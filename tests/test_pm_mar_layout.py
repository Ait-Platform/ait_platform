"""MAR-only workspace visibility and navigation over existing authorized routes."""
import unittest
from urllib.parse import parse_qs,urlsplit
from lxml import html
import test_pm_reconciliation_payments as fixture


class MARLayoutTests(fixture.ReconciliationPaymentTests):
    def test_initial_mar_header_menu_and_hidden_workspaces_are_read_only(self):
        before=self.snapshot_payments()
        page=self.client.get(self.statements_url+'?billing_month=2026-09',headers=self.headers)
        self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        title=tree.xpath('//h1')[0]
        self.assertEqual(title.text,'Municipal Account Reconciliation (MAR)')
        self.assertIn('flex',title.getparent().get('class'))
        self.assertEqual(title.getparent().xpath('./a/text()'),['Back to Utilities'])
        self.assertIn('billing_month=2026-09',title.getparent().xpath('./a/@href')[0])
        self.assertEqual(tree.xpath('//nav[@id="mar-menu"]/a/text()'),['Add','View Register'])
        self.assertFalse(tree.xpath('//nav[@aria-label="Municipal Utilities"]'))
        context=tree.xpath('//*[@id="mar-context-row"]')[0]
        self.assertEqual(context.xpath('./h2/text()'),['Synthetic property'])
        self.assertEqual(context.xpath('./nav/@id'),['mar-menu'])
        self.assertIn('justify-between',context.get('class'))
        self.assertEqual(len(tree.xpath('//h2[text()="Synthetic property"]')),1)
        self.assertTrue(tree.xpath('//*[@id="mar-workspaces"][@hidden]'))
        self.assertEqual(len(tree.xpath('//*[@data-mar-section][not(@hidden)]')),0)
        self.assertTrue(tree.xpath('//*[@id="mar-register"]//a[@id="statement-output"]'))
        self.assertEqual(before,self.snapshot_payments())

    def test_existing_edit_payment_and_error_forms_open_one_workspace(self):
        for url,section in [(self.statements_url+'?edit='+str(self.statement['id']),'form'),(self.url,'payment'),(self.statements_url+'?workspace=add','form'),(self.statements_url+'?workspace=register','register')]:
            page=self.client.get(url,headers=self.headers)
            tree=html.fromstring(page.data)
            self.assertEqual(tree.xpath('//*[@data-mar-section][not(@hidden)]/@data-mar-section'),[section])
        invalid=self.payment_form(payment_amount='0')
        response=self.submit(invalid)
        self.assertEqual(response.status_code,400)
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//*[@data-mar-section][not(@hidden)]/@data-mar-section'),['payment'])
        self.assertFalse(tree.xpath('//input[@name="payment_reference"]'))

    def test_meter_readings_keep_the_existing_layout_and_navigation(self):
        page=self.client.get(self.readings_url,headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//h1/text()'),['Meter Readings'])
        self.assertTrue(tree.xpath('//nav[@aria-label="Municipal Utilities"]'))
        self.assertFalse(tree.xpath('//*[@id="mar-menu" or @id="mar-workspaces"]'))
        self.assertFalse(tree.xpath('//script[contains(@src,"pm_mar_workspace")]'))
        self.assertTrue(tree.xpath('//form[@id="utilities-capture"]'))
        self.assertTrue(tree.xpath('//a[@id="reading-output"]'))

    def test_browser_add_register_edit_payment_close_and_context(self):
        from playwright.sync_api import sync_playwright
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from threading import Thread
        case=self;before=self.snapshot_payments()
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.respond(case.client.get(self.path,headers=case.headers))
            def do_POST(self):
                body=self.rfile.read(int(self.headers['Content-Length']))
                self.respond(case.client.post(self.path,data=body,content_type=self.headers.get('Content-Type'),headers=case.headers))
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
                page=browser.new_page();page.goto(origin+self.statements_url+'?billing_month=2026-09')
                self.assertFalse(page.locator('#mar-workspaces').is_visible())
                self.assertEqual(page.locator('[data-mar-section]:visible').count(),0)
                page.get_by_role('link',name='Add',exact=True).click()
                self.assertTrue(page.locator('#mar-form').is_visible())
                self.assertEqual(page.locator('[data-mar-section]:visible').count(),1)
                page.locator('#capture-cancel').click()
                self.assertFalse(page.locator('#mar-workspaces').is_visible())
                page.get_by_role('link',name='View Register',exact=True).click()
                self.assertTrue(page.locator('#mar-register').is_visible())
                self.assertFalse(page.locator('#utilities-capture').is_visible())
                page.locator('#statement-account-filter').select_option('1')
                row=page.locator('[data-statement-row]').filter(has_text='2026-09-28').filter(has=page.get_by_role('link',name='Edit',exact=True)).first
                row.get_by_role('link',name='Edit',exact=True).click()
                self.assertTrue(page.locator('#mar-form').is_visible())
                self.assertEqual(page.locator('[data-mar-section]:visible').count(),1)
                self.assertEqual(page.locator('#statement_date').input_value(),'2026-09-28')
                self.assertIn('view_item=1',page.url);self.assertIn('billing_month=2026-09',page.url)
                page.locator('#due_date').fill('2026-09-01')
                page.locator('#utilities-capture').get_by_role('button',name='Save',exact=True).click()
                self.assertIn('Due date cannot precede',page.content())
                self.assertTrue(page.locator('#mar-form').is_visible())
                page.get_by_role('link',name='Close',exact=True).click()
                self.assertFalse(page.locator('#mar-workspaces').is_visible())
                page.get_by_role('link',name='Add',exact=True).click()
                self.assertEqual(page.locator('#utilities-capture input[name="record_id"]').input_value(),'')
                self.assertEqual(page.locator('#account_id').input_value(),'')
                page.get_by_role('link',name='View Register',exact=True).click()
                page.locator('#statement-account-filter').select_option('1')
                page.locator('[data-statement-row]').filter(has_text='2026-09-28').first.get_by_role('link',name='Record Payment',exact=True).click()
                self.assertTrue(page.locator('#mar-payment').is_visible())
                self.assertEqual(page.locator('[data-mar-section]:visible').count(),1)
                self.assertEqual(page.locator('#municipal-payment-form [name="payment_reference"]').count(),0)
                self.assertTrue(page.locator('#municipal-payment-form [name="payment_date"]').is_editable())
                self.assertTrue(page.locator('#municipal-payment-form [name="payment_amount"]').is_editable())
                page.locator('#municipal-payment-form').get_by_role('link',name='Cancel',exact=True).click()
                self.assertFalse(page.locator('#mar-workspaces').is_visible())
                page.reload()
                self.assertFalse(page.locator('#mar-workspaces').is_visible())
                self.assertIn('billing_month=2026-09',page.url)
                self.assertIn('view_item=1',page.url)
            finally:browser.close()
        self.assertEqual(before,self.snapshot_payments())


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(MARLayoutTests(name) for name in MARLayoutTests.__dict__ if name.startswith('test_'))
