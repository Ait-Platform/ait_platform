"""Real browser filtering over property-isolated canonical PostgreSQL readings."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_utilities_capture as fixture
class ReadingFilterTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        from app.program_billing import utilities_output
        mock=patch.object(utilities_output,'db',self.db);mock.start();self.addCleanup(mock.stop)
        mock=patch.object(utilities_output,'current_user',self.user);mock.start();self.addCleanup(mock.stop)
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(99,26,'ANG489','Water'),(98,26,'EMPTY','Water'),(97,28,'FOREIGN','Water'),(96,26,'ELEC-96','Electricity')"))
        for day in range(1,9):
            self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(99,CAST(:d AS date),:v)'),dict(d='2026-09-%02d'%day,v=100+day))
        self.session.commit();self.conn.commit()
    def test_filter_options_and_rows_are_property_isolated_and_descending(self):
        before=self.snapshot();page=self.client.get(self.readings_url,headers=self.headers)
        self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        options=tree.xpath('//select[@id="reading-meter-filter"]/option/text()')
        self.assertIn('All Meters',options);self.assertIn('ANG489',options);self.assertNotIn('FOREIGN',options)
        dates=tree.xpath('//tr[@data-reading-row][@data-meter-id="99"]/td[1]/text()')
        self.assertEqual(dates,sorted(dates,reverse=True));self.assertEqual(len(dates),8)
        self.assertEqual(before,self.snapshot())
    def test_reading_sections_and_edit_workspace_preserve_form_and_context(self):
        before=self.snapshot()
        tree=html.fromstring(self.client.get(self.readings_url+'?billing_month=2026-09',headers=self.headers).data)
        self.assertFalse(tree.xpath('//nav[@aria-label="Municipal Utilities"]'))
        self.assertEqual(tree.xpath('//nav[@id="reading-menu"]/a/text()'),['Capture Reading','View Register'])
        self.assertTrue(tree.xpath('//*[@id="reading-workspaces"][@hidden]'))
        self.assertTrue(tree.xpath('//section[@id="reading-register"]//button[@id="reading-output"][@value="municipal_pdf"][@form="municipal-reading-period"]'))
        self.assertTrue(tree.xpath('//section[@id="reading-capture"]//form[@id="utilities-capture"]'))
        self.assertTrue(tree.xpath('//section[@id="reading-register"]//tr[@data-reading-row]'))
        self.assertEqual(len(tree.xpath('//*[@data-reading-section][not(@hidden)]')),0)
        self.assertEqual(len(tree.xpath('//form[@id="utilities-capture"]')),1)
        record=self.session.execute(text('SELECT id FROM bil_meter_reading WHERE meter_id=99 ORDER BY reading_date DESC LIMIT 1')).scalar()
        edit=self.readings_url+'?edit='+str(record)+'&billing_month=2026-09&view_item=99'
        tree=html.fromstring(self.client.get(edit,headers=self.headers).data)
        self.assertTrue(tree.xpath('//section[@id="reading-capture"][not(@hidden)]//form[@id="utilities-capture"]'))
        self.assertEqual(len(tree.xpath('//form[@id="utilities-capture"]')),1)
        self.assertEqual(tree.xpath('//input[@name="record_id"]/@value'),[str(record)])
        self.assertEqual(tree.xpath('//input[@name="reading_value"]/@value'),['108.0'])
        self.assertEqual(tree.xpath('//input[@name="billing_month"]/@value'),['2026-09','2026-09'])
        self.assertEqual(tree.xpath('//select[@id="meter_id"]/option[@selected]/@value'),['99'])
        data=self.capture_form(edit);data.pop('csrf_token');data['reading_value']='-1'
        failed=self.capture_post(self.readings_url,**data)
        self.assertEqual(failed.status_code,400)
        invalid=html.fromstring(failed.data)
        self.assertEqual(invalid.xpath('//section[@id="reading-capture"]//input[@name="reading_value"]/@value'),['-1'])
        self.assertTrue(invalid.xpath('//p[@role="alert"]'))
        self.assertEqual(before,self.snapshot())

    def test_browser_filter_show_more_less_and_all_meters(self):
        from playwright.sync_api import sync_playwright
        before=self.snapshot();page=self.client.get(self.readings_url,headers=self.headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort())
                tab.set_content('<base href="http://127.0.0.1/">'+page.text,wait_until='domcontentloaded')
                tab.add_script_tag(path='app/static/js/pm_reading_toggle.js')
                tab.add_script_tag(path='app/static/js/pm_reading_workspace.js')
                self.assertFalse(tab.locator('#reading-workspaces').is_visible())
                tab.get_by_role('link',name='Capture Reading',exact=True).click()
                self.assertTrue(tab.locator('#reading-capture').is_visible())
                tab.select_option('#meter_id','99')
                self.assertEqual(tab.locator('#reading-unit').inner_text(),'kL')
                tab.select_option('#meter_id','96')
                self.assertEqual(tab.locator('#reading-unit').inner_text(),'kWh')
                tab.get_by_role('link',name='View Register',exact=True).click()
                rows=tab.locator('[data-reading-row]:visible')
                self.assertEqual(rows.count(),5)
                self.assertFalse(tab.locator('#reading-capture').is_visible())
                self.assertEqual(tab.locator('#reading-register-actions #reading-meter-filter').count(),1)
                self.assertEqual(tab.locator('#capture-actions').get_by_role('link',name='Cancel',exact=True,include_hidden=True).count(),1)
                self.assertEqual(tab.locator('#capture-actions').get_by_role('button',name='Save',exact=True,include_hidden=True).count(),1)
                tab.select_option('#reading-meter-filter','99')
                self.assertEqual(tab.locator('#reading-period-filter').input_value(),'99')
                self.assertEqual(tab.locator('#reading-output').get_attribute('value'),'municipal_pdf')
                self.assertEqual(rows.count(),5)
                self.assertEqual(tab.locator('[data-reading-row]:visible:not([data-meter-id="99"])').count(),0)
                self.assertEqual(tab.locator('#reading-toggle').inner_text(),'Show More (5 of 8)')
                tab.click('#reading-toggle');self.assertEqual(rows.count(),8)
                self.assertEqual(tab.locator('#reading-toggle').inner_text(),'Show Less (8 of 8)')
                tab.click('#reading-toggle');self.assertEqual(rows.count(),5)
                tab.select_option('#reading-meter-filter','98');self.assertEqual(rows.count(),0)
                self.assertTrue(tab.locator('#reading-filter-empty').is_visible())
                self.assertFalse(tab.locator('#reading-toggle').is_visible())
                tab.select_option('#reading-meter-filter','');self.assertEqual(rows.count(),5)
                self.assertEqual(tab.locator('#reading-period-filter').input_value(),'')
                tab.click('#reading-toggle');self.assertEqual(rows.count(),9)
            finally:browser.close()
        self.assertEqual(before,self.snapshot())
    def test_browser_hub_close_edit_capture_and_pdf_month_context(self):
        from playwright.sync_api import sync_playwright
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from threading import Thread
        from urllib.parse import urlsplit,parse_qs
        case=self;before=self.snapshot()
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
                tab=browser.new_page();tab.goto(origin+self.readings_url+'?billing_month=2026-10')
                self.assertFalse(tab.locator('#reading-workspaces').is_visible())
                self.assertEqual(tab.locator('[data-reading-section]:visible').count(),0)
                tab.get_by_role('link',name='Capture Reading',exact=True).click()
                self.assertTrue(tab.locator('#reading-capture').is_visible())
                tab.locator('#reading_date').evaluate('(el)=>el.blur()')
                for field in ['#reading_date','#meter_id','#reading_value']:
                    self.assertEqual(tab.locator(field).evaluate('(el)=>getComputedStyle(el).borderTopColor'),'rgb(184, 205, 230)')
                    self.assertEqual(tab.locator(field).evaluate('(el)=>getComputedStyle(el).height'),'42px')
                tab.locator('#reading_value').focus()
                self.assertEqual(tab.locator('#reading_value').evaluate('(el)=>getComputedStyle(el).borderTopColor'),'rgb(74, 144, 217)')
                tab.get_by_role('link',name='View Register',exact=True).click()
                self.assertEqual(tab.locator('[data-reading-section]:visible').count(),1)
                tab.select_option('#reading-meter-filter','99')
                self.assertEqual(tab.locator('input[type=month]').count(),0)
                self.assertEqual(tab.get_by_role('button',name='Show Period',exact=True).count(),0)
                self.assertEqual(tab.get_by_text('Reporting month',exact=True).count(),0)
                self.assertTrue(tab.locator('#reading-register').is_visible())
                self.assertEqual(tab.locator('#reading-meter-filter').input_value(),'99')
                # Verify the real form submits the selected month to the existing PDF endpoint.
                requests=[]
                def pdf_request(route):
                    requests.append(route.request.url)
                    route.fulfill(status=204,body='')
                tab.route('**/*format=municipal_pdf*',pdf_request)
                with tab.expect_request(lambda req:'format=municipal_pdf' in req.url):tab.get_by_role('button',name='Output',exact=True).click()
                self.assertEqual(parse_qs(urlsplit(requests[0]).query)['billing_month'],['2026-10'])
                self.assertEqual(urlsplit(requests[0]).path,self.readings_url)
                tab.get_by_role('link',name='Close',exact=True).click()
                self.assertFalse(tab.locator('#reading-workspaces').is_visible())
                tab.reload();self.assertFalse(tab.locator('#reading-workspaces').is_visible())
                tab.get_by_role('link',name='View Register',exact=True).click()
                tab.locator('[data-reading-row][data-meter-id="99"]').first.get_by_role('link',name='Edit',exact=True).click()
                self.assertTrue(tab.locator('#reading-capture').is_visible())
                self.assertEqual(tab.locator('[data-reading-section]:visible').count(),1)
                self.assertEqual(tab.locator('#reading_value').input_value(),'108.0')
                self.assertEqual(tab.locator('#capture-view-item').input_value(),'99')
                self.assertEqual(tab.locator('#municipal-reading-month').input_value(),'2026-10')
                tab.locator('#reading_value').fill('-1')
                tab.locator('#utilities-capture').evaluate('(form)=>{form.noValidate=true;form.requestSubmit();}')
                tab.wait_for_load_state('networkidle')
                self.assertTrue(tab.locator('#reading-capture').is_visible())
                self.assertEqual(tab.locator('#reading_value').input_value(),'-1')
                tab.get_by_role('link',name='View Register',exact=True).click()
                tab.get_by_role('link',name='Capture Reading',exact=True).click()
                self.assertEqual(tab.locator('#utilities-capture [name="record_id"]').input_value(),'')
                self.assertEqual(tab.locator('#reading_value').input_value(),'')
                tab.locator('#capture-cancel').click()
                self.assertFalse(tab.locator('#reading-workspaces').is_visible())
            finally:browser.close()
        self.assertEqual(before,self.snapshot())

    def test_browser_success_stays_in_capture_retains_date_and_validation_draft(self):
        from playwright.sync_api import sync_playwright
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from threading import Thread
        case=self;before=self.snapshot()
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
                tab=browser.new_page();tab.goto(origin+self.readings_url+'?billing_month=2026-10')
                tab.get_by_role('link',name='Capture Reading',exact=True).click()
                tab.locator('#reading_date').fill('2026-09-09')
                for meter,value in [('96','123.45'),('98','50')]:
                    tab.select_option('#meter_id',meter);tab.locator('#reading_value').fill(value)
                    tab.locator('#utilities-capture').get_by_role('button',name='Save',exact=True).click()
                    self.assertTrue(tab.locator('#reading-capture').is_visible())
                    self.assertFalse(tab.locator('#reading-register').is_visible())
                    self.assertEqual(tab.locator('[data-reading-section]:visible').count(),1)
                    self.assertEqual(tab.locator('#reading_date').input_value(),'2026-09-09')
                    self.assertEqual(tab.locator('#meter_id').input_value(),'')
                    self.assertEqual(tab.locator('#reading_value').input_value(),'')
                    self.assertIn('Meter reading saved.',tab.content())
                    self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading WHERE meter_id=:m AND reading_date=CAST(:d AS date)'),dict(m=int(meter),d='2026-09-09')).scalar(),1)
                tab.reload()
                self.assertEqual(tab.locator('#reading_date').input_value(),'2026-09-09')
                self.assertTrue(tab.locator('#reading-capture').is_visible())
                saved=self.snapshot()
                tab.select_option('#meter_id','96');tab.locator('#reading_date').fill('2026-09-10');tab.locator('#reading_value').fill('-1')
                tab.locator('#utilities-capture').evaluate('(form)=>{form.noValidate=true;form.requestSubmit();}')
                tab.wait_for_load_state('networkidle')
                self.assertTrue(tab.locator('#reading-capture').is_visible())
                self.assertFalse(tab.locator('#reading-register').is_visible())
                self.assertEqual(tab.locator('#meter_id').input_value(),'96')
                self.assertEqual(tab.locator('#reading_date').input_value(),'2026-09-10')
                self.assertEqual(tab.locator('#reading_value').input_value(),'-1')
                self.assertTrue(tab.locator('p[role="alert"]').is_visible())
                self.assertEqual(saved,self.snapshot())
                tab.get_by_role('link',name='Close',exact=True).click()
                self.assertFalse(tab.locator('#reading-workspaces').is_visible())
                tab.get_by_role('link',name='View Register',exact=True).click()
                self.assertTrue(tab.locator('#reading-register').is_visible())
            finally:browser.close()
        after=self.snapshot()
        for table in ('bil_muni_account','bil_consumption','bil_architecture_draft','bil_meter'):self.assertEqual(before[table],after[table])
        for row in before['bil_meter_reading']:self.assertIn(row,after['bil_meter_reading'])
        self.assertEqual(len(after['bil_meter_reading']),len(before['bil_meter_reading'])+2)

    def test_output_carries_meter_or_all_and_rejects_other_property(self):
        before=self.snapshot()
        base='/billing/utilities/26/output?source=readings&preview=1&item_id='
        page=self.client.get(base+'99',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//tbody/tr')),8)
        self.assertIn('ANG489',page.text);self.assertIn('view_item=99',page.text)
        self.assertEqual(len(html.fromstring(self.client.get(base,headers=self.headers).data).xpath('//tbody/tr')),9)
        self.assertEqual(self.client.get(base+'97',headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(base.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(base).status_code,403)
        self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReadingFilterTests(name) for name in ReadingFilterTests.__dict__ if name.startswith('test_'))
