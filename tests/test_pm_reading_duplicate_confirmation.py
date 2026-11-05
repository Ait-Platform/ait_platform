"""Exact-submission monthly duplicate confirmation on isolated PostgreSQL."""
import unittest
from lxml import html
from sqlalchemy import text
import test_pm_utilities_capture as fixture

class ReadingDuplicateTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(98,26,'HIST-E','Electricity'),(99,26,'HIST-W','Water'),(97,28,'FOREIGN','Water'),(96,26,'EMPTY-W','Water')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(98,'2025-01-10',100),(98,'2025-03-01',300),(99,'2025-01-10',100),(99,'2025-03-01',300)"))
        self.session.execute(text("INSERT INTO bil_consumption(meter_id,month,consumption) VALUES(98,'2025-01',777),(99,'2025-01',888)"))
        self.session.commit();self.conn.commit()
    def draft(self,**changes):
        fields=dict(meter_id='98',reading_date='2025-01-20',reading_value='150',billing_month='2026-11')
        fields.update(changes);return fields
    def post_draft(self,fields):return self.capture_post(self.readings_url,**fields)
    def token(self,response):
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        self.assertTrue(tree.xpath('//*[@id="reading-duplicate-warning"]'))
        token=tree.xpath('//input[@name="reading_duplicate_token"]/@value')[0]
        if tree.xpath('//button[@value="review_additional"]'):
            fields={e.get('name'):e.get('value','') for e in tree.xpath('//form[@id="utilities-capture"]//input[@name]')}
            fields['meter_id']=tree.xpath('//select[@name="meter_id"]/option[@selected]/@value')[0]
            fields['reading_duplicate_action']='review_additional'
            reviewed=self.capture_post(self.readings_url,**{k:v for k,v in fields.items() if k!='csrf_token'})
            token=html.fromstring(reviewed.data).xpath('//input[@name="reading_duplicate_token"]/@value')[0]
        return token
    def state(self):
        return self.snapshot(),list(self.session.execute(text('SELECT * FROM bil_utilities_capture_audit ORDER BY id')))

    def test_both_meter_types_warn_cancel_and_confirm_preserve_financial_history(self):
        for meter in ['98','99']:
            with self.subTest(meter=meter):
                fields=self.draft(meter_id=meter);before=self.state()
                warning=self.post_draft(fields);token=self.token(warning)
                tree=html.fromstring(warning.data)
                self.assertIn('A reading already exists for this meter for the selected month. Choose Replace Reading or Save Additional Reading.',tree.text_content())
                self.assertEqual(tree.xpath('//*[@id="reading-duplicate-warning"]//tbody/tr/td[position()>1]/text()'),['2025-01-10','100.0'])
                self.assertEqual(before,self.state())
                cancelled=self.post_draft(dict(fields,reading_duplicate_action='cancel',reading_duplicate_token=token))
                self.assertEqual(cancelled.status_code,200)
                cancelled_tree=html.fromstring(cancelled.data)
                self.assertFalse(cancelled_tree.xpath('//*[@id="reading-duplicate-warning"]'))
                for key in ['reading_date','reading_value']:self.assertEqual(cancelled_tree.xpath('//input[@name="'+key+'"]/@value'),[fields[key]])
                self.assertEqual(cancelled_tree.xpath('//select[@name="meter_id"]/option[@selected]/@value'),[meter])
                self.assertEqual(before,self.state())
                confirmed=self.post_draft(dict(fields,reading_duplicate_action='confirm',reading_duplicate_token=token))
                self.assertEqual(confirmed.status_code,302,confirmed.text)
                self.assertIn('workspace=capture',confirmed.location)
                after=self.snapshot()
                self.assertEqual(after['bil_consumption'],before[0]['bil_consumption'])
                for table,rows in before[0].items():
                    for row in rows:self.assertIn(row,after[table])
                self.assertEqual(len(after['bil_meter_reading']),len(before[0]['bil_meter_reading'])+1)
                audit=self.session.execute(text("SELECT after_data FROM bil_utilities_capture_audit WHERE kind='reading' ORDER BY id DESC LIMIT 1")).scalar()
                self.assertTrue(audit['additional_reading_confirmed'])
                saved=self.state()
                self.token(self.post_draft(dict(fields,reading_duplicate_action='confirm',reading_duplicate_token=token)))
                self.assertEqual(saved,self.state())

    def test_changed_payload_forged_token_and_new_existing_reading_require_fresh_check(self):
        fields=self.draft();token=self.token(self.post_draft(fields));before=self.state()
        for changes in [dict(meter_id='99'),dict(reading_date='2025-01-21'),dict(reading_value='151'),dict(reading_duplicate_token='forged')]:
            changed=dict(fields,reading_duplicate_action='confirm',reading_duplicate_token=token);changed.update(changes)
            self.token(self.post_draft(changed));self.assertEqual(before,self.state())
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(98,'2025-01-15',120)"));self.session.commit();self.conn.commit()
        before=self.state();response=self.post_draft(dict(fields,reading_duplicate_action='confirm',reading_duplicate_token=token))
        self.token(response);self.assertIn('2025-01-15',response.text);self.assertEqual(before,self.state())

    def test_same_date_confirmation_inserts_separate_record_without_overwrite(self):
        fields=self.draft(reading_date='2025-01-10',reading_value='105')
        before=self.state();token=self.token(self.post_draft(fields))
        self.assertEqual(self.post_draft(dict(fields,reading_duplicate_action='confirm',reading_duplicate_token=token)).status_code,302)
        values=list(self.session.execute(text("SELECT reading_value FROM bil_meter_reading WHERE meter_id=98 AND reading_date='2025-01-10' ORDER BY id")).scalars())
        self.assertEqual(values,[100,105]);self.assertEqual(before[0]['bil_consumption'],self.snapshot()['bil_consumption'])

    def test_different_month_year_meter_and_validation_keep_existing_rules(self):
        for fields in [self.draft(meter_id='96',reading_date='2025-01-20',reading_value='50'),self.draft(reading_date='2025-02-10',reading_value='200'),self.draft(meter_id='99',reading_date='2024-01-20',reading_value='50')]:
            result=self.post_draft(fields);self.assertEqual(result.status_code,302,result.text)
            self.assertIn('workspace=capture',result.location)
        before=self.state()
        self.assertEqual(self.post_draft(self.draft(meter_id='97')).status_code,403)
        self.assertEqual(self.post_draft(self.draft(reading_value='-1')).status_code,400)
        self.assertEqual(self.post_draft(self.draft(reading_value='NaN')).status_code,400)
        self.assertEqual(before,self.state())

    def test_period_controls_are_single_block_immediately_below_register_heading(self):
        tree=html.fromstring(self.client.get(self.readings_url+'?workspace=register',headers=self.headers).data)
        self.assertEqual(len(tree.xpath('//form[@id="municipal-reading-period"]')),1)
        form=tree.xpath('//*[@id="reading-register-title"]/following-sibling::*[1]')[0]
        self.assertEqual(form.get('id'),'municipal-reading-period')
        self.assertNotIn('justify-end',form.get('class'))
        self.assertEqual(form.xpath('.//input[@type="month"]/@name'),['billing_month'])
        self.assertEqual(form.xpath('.//button/text()'),['Show Period'])

    def test_browser_warning_cancel_confirmation_and_register(self):
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
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.goto('http://127.0.0.1:'+str(server.server_port)+self.readings_url+'?billing_month=2026-11')
                page.get_by_role('link',name='Capture Reading',exact=True).click()
                page.select_option('#meter_id','98');page.locator('#reading_date').fill('2025-01-20');page.locator('#reading_value').fill('150')
                before=self.state();page.get_by_role('button',name='Save',exact=True).click()
                self.assertTrue(page.locator('#reading-duplicate-warning').is_visible());self.assertEqual(before,self.state())
                page.get_by_role('button',name='Cancel',exact=True).click()
                self.assertFalse(page.locator('#reading-duplicate-warning').count())
                self.assertEqual(page.locator('#meter_id').input_value(),'98');self.assertEqual(page.locator('#reading_date').input_value(),'2025-01-20');self.assertEqual(page.locator('#reading_value').input_value(),'150')
                self.assertEqual(before,self.state())
                page.get_by_role('button',name='Save',exact=True).click()
                page.get_by_role('button',name='Save Additional Reading',exact=True).click()
                page.get_by_role('button',name='Confirm Save Additional Reading',exact=True).click()
                self.assertTrue(page.locator('#reading-capture').is_visible());self.assertFalse(page.locator('#reading-register').is_visible())
                self.assertEqual(page.locator('#reading_date').input_value(),'2025-01-20');self.assertEqual(page.locator('#meter_id').input_value(),'');self.assertEqual(page.locator('#reading_value').input_value(),'')
                self.assertIn('Meter reading saved.',page.content())
                target=case.session.execute(text("SELECT id FROM bil_meter_reading WHERE meter_id=98 AND reading_date='2025-01-10'")).scalar()
                count=case.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar()
                page.select_option('#meter_id','98');page.locator('#reading_date').fill('2025-01-05');page.locator('#reading_value').fill('110')
                page.get_by_role('button',name='Save',exact=True).click()
                page.locator('input[name="reading_replace_id"][value="'+str(target)+'"]').check()
                page.get_by_role('button',name='Replace Reading',exact=True).click()
                self.assertEqual(case.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),count)
                page.get_by_role('button',name='Confirm Replace Reading',exact=True).click()
                self.assertTrue(page.locator('#reading-capture').is_visible())
                self.assertEqual(page.locator('#reading_date').input_value(),'2025-01-05');self.assertEqual(page.locator('#meter_id').input_value(),'')
                self.assertEqual(case.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE id=:r'),dict(r=target)).scalar(),110)
                self.assertEqual(case.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),count)
                page.get_by_role('link',name='View Register',exact=True).click()
                self.assertIn('2025-01-20',page.locator('#reading-register').inner_text())
            finally:browser.close()


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReadingDuplicateTests(name) for name in ReadingDuplicateTests.__dict__ if name.startswith('test_'))
