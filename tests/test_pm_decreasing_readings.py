"""Lower historical corrections and captures warn without blocking; isolated data only."""
import unittest
from datetime import date
from sqlalchemy import text
from lxml import html
from test_pm_historical_reading_edits import HistoricalReadingEditTests

class DecreasingReadingTests(HistoricalReadingEditTests):
    def save_edit(self,meter=98,day='2025-09-30',**changes):
        record,data=self.edit(meter,day,**changes)
        response=self.capture_post(self.readings_url,**data)
        self.assertEqual(response.status_code,302,response.text)
        return record,self.client.get(response.location,headers=self.headers)
    def test_both_utilities_lower_corrections_keep_identity_date_and_refresh_valid_consumption(self):
        count=self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar()
        for meter in (98,99):
            statements=self.state()[1];record,response=self.save_edit(meter,reading_value='180')
            self.assertIn('The corrected reading is lower',response.text)
            self.assertEqual(self.session.execute(text('SELECT meter_id,reading_date,reading_value FROM bil_meter_reading WHERE id=:r'),dict(r=record)).one(),(meter,date(2025,9,30),180))
            self.assertEqual(list(self.session.execute(text("SELECT month,consumption FROM bil_consumption WHERE meter_id=:m ORDER BY month"),dict(m=meter))),[('2025-09',80),('2025-10',120),('2025-11',777)])
            self.assertEqual(statements,self.state()[1])
            event=self.session.execute(text("SELECT before_data,after_data FROM bil_utilities_capture_audit WHERE kind='reading' AND record_id=:r"),dict(r=record)).one()
            self.assertEqual(event[0]['reading_value'],200);self.assertEqual(event[1]['reading_value'],'180')
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),count)
    def test_below_preceding_month_saves_then_surrounding_corrections_rebuild_consumption(self):
        count=self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar();statements=self.state()[1]
        record,response=self.save_edit(reading_value='80')
        self.assertIn('lower than the preceding',response.text);self.assertIn('Monthly consumption is unavailable for 2025-09',response.text)
        self.assertIsNone(self.session.execute(text("SELECT consumption FROM bil_consumption WHERE meter_id=98 AND month='2025-09'")).scalar())
        self.assertEqual(self.session.execute(text("SELECT consumption FROM bil_consumption WHERE meter_id=98 AND month='2025-10'")).scalar(),220)
        self.save_edit(day='2025-08-31',reading_value='60')
        self.save_edit(day='2025-10-31',reading_value='120')
        self.assertEqual(list(self.session.execute(text("SELECT month,consumption FROM bil_consumption WHERE meter_id=98 ORDER BY month"))),[('2025-09',20),('2025-10',40),('2025-11',777)])
        self.assertEqual(self.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE id=:r'),dict(r=record)).scalar(),80)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading')).scalar(),count);self.assertEqual(statements,self.state()[1])
        register=self.client.get(self.readings_url+'?workspace=register',headers=self.headers)
        tree=html.fromstring(register.data)
        self.assertEqual(tree.xpath('//tr[@data-meter-id="98"][td[1]="2025-09-30"]/td[3]/text()'),['80 kWh'])
    def test_normal_capture_decrease_saves_with_inline_warning_and_invalid_values_still_fail(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(96,26,'CAPTURE-LOW','Water')"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(96,'2025-08-31',500)"));self.session.commit();self.conn.commit()
        response=self.capture_post(self.readings_url,meter_id='96',reading_date='2025-09-30',reading_value='450')
        self.assertEqual(response.status_code,302,response.text)
        tree=html.fromstring(self.client.get(response.location,headers=self.headers).data)
        self.assertIn('lower than the preceding',tree.xpath('//*[@id="reading-capture"]')[0].text_content())
        self.assertTrue(tree.xpath('//*[@id="reading-save-success"]'))
        self.assertEqual(self.session.execute(text("SELECT reading_value FROM bil_meter_reading WHERE meter_id=96 AND reading_date='2025-09-30'")).scalar(),450)
        before=self.state()
        for value in ('','-1','not-a-number','NaN','Infinity'):
            self.assertEqual(self.capture_post(self.readings_url,meter_id='96',reading_date='2025-10-01',reading_value=value).status_code,400)
            self.assertEqual(before,self.state())
    def test_failed_audit_rolls_back_lower_reading_and_all_affected_consumption(self):
        self.session.execute(text("ALTER TABLE bil_utilities_capture_audit ADD CONSTRAINT fail_lower_reading CHECK(kind<>'reading')"));self.session.commit();self.conn.commit()
        _,data=self.edit(reading_value='80');before=self.state()
        with self.assertLogs(self.app.logger,level='ERROR'):response=self.capture_post(self.readings_url,**data)
        self.assertEqual(response.status_code,503,response.text);self.assertEqual(before,self.state())
        self.assertNotIn('The corrected reading is lower',response.text)
    def test_stale_and_foreign_requests_remain_rejected(self):
        record,data=self.edit(reading_value='80')
        self.assertEqual(self.capture_post(self.readings_url,**data).status_code,302)
        before=self.state();self.assertEqual(self.capture_post(self.readings_url,**data).status_code,409);self.assertEqual(before,self.state())
        _,data=self.edit(reading_value='70');data['meter_id']='97'
        self.assertEqual(self.capture_post(self.readings_url,**data).status_code,403);self.assertEqual(before,self.state())
    def test_browser_edit_saves_actual_lower_value_and_register_shows_same_record(self):
        from playwright.sync_api import sync_playwright
        from http.server import HTTPServer,BaseHTTPRequestHandler
        from threading import Thread
        case=self;record,_=self.edit();count=len(self.snapshot()['bil_meter_reading'])
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
                page=browser.new_page();page.goto('http://127.0.0.1:'+str(server.server_port)+self.readings_url+'?edit='+str(record))
                page.locator('#reading_value').fill('80');page.get_by_role('button',name='Save',exact=True).click()
                self.assertIn('The corrected reading is lower',page.content())
                page.get_by_role('link',name='View Register',exact=True).click()
                self.assertIn('80 kWh',page.locator('[data-reading-row][data-meter-id="98"]').all_inner_texts()[1])
            finally:browser.close()
        self.assertEqual(len(self.snapshot()['bil_meter_reading']),count)
        self.assertEqual(self.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE id=:r'),dict(r=record)).scalar(),80)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(DecreasingReadingTests(name) for name in DecreasingReadingTests.__dict__ if name.startswith('test_'))
