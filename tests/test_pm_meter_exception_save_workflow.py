"""Committed saves, validation recovery and in-tile flashes on isolated PostgreSQL."""
import unittest
from urllib.parse import urlsplit,parse_qs
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_meter_exception_disputes as fixture
class SaveWorkflowTests(fixture.DisputeTests):
    def setUp(self):
        super().setUp()
        self.client.get(self.exceptions_url+'?account_id=1&exception_id='+str(self.incident),headers=self.headers)
    def count(self):return self.session.execute(text('SELECT count(*) FROM bil_meter_exception')).scalar()
    def test_success_commits_redirects_flashes_once_and_replays_same_incident(self):
        before=self.count();fields=self.fields(replacement_meter_id='',replacement_date='')
        result=self.post_exception(fields);self.assertEqual(result.status_code,302,result.text)
        self.assertEqual(self.count(),before+1)
        saved=self.last_id();query=parse_qs(urlsplit(result.location).query)
        self.assertEqual(query['exception_id'],[str(saved)]);self.assertEqual(query['account_id'],['1'])
        row=self.session.execute(text('SELECT meter_id,replacement_meter_id,replacement_date FROM bil_meter_exception WHERE id=:e'),dict(e=saved)).one()
        self.assertEqual(tuple(row),(1,None,None))
        page=self.client.get(result.location,headers=self.headers);doc=html.fromstring(page.data)
        flash=doc.xpath('//*[@id="flash-toast-container"]')
        self.assertEqual(len(flash),1);self.assertIn('Meter exception saved successfully.',flash[0].text_content())
        self.assertFalse(doc.xpath('//form[@id="exception-create-form"]'))
        self.assertNotIn('Meter exception saved successfully.',self.client.get(result.location,headers=self.headers).text)
        replay=self.post_exception(fields);self.assertEqual(replay.location,result.location)
        self.assertEqual(self.count(),before+1)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_meter_exception_event WHERE exception_id=:e AND kind='created'"),dict(e=saved)).scalar(),1)
    def test_mismatched_replacement_is_not_saved_and_form_selection_is_retained(self):
        before=self.count()
        for changes,message in [(dict(replacement_meter_id='92'),'meter was selected without a replacement date'),(dict(replacement_date='2026-09-03'),'date was submitted without a replacement meter')]:
            fields=self.fields(**changes)
            result=self.post_exception(fields);self.assertEqual(result.status_code,400,result.text)
            self.assertEqual(self.count(),before)
            doc=html.fromstring(result.data);self.assertIn('Not saved.',doc.xpath('//*[@role="alert"]')[0].text_content());self.assertIn(message,result.text)
            self.assertTrue(doc.xpath('//select[@name="account_id"]/option[@value="1" and @selected]'))
            self.assertTrue(doc.xpath('//select[@name="meter_id"]/option[@value="1" and @selected]'))
            self.assertEqual(doc.xpath('//form[@id="exception-create-form"]//input[@name="submission_key"]/@value'),[fields['submission_key']])
            self.assertNotIn('Meter exception saved successfully.',result.text)
            fields.update(replacement_meter_id='',replacement_date='')
            saved=self.post_exception(fields);self.assertEqual(saved.status_code,302)
            self.client.get(saved.location,headers=self.headers)
            before+=1
    def test_failed_registration_with_old_detail_url_stays_in_registration(self):
        before=self.count();fields=self.fields(description='')
        result=self.capture_post(self.exceptions_url+'?account_id=1&exception_id='+str(self.incident),**fields)
        self.assertEqual(result.status_code,400,result.text);self.assertEqual(self.count(),before)
        doc=html.fromstring(result.data);form=doc.xpath('//form[@id="exception-create-form"]')[0]
        self.assertNotIn('exception_id',form.get('action'))
        self.assertFalse(doc.xpath('//form[@id="exception-status-form"]'))
    def test_commit_failure_has_no_success_flash_and_rolls_back(self):
        before=self.count()
        with patch.object(self.session,'commit',side_effect=RuntimeError('test commit failure')):
            with self.assertLogs(self.app.logger,level='ERROR'):result=self.post_exception(self.fields())
        self.assertEqual(result.status_code,503);self.assertEqual(self.count(),before)
        self.assertIn('Not saved.',result.text);self.assertNotIn('Meter exception saved successfully.',result.text)
    def test_browser_post_redirect_flash_failure_and_retry(self):
        from playwright.sync_api import sync_playwright
        from http.server import BaseHTTPRequestHandler,HTTPServer
        from threading import Thread
        case=self;requests=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def handle_request(self):
                data=self.rfile.read(int(self.headers.get('Content-Length','0'))) or None
                headers=dict(case.headers)
                if self.headers.get('Content-Type'):headers['Content-Type']=self.headers['Content-Type']
                response=case.client.open(self.path,method=self.command,data=data,headers=headers)
                requests.append((self.command,response.status_code))
                self.send_response(response.status_code)
                self.send_header('Content-Type',response.content_type)
                if response.location:self.send_header('Location',response.location)
                self.send_header('Content-Length',str(len(response.data)));self.end_headers();self.wfile.write(response.data)
            do_GET=handle_request
            do_POST=handle_request
        server=HTTPServer(('127.0.0.1',0),Handler);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(timeout=2)))
        origin='http://127.0.0.1:'+str(server.server_port)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page()
                tab.goto(origin+self.exceptions_url+'?account_id=1')
                form=tab.locator('#exception-create-form')
                form.locator('[name="meter_id"]').select_option('1')
                form.locator('[name="incident_date"]').fill('2026-09-01')
                form.locator('[name="description"]').fill('Browser save with no replacement')
                self.assertTrue(form.locator('[name="replacement_date"]').is_disabled())
                # Even an autofilled orphan date is excluded when No replacement is selected.
                form.locator('[name="replacement_date"]').evaluate("field=>field.value='2026-09-03'")
                before=self.count()
                with tab.expect_navigation():form.locator('button').click()
                self.assertEqual(self.count(),before+1)
                self.assertIn('exception_id=',tab.url)
                self.assertEqual(tab.get_by_text('Meter exception saved successfully.',exact=True).count(),1)
                self.assertIn(('POST',302),requests)
                tab.get_by_text('Register Another Exception',exact=True).click()
                form=tab.locator('#exception-create-form')
                form.locator('[name="meter_id"]').select_option('1')
                form.locator('[name="incident_date"]').fill('2026-09-01')
                # Native validation stays intact; use a server-only validation error.
                form.locator('[name="description"]').fill('   ')
                with tab.expect_navigation():form.locator('button').click()
                self.assertEqual(self.count(),before+1)
                self.assertIn(('POST',400),requests)
                self.assertIn('Not saved.',tab.locator('[role="alert"]').inner_text())
                self.assertEqual(tab.locator('[name="meter_id"]').input_value(),'1')
                self.assertEqual(tab.locator('#exception-account').input_value(),'1')
                self.assertEqual(tab.get_by_text('Meter exception saved successfully.',exact=True).count(),0)
                form=tab.locator('#exception-create-form');form.locator('[name="description"]').fill('Corrected registration')
                with tab.expect_navigation():form.locator('button').click()
                self.assertEqual(self.count(),before+2)
                self.assertEqual(tab.get_by_text('Meter exception saved successfully.',exact=True).count(),1)
            finally:browser.close()

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(SaveWorkflowTests(name) for name in SaveWorkflowTests.__dict__ if name.startswith('test_'))
