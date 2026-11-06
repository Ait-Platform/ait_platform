"""Capture-only success placement and scroll continuity; isolated PostgreSQL/browser."""
import unittest
from lxml import html
from sqlalchemy import text
from test_pm_utilities_capture import UtilitiesCaptureTests

class CaptureSuccessTests(UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(98,26,'CAPTURE-E','Electrical'),(99,26,'CAPTURE-W','Water')"));self.session.commit();self.conn.commit()
    def test_only_capture_success_moves_and_other_flashes_keep_their_container(self):
        with self.client.session_transaction() as session:session['_flashes']=[('success','Meter reading saved.'),('warning','Existing warning'),('success','Other success')]
        tree=html.fromstring(self.client.get(self.readings_url+'?workspace=capture',headers=self.headers).data)
        self.assertEqual(tree.xpath('//*[@id="reading-capture-heading"]/h2/@id'),['reading-capture-title'])
        self.assertEqual(tree.xpath('//*[@id="reading-capture-heading"]/*[@id="reading-save-success"]/text()'),['Meter reading saved.'])
        self.assertTrue(tree.xpath('//*[@id="reading-capture"]//*[@id="reading-save-success"]'))
        toast=tree.xpath('//*[@id="flash-toast-container"]')[0].text_content()
        self.assertIn('Existing warning',toast);self.assertIn('Other success',toast);self.assertNotIn('Meter reading saved.',toast)
        with self.client.session_transaction() as session:session['_flashes']=[('success','Meter reading saved.'),('success','Municipal statement saved.')]
        tree=html.fromstring(self.client.get(self.statements_url,headers=self.headers).data)
        self.assertFalse(tree.xpath('//*[@id="reading-save-success"]'))
        self.assertIn('Municipal statement saved.',tree.xpath('//*[@id="flash-toast-container"]')[0].text_content())
        self.assertIn('Meter reading saved.',tree.xpath('//*[@id="flash-toast-container"]')[0].text_content())
    def test_save_retains_date_clears_only_capture_values_and_shows_one_inline_message(self):
        response=self.capture_post(self.readings_url,meter_id='98',reading_date='2025-09-30',reading_value='100')
        self.assertEqual(response.status_code,302)
        tree=html.fromstring(self.client.get(response.location,headers=self.headers).data)
        self.assertEqual(tree.xpath('//input[@id="reading_date"]/@value'),['2025-09-30'])
        self.assertEqual(tree.xpath('//input[@id="reading_value"]/@value'),[''])
        self.assertFalse(tree.xpath('//select[@id="meter_id"]/option[@selected and @value!=""]'))
        self.assertEqual(tree.xpath('//*[@id="reading-save-success"]/text()'),['Meter reading saved.'])
        self.assertFalse(tree.xpath('//*[@id="flash-toast-container"]'))
        tree=html.fromstring(self.client.get(response.location,headers=self.headers).data)
        self.assertFalse(tree.xpath('//*[@id="reading-save-success"]'))
    def test_edit_success_flash_keeps_existing_non_capture_behavior(self):
        data=self.capture_form(self.readings_url+'?edit=1');data.pop('csrf_token');data['reading_value']='124'
        response=self.capture_post(self.readings_url,**data);self.assertEqual(response.status_code,302,response.text)
        tree=html.fromstring(self.client.get(response.location,headers=self.headers).data)
        self.assertFalse(tree.xpath('//*[@id="reading-save-success"]'))
        self.assertIn('Meter reading saved.',tree.xpath('//*[@id="flash-toast-container"]')[0].text_content())
    def test_browser_two_captures_restore_scroll_keep_fields_visible_and_align_success(self):
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
                self.end_headers()
                if response.mimetype=='text/html':
                    self.wfile.write(b'<style>#reading-page{margin-top:900px;margin-bottom:900px}#reading-capture-heading{display:flex;align-items:center;justify-content:space-between}#reading-capture-heading h2,#reading-capture-heading p{margin:0}</style>')
                self.wfile.write(response.data);response.close()
        server=HTTPServer(('127.0.0.1',0),Handler);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(timeout=2)))
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(viewport={'width':1280,'height':900});page.goto('http://127.0.0.1:'+str(server.server_port)+self.readings_url+'?workspace=capture')
                page.locator('#reading_date').fill('2025-09-30')
                page.evaluate("window.scrollTo(0,document.getElementById('reading_date').getBoundingClientRect().top+window.scrollY-110)")
                for meter,value in [('98','100'),('99','200')]:
                    page.select_option('#meter_id',meter);page.locator('#reading_value').fill(value)
                    scroll=page.evaluate('window.scrollY');self.assertGreater(scroll,500)
                    page.get_by_role('button',name='Save',exact=True).click()
                    page.locator('#reading-save-success').wait_for(state='visible')
                    page.wait_for_function('(expected)=>Math.abs(window.scrollY-expected)<3',arg=scroll)
                    self.assertTrue(page.locator('#reading-capture').is_visible());self.assertFalse(page.locator('#reading-register').is_visible())
                    self.assertEqual(page.locator('#reading_date').input_value(),'2025-09-30');self.assertEqual(page.locator('#meter_id').input_value(),'');self.assertEqual(page.locator('#reading_value').input_value(),'')
                    for field in ('#reading_date','#meter_id','#reading_value'):
                        bounds=page.locator(field).bounding_box();self.assertGreaterEqual(bounds['y'],0);self.assertLess(bounds['y']+bounds['height'],900)
                    title=page.locator('#reading-capture-title').bounding_box();badge=page.locator('#reading-save-success').bounding_box()
                    self.assertGreaterEqual(badge['x'],title['x']+title['width'])
                    self.assertLess(abs((badge['y']+badge['height']/2)-(title['y']+title['height']/2)),3)
                    self.assertEqual(page.locator('#flash-toast-container').count(),0)
                    self.assertEqual(page.evaluate('document.activeElement.id'),'meter_id')
                self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_meter_reading WHERE meter_id IN (98,99)')).scalar(),2)
            finally:browser.close()

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(CaptureSuccessTests(name) for name in CaptureSuccessTests.__dict__ if name.startswith('test_'))
