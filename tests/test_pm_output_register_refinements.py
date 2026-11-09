"""Output toolbar, filtered arrangement totals and shared report typography."""
import os
from pathlib import Path
from io import BytesIO
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
from pypdf import PdfReader
import test_pm_central_utilities_output as fixture
from test_pm_directory_register import open_browser


class OutputRegisterRefinementTests(unittest.TestCase):
    def setUp(self):
        self.owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records');self.owner.setUp();self.addCleanup(self.owner.doCleanups)
        self.fx=self.owner.fx;self.output=self.owner.output
        self.fx.session.execute(text("INSERT INTO bil_municipal_arrangement VALUES(2,26,2,'CA0815202400000098','2026-08-01',500,50,'2026-08-01')"))
        self.fx.session.commit();self.fx.conn.commit()

    def test_single_multiple_filtered_totals_and_existing_pdf_email_paths(self):
        for item,dates,expected in [('1',{},['R1,000.00','R100.00','R800.00']),('',{},['R1,500.00','R150.00','R1,150.00']),('',dict(date_from='2026-09-01',date_to='2026-10-31'),['R1,000.00','R100.00','R800.00'])]:
            with self.subTest(item=item,dates=dates):
                response=self.owner.get('arrangements','arrangement_register',item_id=item,**dates);self.assertEqual(response.status_code,200,response.text)
                tree=html.fromstring(response.data)
                self.assertEqual(tree.xpath('//*[@id="output-arrangement-register"]/tfoot/tr/th/strong/text()'),['TOTAL'])
                self.assertEqual(tree.xpath('//*[@id="output-arrangement-register"]/tfoot/tr/td/strong/text()'),expected)
                fields=self.owner.fields(response)
                download=self.fx.client.get(self.owner.url,query_string=dict(fields,format='pdf',download='1'),headers=self.fx.headers)
                self.assertEqual(download.status_code,200)
                content=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(download.data)).pages)
                for value in expected:self.assertIn(value,content)
                with patch.object(self.output,'email_pdf',return_value=True) as sender:
                    sent=self.fx.client.post(self.owner.url,data=dict(fields,action='email',email_to='manager@example.com'),headers=self.fx.headers)
                    self.assertEqual(sent.status_code,302,sent.text)
                    attachment=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(sender.call_args.args[2])).pages)
                    for value in expected:self.assertIn(value,attachment)

    def test_desktop_menu_and_tables_fit_preview_print_and_pdf(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            for source,report,table in [('arrangements','arrangement_register','output-arrangement-register'),('reconciliation','utilities_reconciliation','metsoa-comparison')]:
                response=self.owner.get(source,report,recipient_id='11');self.assertEqual(response.status_code,200,response.text)
                browser,page=open_browser(runtime,markup=response.text)
                try:
                    page.set_viewport_size(dict(width=900,height=1000))
                    controls=page.locator('#output-actions').locator('button,a')
                    self.assertEqual(controls.all_text_contents(),['Print','Email','Save Report','Folders'])
                    boxes=controls.evaluate_all('(els)=>els.map(e=>e.getBoundingClientRect().top)');self.assertEqual(len(set(boxes)),1)
                    self.assertTrue(page.locator('#output-download').is_hidden())
                    self.assertIn('property_id=26',page.locator('#output-folders').get_attribute('href'))
                    for medium in ['screen','print']:
                        page.emulate_media(media=medium)
                        result=page.evaluate("""id=>{
                            const t=document.getElementById(id),cells=[...t.querySelectorAll('tbody .money,tfoot .money')];
                            return {overflow:t.scrollWidth>t.clientWidth+1,fonts:cells.map(c=>getComputedStyle(c).fontSize),padding:cells.map(c=>getComputedStyle(c).padding),align:cells.map(c=>getComputedStyle(c).textAlign)};
                        }""",table)
                        self.assertFalse(result['overflow'])
                        self.assertTrue(all(v=='12px' for v in result['fonts']))
                        self.assertTrue(all(v=='6px' for v in result['padding']))
                        self.assertTrue(all(v=='right' for v in result['align']))
                finally:browser.close()
                with self.fx.app.test_request_context():
                    data=self.output.report(26,dict(source=source,report=report,preview='1',billing_month='2026-10',recipient_id='11'))
                    pdf=self.output.render_pdf(data)
                self.assertEqual(len(PdfReader(BytesIO(pdf)).pages),1)
                if os.environ.get('PM_OUTPUT_REGISTER_QA'):
                    path=Path(os.environ['PM_OUTPUT_REGISTER_QA']);path.mkdir(parents=True,exist_ok=True);(path/(report+'.pdf')).write_bytes(pdf)

    def test_folder_error_reveals_download_fallback_and_changed_selection_hides_it(self):
        from playwright.sync_api import sync_playwright
        response=self.owner.get('readings','history')
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,markup=response.text)
            try:
                page.evaluate("()=>{AITDirectories.save=async()=>{throw new Error('Folder permission was not granted.');};}")
                page.locator('#output-save').click()
                page.wait_for_function("!document.getElementById('output-download').hidden")
                self.assertTrue(page.locator('#output-download').is_visible())
                self.assertIn('format=pdf',page.locator('#output-download').get_attribute('href'))
                page.select_option('#output-source','reconciliation')
                self.assertTrue(page.locator('#output-download').is_hidden())
                self.assertIsNone(page.locator('#output-download').get_attribute('href'))
            finally:browser.close()


if __name__=='__main__':unittest.main()
