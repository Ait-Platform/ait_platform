"""Real local browser native printing; no server PDF renderer or production data."""
import json
from pathlib import Path
import tempfile
import unittest
from urllib.parse import parse_qs,urlsplit
import fitz
from lxml import html
from playwright.sync_api import sync_playwright
from sqlalchemy import text
from unittest.mock import patch
import test_pm_central_utilities_output as fixture

ROOT=Path(__file__).resolve().parents[1]

class NativeMetsoaSaveTests(unittest.TestCase):
    def test_july_and_october_native_save_layout_totals_and_no_pdf_request(self):
        owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records')
        owner.setUp();self.addCleanup(owner.doCleanups)
        fx=owner.fx
        # Full July fixture readings, equivalent to existing September/October
        # fixture intervals. Only session-temporary local PostgreSQL records.
        fx.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) SELECT meter_id,(reading_date - interval '3 months')::date,reading_value FROM bil_meter_reading WHERE reading_date >= '2026-09-01' AND reading_date < '2026-11-01'"))
        fx.session.commit();fx.conn.commit()
        pages={};totals={};cells={}
        for month in ['2026-07','2026-10']:
            response=fx.client.get(owner.url,query_string=dict(source='metsoa',report='metsoa_statement',recipient_id='11',preview='1',by_month=month,billing_month=month),headers=fx.headers)
            self.assertEqual(response.status_code,200,response.text)
            pages[month]=response.text
            tree=html.fromstring(response.data)
            cells[month]=[' '.join(e.text_content().split()) for e in tree.xpath('//*[@id="utilities-report"]//td') if e.text_content().strip()]
            totals[month]=' '.join(tree.xpath('//*[@id="utilities-report"]//*[contains(@class,"total")]')[0].text_content().split())
        out=ROOT/'output/pdf/native-metsoa-save-fixed';out.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='native-print-',dir=ROOT/'tmp') as profile:
            # Test-only browser preferences choose Save as PDF explicitly. The
            # application does not change printer settings or use kiosk mode.
            state={'recentDestinations':[{'id':'Save as PDF','origin':'local','account':''}],'selectedDestinationId':'Save as PDF','version':2,'isHeaderFooterEnabled':False,'mediaSize':{'width_microns':210000,'height_microns':297000,'custom_display_name':'A4'}}
            save_dir=Path(profile).resolve()/'pdfs';save_dir.mkdir()
            default=Path(profile)/'Default';default.mkdir()
            (default/'Preferences').write_text(json.dumps({'printing':{'print_preview_sticky_settings':{'appState':json.dumps(state)}},'savefile':{'default_directory':str(save_dir)}}))
            with sync_playwright() as runtime:
                context=runtime.chromium.launch_persistent_context(str(Path(profile).resolve()),headless=False,args=['--kiosk-printing'])
                try:
                    page=context.new_page();pdf_requests=[]
                    def serve(route):
                        parts=urlsplit(route.request.url);q=parse_qs(parts.query)
                        if q.get('format')==['pdf']:
                            pdf_requests.append(True);route.abort();return
                        if parts.path.startswith('/static/js/'):
                            route.fulfill(body=(ROOT/('app'+parts.path)).read_text(encoding='utf-8'),content_type='text/javascript');return
                        route.fulfill(body=pages[q.get('by_month',['2026-07'])[0]],content_type='text/html')
                    page.route('**/*',serve)
                    # Server rendering must never be called by native Save.
                    with patch.object(owner.output,'render_pdf',side_effect=AssertionError('Server PDF invoked')) as renderer:
                        page.goto('https://ait.test'+owner.url+'?by_month=2026-07')
                        for month,name in [('2026-07','310726_METSOA.pdf'),('2026-10','311026_METSOA.pdf')]:
                            if month=='2026-10':
                                page.locator('#output-month').fill(month);page.locator('#output-month').dispatch_event('change')
                                self.assertTrue(page.locator('#output-save').is_disabled())
                                with page.expect_navigation():page.locator('button[name="preview"]').click()
                            original=page.locator('#utilities-report').inner_html()
                            before={p.name for p in save_dir.glob('*.pdf')}
                            # Test the exact print-only CSS before native printing.
                            page.emulate_media(media='print')
                            page.set_viewport_size({'width':794,'height':1123})
                            page.evaluate("document.documentElement.dataset.metsoaNativeSave='' ")
                            overflow=page.evaluate('''()=>{const errors=[];for(const td of document.querySelectorAll('.mar-metsoa .details th,.mar-metsoa .details td')){const bounds=td.getBoundingClientRect(),walker=document.createTreeWalker(td,NodeFilter.SHOW_TEXT);let node;while(node=walker.nextNode()){if(!node.textContent.trim())continue;const range=document.createRange();range.selectNodeContents(node);for(const r of range.getClientRects())if(r.left<bounds.left-1 || r.right>bounds.right+1)errors.push(node.textContent);}}return errors;}''')
                            self.assertEqual(overflow,[],'Text crosses table cell boundaries')
                            page.evaluate("delete document.documentElement.dataset.metsoaNativeSave")
                            page.emulate_media(media='null')
                            page.locator('#output-save').click()
                            for _ in range(40):
                                files=[p for p in save_dir.glob('*.pdf') if p.name not in before]
                                if files and all(p.stat().st_size for p in files):break
                                page.wait_for_timeout(250)
                            self.assertEqual(len(files),1)
                            self.assertEqual(files[0].name,name)
                            (out/name).write_bytes(files[0].read_bytes())
                            self.assertEqual(page.locator('#utilities-report').inner_html(),original)
                            doc=fitz.open(files[0]);all_text=' '.join(' '.join(p.get_text().split()) for p in doc)
                            self.assertIn(month+'-31',all_text)
                            self.assertNotIn('Back to Utilities',all_text)
                            self.assertNotIn('Choose Save as PDF',all_text)
                            self.assertIn(totals[month],all_text)
                            for cell in cells[month]:self.assertIn(''.join(cell.split()),''.join(all_text.split()))
                            for i,p in enumerate(doc):
                                self.assertAlmostEqual(p.rect.width,595,delta=1)
                                self.assertAlmostEqual(p.rect.height,842,delta=1)
                                self.assertTrue(p.get_text().strip(),'Blank print page')
                                for word in p.get_text('words'):
                                    self.assertGreaterEqual(word[0],0);self.assertLessEqual(word[2],p.rect.width+1)
                                p.get_pixmap(matrix=fitz.Matrix(1,1)).save(str(out/(month+'-'+str(i+1)+'.png')))
                            print(json.dumps({'month':month,'native_filename':files[0].name,'pages':len(doc),'cells_verified':len(cells[month]),'total':totals[month]}))
                            doc.close()
                        renderer.assert_not_called()
                    self.assertEqual(pdf_requests,[])
                finally:context.close()

if __name__=='__main__':unittest.main()
