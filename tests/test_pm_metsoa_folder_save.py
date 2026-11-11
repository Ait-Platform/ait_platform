"""Complete METSOA HTTP -> genuine PDF -> persisted local directory workflow."""
import base64
from io import BytesIO
from pathlib import Path
import threading
import unittest
from urllib.parse import parse_qs,urlsplit
from unittest.mock import patch
import fitz
from playwright.sync_api import sync_playwright
from pypdf import PdfReader
from sqlalchemy import text
from werkzeug.serving import make_server, WSGIRequestHandler
import test_pm_central_utilities_output as fixture

ROOT=Path(__file__).resolve().parents[1]

class QuietHandler(WSGIRequestHandler):
    def log(self,kind,message,*args):
        pass  # Never print signed preview URLs from the local server.

class LocalFolderMetsoaSaveTests(unittest.TestCase):
    def test_july_october_actual_endpoint_pdf_and_registered_folder(self):
        owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records')
        owner.setUp();self.addCleanup(owner.doCleanups)
        fx=owner.fx
        fx.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) SELECT meter_id,(reading_date - interval '3 months')::date,reading_value FROM bil_meter_reading WHERE reading_date >= '2026-09-01' AND reading_date < '2026-11-01'"))
        fx.session.commit();fx.conn.commit()
        server=make_server('127.0.0.1',0,fx.app,request_handler=QuietHandler)
        worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),worker.join(timeout=10)))
        base='http://127.0.0.1:'+str(server.server_port)
        out=ROOT/'output/pdf/restored-metsoa-folder-save';out.mkdir(parents=True,exist_ok=True)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(extra_http_headers=fx.headers)
                page.add_init_script("""window.print=()=>{window.unexpectedPrint=true;throw new Error('Save must not print');};window.showDirectoryPicker=()=>{window.unexpectedPicker=true;throw new Error('Folder already registered');};""")
                pdf_responses=[]
                def response_received(response):
                    if parse_qs(urlsplit(response.url).query).get('format')==['pdf']:
                        pdf_responses.append(response.status)
                page.on('response',response_received)
                page.goto(base+owner.url+'?source=metsoa&report=metsoa_statement&recipient_id=11&by_month=2026-07&preview=1')
                page.evaluate("""async()=>{const scope=JSON.parse(document.getElementById('output-directory-context').textContent);const root=await navigator.storage.getDirectory();await AITDirectories.set(scope,await root.getDirectoryHandle('PreviouslySelected',{create:true}));}""")
                page.reload()  # The handle must survive reload via IndexedDB.
                for month,name in [('2026-07','310726_METSOA.pdf'),('2026-10','311026_METSOA.pdf')]:
                    if month=='2026-10':
                        page.locator('#output-month').fill(month);page.locator('#output-month').dispatch_event('change')
                        self.assertTrue(page.locator('#output-save').is_disabled())
                        with page.expect_navigation():page.locator('button[name="preview"]').click()
                    before=page.locator('#utilities-report').inner_html()
                    cells=page.locator('#utilities-report td').all_text_contents()
                    # Save uses the actual endpoint and renderer, not a fixture blob.
                    page.locator('#output-save').click()
                    page.wait_for_function("name=>document.getElementById('output-save-message').textContent.includes('Saved '+name+' to your registered folder.')",arg=name,timeout=30000)
                    self.assertEqual(page.locator('#utilities-report').inner_html(),before)
                    data=page.evaluate("""async name=>{const scope=JSON.parse(document.getElementById('output-directory-context').textContent),row=await AITDirectories.get(scope),f=await(await row.handle.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement'),file=await(await f.getFileHandle(name)).getFile();const bytes=new Uint8Array(await file.arrayBuffer());let raw='';for(const b of bytes)raw+=String.fromCharCode(b);return {root:row.handle.name,base64:btoa(raw),type:file.type};}""",name)
                    self.assertEqual(data['root'],'PreviouslySelected')
                    payload=base64.b64decode(data['base64']);self.assertTrue(payload.startswith(b'%PDF-'))
                    reader=PdfReader(BytesIO(payload));self.assertGreater(len(reader.pages),0)
                    all_text=''.join(''.join((p.extract_text() or '').split()) for p in reader.pages)
                    self.assertIn(month+'-31',all_text)
                    for cell in cells:
                        if cell.strip():self.assertIn(''.join(cell.split()),all_text)
                    (out/name).write_bytes(payload)
                    doc=fitz.open(stream=payload,filetype='pdf')
                    for i,p in enumerate(doc):p.get_pixmap(matrix=fitz.Matrix(1,1)).save(str(out/(month+'-'+str(i+1)+'.png')))
                    doc.close()
                    print(month+': saved '+name+'; genuine PDF; '+str(len(reader.pages))+' pages; all table cells retained')
                    self.assertFalse(page.evaluate('!!window.unexpectedPrint'))
                    self.assertFalse(page.evaluate('!!window.unexpectedPicker'))
                self.assertEqual(pdf_responses,[200,200])
                # A collision must create (1), never replace the original file.
                page.wait_for_function("!document.getElementById('output-save').disabled",timeout=15000)
                page.locator('#output-save').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.includes('Saved 311026_METSOA(1).pdf')",timeout=30000)
                self.assertEqual(pdf_responses,[200,200,200])
                names=page.evaluate("""async()=>{const row=await AITDirectories.get(JSON.parse(document.getElementById('output-directory-context').textContent)),f=await(await row.handle.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement'),names=[];for await(const [n] of f.entries())names.push(n);return names;}""")
                self.assertEqual(set(names),{'310726_METSOA.pdf','311026_METSOA.pdf','311026_METSOA(1).pdf'})
            finally:browser.close()

if __name__=='__main__':unittest.main()
