"""Isolated printing of original saved PDFs and authenticated email defaults."""
import base64
import unittest
import fitz
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser,browser_html

PRINT_SPY="""window.printCalls=[];
if(window===window.top){window.print=()=>{throw Error('Must not print the PM webpage');};}
else{window.print=()=>{const canvases=Array.from(document.querySelectorAll('canvas'));
    window.parent.printCalls.push({title:document.title,count:canvases.length,children:Array.from(document.body.children).map(n=>n.tagName),
        colors:canvases.map(c=>Array.from(c.getContext('2d').getImageData(Math.floor(c.width/4),Math.floor(c.height/5),1,1).data))});};}"""

class PMPDFPrintTests(unittest.TestCase):
    def open_history(self,runtime,count=1,email=''):
        pdf=fitz.open()
        for color in [(1,0,0),(0,1,0),(0,0,1)][:count]:
            page=pdf.new_page(width=400,height=500);page.draw_rect(fitz.Rect(40,40,160,160),color=color,fill=color);page.insert_text((40,200),'Original September historical figures')
        original=pdf.tobytes();pdf.close()
        browser,page=open_browser(runtime,markup=browser_html(27,email_to=email),init=PRINT_SPY)
        page.evaluate("""async encoded=>{const root=await navigator.storage.getDirectory(),folder=await(await root.getDirectoryHandle('METSOA',{create:true})).getDirectoryHandle('METSOA Statement',{create:true});
            const file=await folder.getFileHandle('September original report.pdf',{create:true}),writer=await file.createWritable();await writer.write(Uint8Array.from(atob(encoded),c=>c.charCodeAt(0)));await writer.close();await AITDirectories.set({user:'7',property:'27'},root);}""",base64.b64encode(original).decode())
        page.locator('#folder-register-open').click();page.get_by_role('button',name='METSOA / METSOA Statement',exact=True).click();page.get_by_role('button',name='September original report.pdf',exact=True).click()
        page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('page(s)')")
        return browser,page,original

    def test_single_and_multi_page_print_only_original_pages_and_cleanup_after_dialog(self):
        with sync_playwright() as runtime:
            for count in [1,3]:
                with self.subTest(pages=count):
                    browser,page,original=self.open_history(runtime,count)
                    try:
                        viewer_url=page.locator('#folder-view-pages').get_attribute('data-object-url')
                        page.locator('#folder-view-print').click();page.wait_for_function('printCalls.length===1')
                        printed=page.evaluate('printCalls[0]')
                        self.assertEqual(printed,dict(title='September original report.pdf',count=count,children=['CANVAS']*count,colors=[[255,0,0,255],[0,255,0,255],[0,0,255,255]][:count]))
                        self.assertTrue(page.locator('#folder-view-print').is_disabled())
                        page.evaluate("document.getElementById('folder-view-print').onclick()")
                        self.assertEqual(page.evaluate('printCalls.length'),1)
                        page.evaluate("document.getElementById('folder-view-pdf').contentWindow.dispatchEvent(new Event('afterprint'))")
                        page.wait_for_function("!document.getElementById('folder-view-print').disabled")
                        self.assertIsNone(page.locator('#folder-view-pdf').get_attribute('srcdoc'))
                        self.assertEqual(page.locator('#folder-view-pages canvas').count(),count)
                        self.assertEqual(page.locator('#folder-view-pages').get_attribute('data-object-url'),viewer_url)
                        viewed=page.evaluate('async url=>Array.from(new Uint8Array(await(await fetch(url)).arrayBuffer()))',viewer_url)
                        self.assertEqual(bytes(viewed),original)
                        saved=page.evaluate("""async()=>{const root=(await AITDirectories.get({user:'7',property:'27'})).handle,handle=await(await(await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement')).getFileHandle('September original report.pdf');return Array.from(new Uint8Array(await(await handle.getFile()).arrayBuffer()));}""")
                        self.assertEqual(bytes(saved),original)
                    finally:browser.close()

    def test_email_defaults_to_registered_user_and_remains_editable(self):
        with sync_playwright() as runtime:
            browser,page,original=self.open_history(runtime,email='registered.user@example.com')
            try:
                page.locator('#folder-view-email').click()
                self.assertEqual(page.locator('#folder-email-to').input_value(),'registered.user@example.com')
                page.locator('#folder-email-to').fill('chosen.recipient@example.com')
                page.locator('#folder-view-email').click()
                self.assertEqual(page.locator('#folder-email-to').input_value(),'chosen.recipient@example.com')
                sent=[]
                def deliver(route):sent.append(route.request.post_data_buffer);route.fulfill(json=dict(message='Accepted original PDF'))
                page.route('**/billing_bp.directory_email',deliver)
                page.locator('#folder-view-email-form button').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent==='Accepted original PDF'")
                self.assertEqual(len(sent),1);self.assertIn(b'chosen.recipient@example.com',sent[0]);self.assertIn(original,sent[0])
            finally:browser.close()

    def test_close_cancels_pending_print_without_affecting_saved_document(self):
        with sync_playwright() as runtime:
            browser,page,original=self.open_history(runtime)
            try:
                page.evaluate("""()=>{const read=Blob.prototype.arrayBuffer;Blob.prototype.arrayBuffer=function(){return read.call(this).then(bytes=>new Promise(resolve=>window.finishPrintRead=()=>resolve(bytes)));};}""")
                page.locator('#folder-view-print').click();page.wait_for_function("typeof finishPrintRead==='function'")
                page.locator('#folder-register-close').click();page.evaluate('finishPrintRead()')
                self.assertEqual(page.evaluate('printCalls'),[])
                self.assertTrue(page.locator('#folder-register-area').is_hidden())
                self.assertFalse(page.locator('#folder-view-print').is_disabled())
            finally:browser.close()

    def test_parse_failure_does_not_print_webpage_or_disable_future_printing(self):
        with sync_playwright() as runtime:
            browser,page,_=self.open_history(runtime)
            try:
                page.evaluate("""()=>AITPMPdfPrint.print(new Blob(['%PDF-invalid']),'Invalid.pdf',document.getElementById('folder-view-pages').dataset,text=>document.getElementById('folder-register-message').textContent=text)""")
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('could not be prepared')")
                self.assertEqual(page.evaluate('printCalls'),[])
                self.assertFalse(page.locator('#folder-view-print').is_disabled())
                self.assertEqual(page.locator('#folder-view-pages canvas').count(),1)
            finally:browser.close()
