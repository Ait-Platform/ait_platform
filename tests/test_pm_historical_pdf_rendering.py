"""Actual PDF pixels and original object-URL lifetime, independent of PDF plug-ins."""
import base64
from pathlib import Path
import unittest
import fitz
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser


class HistoricalPDFRenderingTests(unittest.TestCase):
    def test_pages_render_with_spaces_and_urls_survive_until_back_or_close(self):
        pdf=fitz.open()
        for index,color in enumerate([(1,0,0),(0,0,1)],1):
            page=pdf.new_page(width=400,height=500)
            page.draw_rect(fitz.Rect(40,40,160,160),color=color,fill=color)
            page.insert_text((40,200),'Original historical page '+str(index))
        original=pdf.tobytes();pdf.close()
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async encoded=>{const folder=await navigator.storage.getDirectory(),report=await(await folder.getDirectoryHandle('METSOA',{create:true})).getDirectoryHandle('METSOA Statement',{create:true});
                    const file=await report.getFileHandle('September 2026 original statement.pdf',{create:true}),writer=await file.createWritable();
                    await writer.write(Uint8Array.from(atob(encoded),c=>c.charCodeAt(0)));await writer.close();await AITDirectories.set({user:'7',property:'27'},folder);}""",base64.b64encode(original).decode())
                page.goto('https://ait.test/directory-register?property_id=27')
                page.locator('#folder-register-open').click()
                page.get_by_role('button',name='METSOA / METSOA Statement',exact=True).click()
                for close in ['#folder-view-back','#folder-register-close']:
                    page.get_by_role('button',name='September 2026 original statement.pdf',exact=True).click()
                    page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('2 page(s)')")
                    canvases=page.locator('#folder-view-pages canvas')
                    self.assertEqual(canvases.count(),2)
                    self.assertEqual(canvases.evaluate_all("nodes=>nodes.map(c=>c.getAttribute('aria-label'))"),['PDF page 1 of 2','PDF page 2 of 2'])
                    # Prove the actual saved page content was painted, not a placeholder.
                    colors=canvases.evaluate_all("""nodes=>nodes.map(c=>Array.from(c.getContext('2d').getImageData(Math.floor(c.width/4),Math.floor(c.height/5),1,1).data))""")
                    self.assertEqual(colors,[[255,0,0,255],[0,0,255,255]])
                    self.assertIsNone(page.locator('#folder-view-pdf').get_attribute('src'))
                    url=page.locator('#folder-view-pages').get_attribute('data-object-url')
                    self.assertTrue(url.startswith('blob:'))
                    data=page.evaluate("async url=>Array.from(new Uint8Array(await(await fetch(url)).arrayBuffer()))",url)
                    self.assertEqual(bytes(data),original)
                    page.locator('#folder-view-pages').screenshot(path='tmp/pm-historical-pdf-rendered.png')
                    page.locator(close).click()
                    self.assertEqual(canvases.count(),0)
                    self.assertIsNone(page.locator('#folder-view-pages').get_attribute('data-object-url'))
                    self.assertTrue(page.evaluate("async url=>{try{await fetch(url);return false;}catch(_){return true;}}",url))
                saved=page.evaluate("""async()=>{const root=(await AITDirectories.get({user:'7',property:'27'})).handle,file=await(await(await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement')).getFileHandle('September 2026 original statement.pdf');return Array.from(new Uint8Array(await(await file.getFile()).arrayBuffer()));}""")
                self.assertEqual(bytes(saved),original)
            finally:browser.close()

    def test_close_while_original_bytes_are_pending_does_not_reopen_viewer(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory(),folder=await(await root.getDirectoryHandle('METSOA',{create:true})).getDirectoryHandle('METSOA Statement',{create:true});
                    const handle=await folder.getFileHandle('Pending file.pdf',{create:true}),w=await handle.createWritable();await w.write('%PDF-pending');await w.close();await AITDirectories.set({user:'7',property:'26'},root);}""")
                page.locator('#folder-register-open').click();page.get_by_role('button',name='METSOA / METSOA Statement',exact=True).click()
                page.get_by_role('button',name='Pending file.pdf',exact=True).wait_for()
                page.evaluate("""()=>{const original=FileSystemFileHandle.prototype.getFile;FileSystemFileHandle.prototype.getFile=function(){return original.call(this).then(file=>new Promise(resolve=>window.finishPDF=()=>resolve(file)));};}""")
                page.get_by_role('button',name='Pending file.pdf',exact=True).click()
                page.wait_for_function("typeof window.finishPDF==='function'")
                page.locator('#folder-register-close').click();page.evaluate('finishPDF()')
                self.assertTrue(page.locator('#folder-register-area').is_hidden())
                self.assertEqual(page.locator('#folder-view-pages canvas').count(),0)
                self.assertIsNone(page.locator('#folder-view-pages').get_attribute('data-object-url'))
            finally:browser.close()
