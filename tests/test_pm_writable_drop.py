"""Approved drop-to-authorise PM path. OS drop/permission are simulated here."""
from pathlib import Path
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser, REPORTS


class PMWritableDropTests(unittest.TestCase):
    def drop(self,page,expression):
        page.evaluate("""async expression=>{
            const handle=await (new Function('return (async()=>('+expression+'))()'))();
            const event={preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve(handle)}]}};
            await document.getElementById('directory-select').ondrop(event);
        }""",expression)

    def test_dale_approval_creates_original_structure_and_saves_without_touching_almond(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    const almond=await root.getDirectoryHandle('Almond original',{create:true});
                    const f=await almond.getFileHandle('Existing.pdf',{create:true}),w=await f.createWritable();
                    await w.write('%PDF-Almond history');await w.close();
                    await AITDirectories.set({user:'7',property:'26'},almond);
                    window.showDirectoryPicker=()=>{throw Error('Forbidden picker');};}""")
                page.goto('https://ait.test/directory-register?property_id=27')
                self.drop(page,"(await navigator.storage.getDirectory()).getDirectoryHandle('Dale destination',{create:true})")
                self.assertIn('not yet assigned',page.locator('#directory-message').inner_text())
                self.assertFalse(page.evaluate("AITDirectories.get({user:'7',property:'27'})"))
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await(await navigator.storage.getDirectory()).getDirectoryHandle('Dale destination')).keys())"),[])
                page.evaluate("""()=>{window.permissionClicks=[];FileSystemDirectoryHandle.prototype.queryPermission=async()=>window.writeGranted?'granted':'prompt';
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=>{permissionClicks.push(navigator.userActivation.isActive);window.writeGranted=true;return 'granted';};}""")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('Writable folder assigned')")
                self.assertEqual(page.evaluate('permissionClicks'),[True])
                expected=sorted({r['source']+'/'+r['title'] for r in REPORTS})
                structure=page.evaluate("""async()=>{const root=(await AITDirectories.get({user:'7',property:'27'})).handle,paths=[];
                    for await(const [name,category] of root.entries())for await(const [title] of category.entries())paths.push(name+'/'+title);
                    return paths.sort();}""")
                self.assertEqual(structure,expected)
                page.evaluate("window.fetch=async()=>new Response('%PDF-Dale report',{headers:{'Content-Type':'application/pdf'}})")
                result=page.evaluate("""async()=>{const scope={user:'7',property:'27',period_end:'2026-07-31'};
                    const file=await AITDirectories.save(scope,'metsoa_statement','/test-report.pdf');
                    const root=(await AITDirectories.get(scope)).handle;
                    const handle=await(await(await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement')).getFileHandle(file);
                    const almond=(await AITDirectories.get({user:'7',property:'26'})).handle;
                    return {root:root.name,file,text:await(await handle.getFile()).text(),almond:almond.name,old:await(await(await almond.getFileHandle('Existing.pdf')).getFile()).text()};
                }""")
                self.assertEqual(result,dict(root='Dale destination',file='310726_METSOA.pdf',text='%PDF-Dale report',almond='Almond original',old='%PDF-Almond history'))
                page.reload()
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Dale destination'")
                self.assertTrue(page.locator('#directory-select').is_hidden())
            finally:browser.close()

    def test_rejects_same_nested_and_parent_property_folders(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory(),almond=await root.getDirectoryHandle('Almond',{create:true});
                    await almond.getDirectoryHandle('Nested',{create:true});await AITDirectories.set({user:'7',property:'26'},almond);}""")
                page.goto('https://ait.test/directory-register?property_id=27')
                for expression in ["(await navigator.storage.getDirectory()).getDirectoryHandle('Almond')","(await(await navigator.storage.getDirectory()).getDirectoryHandle('Almond')).getDirectoryHandle('Nested')","navigator.storage.getDirectory()"]:
                    with self.subTest(expression=expression):
                        self.drop(page,expression)
                        self.assertIn('overlaps',page.locator('#directory-message').inner_text())
                        self.assertFalse(page.evaluate("AITDirectories.get({user:'7',property:'27'})"))
            finally:browser.close()

    def test_denial_creates_no_subfolders_and_no_assignment(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.goto('https://ait.test/directory-register?property_id=27')
                self.drop(page,"(await navigator.storage.getDirectory()).getDirectoryHandle('Denied Dale',{create:true})")
                page.evaluate("""()=>{FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';}""")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('not granted')")
                self.assertFalse(page.evaluate("AITDirectories.get({user:'7',property:'27'})"))
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await(await navigator.storage.getDirectory()).getDirectoryHandle('Denied Dale')).keys())"),[])
            finally:browser.close()

    def test_html_selection_is_still_not_a_writable_assignment(self):
        source=(Path(__file__).resolve().parents[1]/'app/static/js/pm_folder_name.js').read_text()
        html_handler=source[source.index('input.onchange='):source.index('input.oncancel=')]
        self.assertNotIn('api.set',html_handler)
        self.assertNotIn('requestPermission',html_handler)
        self.assertNotIn('showDirectoryPicker',source)

    def test_existing_assignment_cannot_be_replaced_by_drop(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    await AITDirectories.set({user:'7',property:'27'},await root.getDirectoryHandle('Original Dale',{create:true}));}""")
                page.goto('https://ait.test/directory-register?property_id=27')
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Original Dale'")
                self.assertTrue(page.locator('#directory-select').is_hidden())
                self.drop(page,"(await navigator.storage.getDirectory()).getDirectoryHandle('New Dale',{create:true})")
                page.evaluate("document.getElementById('directory-select').onclick()")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Original Dale')
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'27'})).handle.name"),'Original Dale')
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await(await navigator.storage.getDirectory()).getDirectoryHandle('New Dale')).keys())"),[])
            finally:browser.close()

    def test_assignment_created_in_another_tab_is_preserved_before_folder_creation(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.goto('https://ait.test/directory-register?property_id=27')
                self.drop(page,"(await navigator.storage.getDirectory()).getDirectoryHandle('Candidate Dale',{create:true})")
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    await AITDirectories.set({user:'7',property:'27'},await root.getDirectoryHandle('Assigned elsewhere',{create:true}));}""")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-select').hidden")
                self.assertIn('preserved',page.locator('#directory-message').inner_text())
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Assigned elsewhere')
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await(await navigator.storage.getDirectory()).getDirectoryHandle('Candidate Dale')).keys())"),[])
            finally:browser.close()
