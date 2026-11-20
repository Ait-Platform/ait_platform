"""Independent new PM module tests. Native Windows selection remains unverified."""
from pathlib import Path
import unittest
from playwright.sync_api import sync_playwright
ROOT=Path(__file__).resolve().parents[1]
MODULE=ROOT/'app/static/js/pm_property_folders.js'
HTML="""<!doctype html><select id="property"><option>26</option><option>27</option></select>
<button id="choose">Select Folder</button><strong id="name">No folder selected</strong>
<a href="/">Back</a><p id="message"></p><script src="/pm_property_folders.js"></script>"""


class NewPMFoldersTests(unittest.TestCase):
    def browser(self,runtime,init=''):
        browser=runtime.chromium.launch(headless=True)
        page=browser.new_page()
        page.route('**/*',lambda route:route.fulfill(body=MODULE.read_text() if route.request.url.endswith('.js') else HTML,content_type='text/javascript' if route.request.url.endswith('.js') else 'text/html'))
        page.add_init_script(init or """window.showDirectoryPicker=async()=>(await navigator.storage.getDirectory()).getDirectoryHandle('Chosen folder',{create:true});""")
        page.goto('https://pm.test/')
        return browser,page

    def mount(self,page,prop=26):
        page.evaluate("""prop=>{window.scope={userId:'644',propertyId:String(prop)};
            window.ui={select:document.querySelector('#choose'),name:document.querySelector('#name'),message:document.querySelector('#message')};
            PMPropertyFolders.mount(scope,ui);}""",prop)

    def choose(self,page,name):
        page.evaluate("name=>{window.showDirectoryPicker=async()=>(await navigator.storage.getDirectory()).getDirectoryHandle(name,{create:true});}",name)
        page.locator('#choose').click()
        page.wait_for_function("document.querySelector('#message').textContent==='Folder saved.'")

    def test_two_properties_replace_reload_and_no_legacy_access(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.evaluate("""async()=>{const old=await navigator.storage.getDirectory();
                    await new Promise(resolve=>{const r=indexedDB.open('ait-pm-directories',1);
                        r.onupgradeneeded=()=>r.result.createObjectStore('assignments',{keyPath:'key'});
                        r.onsuccess=()=>{const db=r.result,tx=db.transaction('assignments','readwrite');
                            tx.objectStore('assignments').put({key:'legacy-marker',handle:old});tx.oncomplete=()=>{db.close();resolve();};};});
                    const open=indexedDB.open.bind(indexedDB);
                    indexedDB.open=(name,...args)=>{if(name==='ait-pm-directories')throw Error('New module touched legacy storage');return open(name,...args);};
                }""")
                self.mount(page,26)
                self.choose(page,'Almond first')
                self.choose(page,'Almond replacement')
                page.reload();self.mount(page,27)
                self.assertEqual(page.locator('#name').inner_text(),'No folder selected')
                self.choose(page,'Dale')
                page.reload();self.mount(page,26)
                page.wait_for_function("document.querySelector('#name').textContent==='Almond replacement'")
                self.assertEqual(page.evaluate("async()=>(await PMPropertyFolders.restore({userId:'644',propertyId:'27'})).name"),'Dale')
                self.assertFalse(page.evaluate("PMPropertyFolders.restore({userId:'645',propertyId:'26'})"))
                self.assertEqual(page.evaluate("""async()=>new Promise(resolve=>{const r=indexedDB.open('ait-pm-directories',1);
                    r.onsuccess=()=>{const db=r.result,tx=db.transaction('assignments'),q=tx.objectStore('assignments').get('legacy-marker');
                        q.onsuccess=()=>resolve(!!q.result);tx.oncomplete=()=>db.close();};})"""),True)
                self.assertFalse(page.evaluate("'AITDirectories' in window"))
            finally:browser.close()

    def test_native_abort_preserves_new_assignment(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                self.mount(page);self.choose(page,'Original')
                page.evaluate("()=>{window.showDirectoryPicker=async()=>{throw new DOMException('Native abort','AbortError');};}")
                page.locator('#choose').click()
                page.wait_for_function("document.querySelector('#message').textContent.includes('browser did not provide')")
                self.assertEqual(page.locator('#name').inner_text(),'Original')
                self.assertEqual(page.evaluate("async()=>(await PMPropertyFolders.restore(scope)).name"),'Original')
            finally:browser.close()

    def test_one_handler_and_no_prompts_on_restore(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                self.mount(page)
                page.evaluate("""()=>{PMPropertyFolders.mount(scope,ui);window.calls=0;
                    window.showDirectoryPicker=async()=>{calls++;return (await navigator.storage.getDirectory()).getDirectoryHandle('Single',{create:true});};}""")
                page.locator('#choose').click()
                page.wait_for_function("document.querySelector('#message').textContent==='Folder saved.'")
                self.assertEqual(page.evaluate('calls'),1)
                page.add_init_script("window.prompts=0;FileSystemDirectoryHandle.prototype.requestPermission=async()=>{prompts++;throw Error('No load prompts');};")
                page.reload();self.mount(page)
                page.wait_for_function("document.querySelector('#name').textContent==='Single'")
                self.assertEqual(page.evaluate('prompts'),0)
            finally:browser.close()

    def test_paths_pdf_validation_collisions_and_property_destination(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                self.mount(page);self.choose(page,'Almond')
                page.evaluate("""()=>{window.fetch=async()=>new Response('%PDF-report',{headers:{'Content-Type':'application/pdf'}});}""")
                names=page.evaluate("""async()=>{const report={source:'METSOA',title:'METSOA Statement',code:'metsoa_statement'};
                    const root=await PMPropertyFolders.restore(scope);const before=await Array.fromAsync(root.keys());
                    const a=await PMPropertyFolders.saveReport(scope,report,'/report.pdf','2026-07-31');
                    const b=await PMPropertyFolders.saveReport(scope,report,'/report.pdf','2026-07-31');
                    const folder=await(await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement');
                    return {before,a,b,text:await(await(await folder.getFileHandle(a)).getFile()).text()};
                }""")
                self.assertEqual(names,dict(before=[],a='310726_METSOA.pdf',b='310726_METSOA(1).pdf',text='%PDF-report'))
                page.evaluate("()=>{window.fetch=async()=>new Response('Not a PDF');}")
                result=page.evaluate("""async()=>{try{await PMPropertyFolders.saveReport(scope,{source:'Unused',title:'Unused',code:'unused'},'/report.pdf','2026-07-31');}
                    catch(error){return {error:error.message,folders:await Array.fromAsync((await PMPropertyFolders.restore(scope)).keys())};}}""")
                self.assertEqual(result['folders'],['METSOA'])
                self.assertIn('No file was saved',result['error'])
            finally:browser.close()

    def test_permissions_and_transaction_failure_do_not_claim_success(self):
        with sync_playwright() as runtime:
            for mode in ('permission','storage'):
                with self.subTest(mode=mode):
                    browser,page=self.browser(runtime)
                    try:
                        self.mount(page)
                        self.choose(page,'Original assignment')
                        if mode=='permission':
                            page.evaluate("()=>{FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'denied';FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';}")
                        else:
                            page.evaluate("()=>{IDBObjectStore.prototype.put=function(){throw new DOMException('Cannot store','DataCloneError');};}")
                        page.locator('#choose').click()
                        page.wait_for_function("document.querySelector('#message').textContent.includes('Could not save')")
                        self.assertEqual(page.evaluate("async()=>(await PMPropertyFolders.restore(scope)).name"),'Original assignment')
                    finally:browser.close()

    def test_production_has_no_old_or_new_picker_until_verified(self):
        template=(ROOT/'templates/program_billing/directory_register.html').read_text()
        self.assertNotIn('pm_directory_register.js',template)
        self.assertNotIn('pm_local_directories.js',template)
        self.assertNotIn('pm_property_folders.js',template)
        self.assertIn('disabled',template)
        self.assertNotIn('document_history',template)
