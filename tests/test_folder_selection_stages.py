"""Real Chromium/IndexedDB/OPFS tests. The OS picker is stubbed, not verified here."""
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser


PT = '''<html><body><span id="output-folder-name">Not selected on this browser</span>
<button id="output-folder-select">Select Folder</button><button id="output-folder-allow">Save</button>
<p id="output-save-message"></p><script id="output-directory-context" type="application/json">
{"user":"7","property":"protrade"}</script><script src="/static/js/pm_local_directories.js" defer></script>
<script src="/static/js/protrade_output.js" defer></script></body></html>'''


class FolderSelectionStagesTests(unittest.TestCase):
    def variants(self):
        return [('PM', None, '#directory-select', '#directory-save', '#directory-folder', '#directory-message', '26'),
                ('PT', PT, '#output-folder-select', '#output-folder-allow', '#output-folder-name', '#output-save-message', 'protrade')]

    def test_selection_save_refresh_and_existing_assignment_isolation(self):
        with sync_playwright() as runtime:
            for program,markup,select,save,name,message,prop in self.variants():
                with self.subTest(program=program):
                    browser,page=open_browser(runtime,markup)
                    try:
                        page.locator(select).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent==='Reports'",arg=name)
                        scope={'user':'7','property':prop}
                        self.assertFalse(page.evaluate('s=>AITDirectories.get(s)',scope))
                        self.assertIn('Click Save',page.locator(message).inner_text())
                        page.evaluate("""()=>{window.permissionGesture=false;
                            FileSystemDirectoryHandle.prototype.requestPermission=async()=>{window.permissionGesture=navigator.userActivation.isActive;return 'granted';};}""")
                        page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.startsWith('Folder saved.')",arg=message)
                        self.assertTrue(page.evaluate('permissionGesture'))
                        self.assertEqual(page.evaluate('async s=>{const h=(await AITDirectories.get(s)).handle;return Array.fromAsync(h.keys());}',scope),[])
                        self.assertFalse(page.evaluate("s=>AITDirectories.get({...s,user:'8'})",scope))
                        page.reload()
                        page.wait_for_function("s=>document.querySelector(s).textContent==='Reports'",arg=name)
                    finally:browser.close()

    def test_picker_cancellation_and_denial_without_a_previous_selection(self):
        with sync_playwright() as runtime:
            for program,markup,select,save,name,message,prop in self.variants():
                for error in ['AbortError','NotAllowedError']:
                    with self.subTest(program=program,error=error):
                        init="window.showDirectoryPicker=async()=>{throw new DOMException('Picker rejected',"+repr(error)+");};"
                        browser,page=open_browser(runtime,markup,init)
                        try:
                            page.locator(select).click()
                            page.wait_for_function("s=>document.querySelector(s).textContent.includes('Picker rejected')",arg=message)
                            self.assertTrue(page.locator(save).is_disabled())
                            self.assertFalse(page.locator(select).is_disabled())
                            self.assertFalse(page.evaluate('s=>AITDirectories.get(s)',{'user':'7','property':prop}))
                            if error=='NotAllowedError':self.assertIn('access was denied',page.locator(message).inner_text())
                        finally:browser.close()

    def test_picker_abort_is_not_claimed_to_be_user_cancellation(self):
        with sync_playwright() as runtime:
            for program,markup,select,save,name,message,prop in self.variants():
                with self.subTest(program=program):
                    browser,page=open_browser(runtime,markup)
                    try:
                        page.locator(select).click();page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.startsWith('Folder saved.')",arg=message)
                        page.evaluate("()=>{window.showDirectoryPicker=async()=>{throw new DOMException('The user aborted a request','AbortError');};}")
                        page.locator(select).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.includes('without returning a folder')",arg=message)
                        self.assertIn('browser failure',page.locator(message).inner_text())
                        self.assertEqual(page.locator(name).inner_text(),'Reports')
                        self.assertEqual(page.evaluate('async s=>(await AITDirectories.get(s)).handle.name',{'user':'7','property':prop}),'Reports')
                    finally:browser.close()

    def test_denied_write_permission_keeps_selection_for_retry(self):
        with sync_playwright() as runtime:
            for program,markup,select,save,name,message,prop in self.variants():
                with self.subTest(program=program):
                    browser,page=open_browser(runtime,markup)
                    try:
                        page.locator(select).click()
                        page.evaluate("FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied'")
                        page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.includes('Folder permission was not granted')",arg=message)
                        self.assertEqual(page.locator(name).inner_text(),'Reports')
                        self.assertFalse(page.locator(save).is_disabled())
                        self.assertFalse(page.evaluate('s=>AITDirectories.get(s)',{'user':'7','property':prop}))
                        page.evaluate("FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'granted'")
                        page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.startsWith('Folder saved.')",arg=message)
                    finally:browser.close()

    def test_indexeddb_abort_preserves_previous_assignment_and_pending_handle(self):
        with sync_playwright() as runtime:
            for program,markup,select,save,name,message,prop in self.variants():
                with self.subTest(program=program):
                    browser,page=open_browser(runtime,markup)
                    try:
                        page.locator(select).click();page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.startsWith('Folder saved.')",arg=message)
                        page.evaluate("""()=>{
                            window.showDirectoryPicker=async()=> (await navigator.storage.getDirectory()).getDirectoryHandle('New selection',{create:true});
                            window.realPut=IDBObjectStore.prototype.put;
                            IDBObjectStore.prototype.put=function(){throw new DOMException('Simulated IndexedDB write abort','AbortError');};
                        }""")
                        page.locator(select).click();page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.includes('Browser storage could not save')",arg=message)
                        self.assertIn('Simulated IndexedDB write abort',page.locator(message).inner_text())
                        self.assertEqual(page.locator(name).inner_text(),'New selection')
                        self.assertEqual(page.evaluate('async s=>(await AITDirectories.get(s)).handle.name',{'user':'7','property':prop}),'Reports')
                        page.evaluate('()=>{IDBObjectStore.prototype.put=window.realPut;}')
                        page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.startsWith('Folder saved.')",arg=message)
                        page.reload()
                        page.wait_for_function("s=>document.querySelector(s).textContent==='New selection'",arg=name)
                    finally:browser.close()

    def test_storage_open_failure_does_not_prevent_selection_display(self):
        with sync_playwright() as runtime:
            for program,markup,select,save,name,message,prop in self.variants():
                with self.subTest(program=program):
                    init="""window.showDirectoryPicker=async()=>({kind:'directory',name:'Chosen local folder',requestPermission:async()=> 'granted',queryPermission:async()=> 'granted'});
                        indexedDB.open=()=>{const request={};setTimeout(()=>{request.error=new DOMException('Local storage unavailable','UnknownError');request.onerror();},0);return request;};"""
                    browser,page=open_browser(runtime,markup,init)
                    try:
                        page.wait_for_function("s=>document.querySelector(s).textContent.includes('could not be restored')",arg=message)
                        page.locator(select).click()
                        self.assertEqual(page.locator(name).inner_text(),'Chosen local folder')
                        page.locator(save).click()
                        page.wait_for_function("s=>document.querySelector(s).textContent.includes('Browser storage could not save')",arg=message)
                        self.assertIn('Local storage unavailable',page.locator(message).inner_text())
                    finally:browser.close()
