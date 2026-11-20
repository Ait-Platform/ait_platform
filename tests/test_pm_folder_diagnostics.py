"""Temporary PM diagnostics: genuine IndexedDB, stubbed native picker."""
import json
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser, browser_html


@unittest.skip('Temporary diagnostics belong to the retired picker; no longer loaded by PM')
class FolderDiagnosticsTests(unittest.TestCase):
    def open(self,runtime,property_id=26,init=''):
        markup=browser_html(property_id).replace('"user": "7"','"user": "644"')
        browser,page=open_browser(runtime,markup,init)
        page.goto('https://ait.test/directory-register?property_id='+str(property_id)+'&folder_debug=1')
        return browser,page

    def test_both_exact_keys_commits_readback_and_restore_origin(self):
        with sync_playwright() as runtime:
            for prop in (26,27):
                with self.subTest(property=prop):
                    browser,page=self.open(runtime,prop)
                    try:
                        page.locator('#directory-select').click()
                        page.wait_for_function("AITPMFolderDiagnostics.some(x=>x.event==='selection-saved')")
                        rows=page.evaluate('AITPMFolderDiagnostics')
                        expected=json.dumps(['644',str(prop),'__main__'],separators=(',',':'))
                        self.assertTrue(all(row['key']==expected for row in rows))
                        write=[r for r in rows if r.get('phase')=='write']
                        self.assertTrue(any(r['event']=='indexeddb-put-start' for r in write))
                        self.assertTrue(any(r['event']=='indexeddb-transaction-complete' and r['mode']=='readwrite' for r in write))
                        readback=next(r for r in rows if r['event']=='readback-result')
                        self.assertEqual(readback['storedKey'],expected)
                        self.assertTrue(readback['matches'])
                        self.assertEqual(readback['folderName'],'Reports')
                        events=[r['event'] for r in rows]
                        self.assertLess(events.index('picker-resolved'),events.index('assignment-write-start'))
                        self.assertLess(events.index('assignment-write-complete'),events.index('readback-result'))
                        self.assertEqual(page.locator('button').count(),1)
                        page.reload()
                        page.wait_for_function("AITPMFolderDiagnostics.some(x=>x.event==='restore-result' && x.exists)")
                        restored=page.evaluate("AITPMFolderDiagnostics.find(x=>x.event==='restore-result')")
                        self.assertEqual(restored['origin'],'previously-stored')
                        self.assertEqual(restored['storedKey'],expected)
                        self.assertFalse(page.evaluate("AITPMFolderDiagnostics.some(x=>x.event==='selection-saved')"))
                    finally:browser.close()

    def test_rejection_proves_no_handle_or_write_and_records_exception(self):
        with sync_playwright() as runtime:
            browser,page=self.open(runtime,27,init="window.showDirectoryPicker=async()=>{throw new DOMException('Exact native abort detail','AbortError');};")
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("AITPMFolderDiagnostics.some(x=>x.event==='picker-rejected')")
                rows=page.evaluate('AITPMFolderDiagnostics')
                rejected=next(r for r in rows if r['event']=='picker-rejected')
                self.assertEqual(rejected['name'],'AbortError')
                self.assertEqual(rejected['message'],'Exact native abort detail')
                self.assertFalse(any(r['event'] in ('picker-resolved','assignment-write-start','selection-saved') for r in rows))
                self.assertIn('Exact native abort detail',page.locator('#directory-message').inner_text())
            finally:browser.close()

    def test_failed_write_and_permission_abort_have_distinct_stages(self):
        with sync_playwright() as runtime:
            for stage in ('storage','permission'):
                with self.subTest(stage=stage):
                    browser,page=self.open(runtime)
                    try:
                        if stage=='storage':
                            page.evaluate("()=>{IDBObjectStore.prototype.put=function(){throw new DOMException('Exact write failure','DataCloneError');};}")
                        else:
                            page.evaluate("()=>{AITDirectories.permission=async()=>{throw new DOMException('Exact permission abort','AbortError');};}")
                        page.locator('#directory-select').click()
                        page.wait_for_function("AITPMFolderDiagnostics.some(x=>x.event==='exception')")
                        rows=page.evaluate('AITPMFolderDiagnostics')
                        self.assertTrue(any(r['event']=='picker-resolved' and r['folderName']=='Reports' for r in rows))
                        error=next(r for r in rows if r['event']=='exception')
                        self.assertEqual(error['stage'],stage)
                        self.assertFalse(any(r['event']=='selection-saved' for r in rows))
                        self.assertFalse(any(r['event']=='indexeddb-transaction-complete' and r.get('mode')=='readwrite' for r in rows))
                    finally:browser.close()

    def test_observer_failure_cannot_break_storage_and_default_page_has_no_trace(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                self.assertFalse(page.evaluate("'AITPMFolderDiagnostics' in window"))
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                result=page.evaluate("""async()=>{const scope={user:'7',property:'26'},handle=(await AITDirectories.get(scope)).handle;
                    const observer=()=>{throw Error('Broken diagnostic consumer');};
                    await AITDirectories.set(scope,handle,observer);
                    return (await AITDirectories.get(scope,observer)).handle.name;}""")
                self.assertEqual(result,'Reports')
                self.assertEqual(page.locator('button').count(),1)
            finally:browser.close()

