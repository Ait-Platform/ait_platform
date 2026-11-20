"""Name-only browser selection; native Windows dialog is not automated."""
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser


class FolderNameTests(unittest.TestCase):
    def test_name_display_has_no_storage_permissions_or_report_operations(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init="""window.calls=0;
                window.showDirectoryPicker=async(...options)=>{window.options=options;calls++;return {name:'Reconciliation'};};
                Object.defineProperty(window,'indexedDB',{get(){throw Error('No storage allowed');}});
                window.fetch=()=>{throw Error('No report requests allowed');};""")
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Reconciliation'")
                self.assertEqual(page.evaluate('calls'),1)
                self.assertEqual(page.evaluate('options'),[])
                self.assertEqual(page.locator('button').count(),1)
                page.reload()
                self.assertEqual(page.locator('#directory-folder').inner_text(),'No folder selected')
                self.assertEqual(page.evaluate('calls'),0)
            finally:browser.close()

    def test_rejection_does_not_claim_selection(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init="window.showDirectoryPicker=async()=>{throw new DOMException('Native failure','AbortError');};")
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='The browser did not return a folder.'")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'No folder selected')
                self.assertFalse(page.locator('#directory-select').is_disabled())
            finally:browser.close()

    def test_unsupported_browser(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init='window.showDirectoryPicker=undefined;')
            try:
                self.assertTrue(page.locator('#directory-select').is_disabled())
            finally:browser.close()
