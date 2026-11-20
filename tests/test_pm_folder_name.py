"""HTML folder-input tests with actual temporary directories, no native FS API."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser

INIT="""window.showDirectoryPicker=()=>{throw Error('Forbidden native API');};
Object.defineProperty(window,'indexedDB',{get(){throw Error('No browser storage');}});
window.fetch=()=>{throw Error('No report or upload requests');};"""


class FolderNameTests(unittest.TestCase):
    def test_almond_and_dale_names_from_directory_file_chooser(self):
        with TemporaryDirectory() as temporary, sync_playwright() as runtime:
            browser,page=open_browser(runtime,init=INIT)
            try:
                for property_id,title in [(26,'Almond reports'),(27,'Dale reports')]:
                    with self.subTest(property=property_id):
                        folder=Path(temporary)/title
                        (folder/'Nested').mkdir(parents=True)
                        (folder/'Nested'/'sample.txt').write_text('Synthetic file; never uploaded')
                        page.goto('https://ait.test/directory-register?property_id='+str(property_id))
                        with page.expect_file_chooser() as chooser:
                            page.locator('#directory-select').click()
                        self.assertTrue(chooser.value.is_multiple())
                        chooser.value.set_files(str(folder))
                        page.wait_for_function("name=>document.getElementById('directory-folder').textContent===name",arg=title)
                        self.assertEqual(page.locator('button').count(),1)
                        self.assertTrue(page.locator('#directory-input').is_hidden())
                        self.assertEqual(page.locator('#directory-input').evaluate('(input)=>input.files.length'),0)
                        self.assertEqual(page.locator('#directory-message').inner_text(),'')
                        page.reload()
                        self.assertEqual(page.locator('#directory-folder').inner_text(),'No folder selected')
            finally:browser.close()

    def test_empty_folder_or_missing_relative_path_is_not_success(self):
        with TemporaryDirectory() as temporary, sync_playwright() as runtime:
            browser,page=open_browser(runtime,init=INIT)
            try:
                # Empty directories may deliver no change event at all. Exercise
                # the resulting empty FileList without relying on a native event.
                page.locator('#directory-input').dispatch_event('change')
                self.assertEqual(page.locator('#directory-folder').inner_text(),'No folder selected')
                self.assertIn('containing a file',page.locator('#directory-message').inner_text())
                # A plain File without a directory-relative path cannot identify a folder.
                page.evaluate("""()=>{const input=document.getElementById('directory-input'),data=new DataTransfer();
                    data.items.add(new File(['test'],'sample.txt'));input.files=data.files;input.dispatchEvent(new Event('change'));}""")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'No folder selected')
            finally:browser.close()

    def test_same_folder_can_be_selected_again_and_cancellation_is_clear(self):
        with TemporaryDirectory() as temporary, sync_playwright() as runtime:
            folder=Path(temporary)/'Reports';folder.mkdir();(folder/'file.txt').write_text('test')
            browser,page=open_browser(runtime,init=INIT)
            try:
                for _ in range(2):
                    with page.expect_file_chooser() as chooser:
                        page.locator('#directory-select').click()
                    chooser.value.set_files(str(folder))
                    self.assertEqual(page.locator('#directory-folder').inner_text(),'Reports')
                page.locator('#directory-input').dispatch_event('cancel')
                self.assertEqual(page.locator('#directory-message').inner_text(),'Folder selection cancelled.')
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Reports')
            finally:browser.close()
