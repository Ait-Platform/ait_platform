"""HTML folder-input tests with actual temporary directories, no native FS API."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser
from test_pm_directory_register import REPORTS

INIT="""window.showDirectoryPicker=()=>{throw Error('Forbidden native API');};
IDBObjectStore.prototype.put=function(){throw Error('No assignment changes allowed');};
window.fetch=()=>{throw Error('No report or upload requests');};"""


class FolderNameTests(unittest.TestCase):
    def test_existing_property_assignments_restore_without_html_replacement(self):
        with TemporaryDirectory() as temporary, sync_playwright() as runtime:
            selected=Path(temporary)/'New name only';selected.mkdir();(selected/'example.txt').write_text('Never uploaded')
            browser,page=open_browser(runtime,init="window.showDirectoryPicker=()=>{throw Error('Old picker forbidden');};")
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    for(const [property,name] of [['26','Original Almond'],['27','Original Dale']])
                        await AITDirectories.set({user:'7',property},await root.getDirectoryHandle(name,{create:true}));}""")
                page.add_init_script("FileSystemDirectoryHandle.prototype.requestPermission=async()=>{throw Error('No new permission request');};")
                for prop,title in [('26','Original Almond'),('27','Original Dale')]:
                    page.goto('https://ait.test/directory-register?property_id='+prop)
                    page.wait_for_function("name=>document.getElementById('directory-folder').textContent===name",arg=title)
                    page.wait_for_function("name=>document.getElementById('directory-message').textContent.includes(name)",arg=title)
                    with page.expect_file_chooser() as chooser:page.locator('#directory-select').click()
                    chooser.value.set_files(str(selected))
                    self.assertEqual(page.locator('#directory-folder').inner_text(),'New name only')
                    self.assertIn(title,page.locator('#directory-message').inner_text())
                    self.assertIn('does not grant write access',page.locator('#directory-message').inner_text())
                    self.assertEqual(page.evaluate("async prop=>(await AITDirectories.get({user:'7',property:prop})).handle.name",prop),title)
                    page.evaluate("""reports=>{const s=document.createElement('script');s.id='directory-reports';s.textContent=JSON.stringify(reports);document.body.append(s);
                        window.fetch=async()=>new Response('%PDF-original destination',{headers:{'Content-Type':'application/pdf'}});}""",REPORTS)
                    actual=page.evaluate("""async prop=>{const scope={user:'7',property:prop,period_end:'2026-07-31'};
                        await AITDirectories.save(scope,'metsoa_statement','/pdf');
                        const root=(await AITDirectories.get(scope)).handle;
                        const f=await(await(await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement')).getFileHandle('310726_METSOA.pdf');
                        return {name:root.name,text:await(await f.getFile()).text()};}""",prop)
                    self.assertEqual(actual,dict(name=title,text='%PDF-original destination'))
                    page.reload()
                    page.wait_for_function("name=>document.getElementById('directory-folder').textContent===name",arg=title)
            finally:browser.close()

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
                        self.assertIn('does not grant write access',page.locator('#directory-message').inner_text())
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
                self.assertIn('Folder selection cancelled.',page.locator('#directory-message').inner_text())
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Reports')
            finally:browser.close()
