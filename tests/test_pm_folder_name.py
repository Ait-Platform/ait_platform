"""HTML folder-input tests with actual temporary directories, no native FS API."""
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser
from test_pm_directory_register import REPORTS

INIT="""window.showDirectoryPicker=()=>{throw Error('Forbidden native API');};
IDBObjectStore.prototype.put=function(){throw Error('No assignment changes allowed');};
window.fetch=()=>{throw Error('No report or upload requests');};"""


class FolderNameTests(unittest.TestCase):
    def test_existing_property_assignments_restore_without_html_replacement(self):
        with sync_playwright() as runtime:
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
                    self.assertTrue(page.locator('#directory-select').is_hidden())
                    page.evaluate("document.getElementById('directory-select').onclick()")
                    self.assertFalse(page.locator('#directory-modal').is_visible())
                    self.assertEqual(page.locator('#directory-folder').inner_text(),title)
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

    def test_empty_folders_auto_register_for_almond_and_dale(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init="window.showDirectoryPicker=()=>{throw Error('Forbidden picker');};")
            try:
                for property_id,title in [(26,'Almond reports'),(27,'Dale reports')]:
                    page.goto('https://ait.test/directory-register?property_id='+str(property_id))
                    page.wait_for_function("!document.getElementById('directory-select').disabled")
                    page.locator('#directory-select').click()
                    page.evaluate("""async name=>{const handle=await(await navigator.storage.getDirectory()).getDirectoryHandle(name,{create:true});
                        await document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve(handle)}]}});}""",title)
                    self.assertEqual(page.locator('#directory-folder').inner_text(),title)
                    self.assertTrue(page.locator('#directory-select').is_hidden())
                    self.assertFalse(page.locator('#directory-modal').is_visible())
                    page.reload();page.wait_for_function("name=>document.getElementById('directory-folder').textContent===name",arg=title)
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'26'})).handle.name"),'Almond reports')
            finally:browser.close()

    def test_cancel_during_pending_drop_never_registers(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.locator('#directory-select').click()
                page.evaluate("""()=>{window.lateDrop=document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>new Promise(resolve=>window.finishDrop=resolve)}]}});}""")
                page.locator('#directory-modal-close').click()
                page.evaluate("async()=>{finishDrop(await(await navigator.storage.getDirectory()).getDirectoryHandle('Late folder',{create:true}));await lateDrop;}")
                self.assertFalse(page.evaluate("AITDirectories.get({user:'7',property:'26'})"))
                self.assertEqual(page.locator('#directory-folder').inner_text(),'No folder selected')
            finally:browser.close()

    def test_invalid_drop_and_storage_failure_do_not_enable_replacement(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.locator('#directory-select').click()
                page.evaluate("async()=>document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve({kind:'file',name:'Not a directory'})}]}})")
                self.assertIn('not a file',page.locator('#directory-message').inner_text())
                self.assertFalse(page.evaluate("AITDirectories.get({user:'7',property:'26'})"))
            finally:browser.close()
            browser,page=open_browser(runtime,init="IDBObjectStore.prototype.get=function(){throw Error('Storage read failed');};")
            try:
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('Could not read')")
                self.assertTrue(page.locator('#directory-select').is_disabled())
            finally:browser.close()

    def test_core_template_registration_supports_other_programmes_and_rejects_unsafe_paths(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                result=page.evaluate("""async()=>{
                    const scope={user:'7',property:'example-client'},root=await(await navigator.storage.getDirectory()).getDirectoryHandle('Example programme',{create:true});
                    AITDirectories.registerTemplate('example',{initial:context=>[{directories:[context.category,'Initial reports']}]});
                    const saved=await AITDirectories.register(scope,root,{program:'example',category:'Documents'});
                    return {name:saved.handle.name,initial:(await root.getDirectoryHandle('Documents')).name,child:(await(await root.getDirectoryHandle('Documents')).getDirectoryHandle('Initial reports')).name};
                }""")
                self.assertEqual(result,dict(name='Example programme',initial='Documents',child='Initial reports'))
                saved=page.evaluate("""async()=>{
                    document.getElementById('directory-program').textContent=JSON.stringify('example');
                    document.getElementById('directory-reports').textContent=JSON.stringify([{code:'example_pdf',title:'Summary',directories:['Documents','Saved PDFs']}]);
                    const scope={user:'7',property:'example-client',period_end:'2026-11-22'},root=(await AITDirectories.get(scope)).handle;
                    const before=await AITDirectories.list(scope,'example_pdf',{prompt:false});
                    const noPrematureFolders=await Array.fromAsync((await root.getDirectoryHandle('Documents')).keys());
                    window.fetch=async()=>new Response('%PDF-original-example');
                    const name=await AITDirectories.save(scope,'example_pdf','/example.pdf');
                    const files=await AITDirectories.list(scope,'example_pdf',{prompt:false});
                    return {before:before.length,initial:noPrematureFolders,name,bytes:await(await files[0].handle.getFile()).text()};
                }""")
                self.assertEqual(saved,dict(before=0,initial=['Initial reports'],name='221126_Summary.pdf',bytes='%PDF-original-example'))

                bad=page.evaluate("""async()=>{
                    const scope={user:'7',property:'bad-template'},root=await(await navigator.storage.getDirectory()).getDirectoryHandle('Unsafe target',{create:true});
                    try{await AITDirectories.register(scope,root,{program:'example',category:'..'});return {failed:false};}
                    catch(error){return {failed:true,assigned:!!await AITDirectories.get(scope),children:await Array.fromAsync(root.keys())};}
                }""")
                self.assertEqual(bad,dict(failed=True,assigned=False,children=[]))
            finally:browser.close()
