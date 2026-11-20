"""Read-only Folder Register browser regressions."""
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser, REPORTS


class FolderRegisterTests(unittest.TestCase):
    def test_counts_open_and_close_never_modify_or_retrieve_file_bytes(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory(),folder=await root.getDirectoryHandle('Almond register',{create:true});
                    const category=await folder.getDirectoryHandle('Meter Readings',{create:true});
                    const report=await category.getDirectoryHandle('Reading History',{create:true});
                    for(const name of ['older.pdf','newer.pdf','notes.txt'])await report.getFileHandle(name,{create:true});
                    await report.getDirectoryHandle('Nested',{create:true});
                    await category.getDirectoryHandle('Municipal Reading Submission',{create:true});
                    await AITDirectories.set({user:'7',property:'26'},folder);
                    window.nativeDirectory=FileSystemDirectoryHandle.prototype.getDirectoryHandle;
                    FileSystemDirectoryHandle.prototype.getDirectoryHandle=function(name,options){if(options?.create)throw Error('Register must not create');return nativeDirectory.call(this,name,options);};
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=>{throw Error('Register must not prompt');};
                    FileSystemDirectoryHandle.prototype.getFileHandle=async()=>{throw Error('Register must not retrieve files');};
                    FileSystemFileHandle.prototype.getFile=async()=>{throw Error('Register must not read bytes');};
                    FileSystemFileHandle.prototype.createWritable=async()=>{throw Error('Register must not write');};
                    AITDirectories.set=async()=>{throw Error('Register must not assign');};
                }""")
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                self.assertEqual(page.locator('thead th').all_text_contents(),['No.','Folder','Files','Action'])
                self.assertEqual(page.locator('#folder-register-rows tr td:first-child').all_text_contents(),[str(i) for i in range(1,len(REPORTS)+1)])
                self.assertEqual(page.locator('#folder-register-rows tr td:nth-child(2)').all_text_contents(),[r['source']+' / '+r['title'] for r in REPORTS])
                self.assertEqual(page.locator('#folder-register-rows tr td:nth-child(3)').all_text_contents(),['3','0']+['\u2014']*5)
                self.assertEqual(page.locator('#folder-register-rows button:enabled').count(),2)
                page.locator('#folder-register-rows button').first.click()
                page.wait_for_function("document.getElementById('folder-register-files').children.length===3")
                self.assertEqual(page.locator('#folder-register-files li').all_text_contents(),['newer.pdf','notes.txt','older.pdf'])
                self.assertTrue(page.locator('#folder-register-table').is_hidden())
                self.assertIn('Utilities Output',page.locator('#folder-register-message').inner_text())
                page.locator('#folder-register-close').click()
                self.assertTrue(page.locator('#folder-register-table').is_visible())
                self.assertTrue(page.locator('#folder-register-area').is_visible())
                page.locator('#folder-register-rows button').nth(1).click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent==='No files in this folder.'")
                page.locator('#folder-register-close').click()
                unchanged=page.evaluate("""async()=>{const root=(await AITDirectories.get({user:'7',property:'26'})).handle;
                    return {roots:await Array.fromAsync(root.keys()),children:(await Array.fromAsync((await root.getDirectoryHandle('Meter Readings')).keys())).sort()};}""")
                self.assertEqual(unchanged,dict(roots=['Meter Readings'],children=['Municipal Reading Submission','Reading History']))
                page.locator('#folder-register-close').click()
                self.assertTrue(page.locator('#folder-register-area').is_hidden())
                self.assertEqual(page.locator('#folder-register-open').get_attribute('aria-expanded'),'false')
            finally:browser.close()

    def test_property_switch_counts_only_its_own_files(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    for(const [property,name,source,title,file] of [['26','Almond only','Meter Readings','Reading History','Almond.pdf'],['27','Dale only','Municipal Accounts','Account Register','Dale.pdf']]){
                        const folder=await root.getDirectoryHandle(name,{create:true});
                        const report=await(await folder.getDirectoryHandle(source,{create:true})).getDirectoryHandle(title,{create:true});
                        await report.getFileHandle(file,{create:true});await AITDirectories.set({user:'7',property},folder);
                    }}""")
                for prop,root,available,file in [('26','Almond only','Meter Readings / Reading History','Almond.pdf'),('27','Dale only','Municipal Accounts / Account Register','Dale.pdf')]:
                    page.goto('https://ait.test/directory-register?property_id='+prop)
                    page.wait_for_function("root=>document.getElementById('directory-folder').textContent===root",arg=root)
                    self.assertTrue(page.locator('#directory-select').is_hidden())
                    page.locator('#folder-register-open').click()
                    page.wait_for_function("root=>document.getElementById('folder-register-message').textContent.includes(root)",arg=root)
                    matches=page.locator('#folder-register-rows tr').evaluate_all("rows=>rows.filter(r=>r.children[2].textContent==='1').map(r=>r.children[1].textContent)")
                    self.assertEqual(matches,[available])
                    page.locator('#folder-register-rows button:enabled').click()
                    page.wait_for_function("document.getElementById('folder-register-files').children.length===1")
                    self.assertEqual(page.locator('#folder-register-files li').all_text_contents(),[file])
            finally:browser.close()

    def test_unavailable_access_is_dash_without_prompts_or_false_zero(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('No report destination')")
                self.assertEqual(set(page.locator('#folder-register-rows tr td:nth-child(3)').all_text_contents()),{'\u2014'})
                page.locator('#folder-register-close').click()
                page.evaluate("""async()=>{await AITDirectories.set({user:'7',property:'26'},await navigator.storage.getDirectory());
                    FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=>{throw Error('No automatic prompts');};
                    FileSystemDirectoryHandle.prototype.getDirectoryHandle=async()=>{throw Error('No access while unauthorised');};
                }""")
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('not currently authorised')")
                self.assertEqual(set(page.locator('#folder-register-rows tr td:nth-child(3)').all_text_contents()),{'\u2014'})
                self.assertEqual(page.locator('#folder-register-rows button:enabled').count(),0)
            finally:browser.close()

    def test_permission_lost_before_open_does_not_show_stale_files(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    await(await root.getDirectoryHandle('METSOA',{create:true})).getDirectoryHandle('METSOA Statement',{create:true});
                    await AITDirectories.set({user:'7',property:'26'},root);}""")
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                page.evaluate("FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'denied'")
                page.locator('#folder-register-rows button:enabled').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('Could not list')")
                self.assertEqual(page.locator('#folder-register-files li').count(),0)
                page.locator('#folder-register-close').click()
                self.assertEqual(set(page.locator('#folder-register-rows tr td:nth-child(3)').all_text_contents()),{'\u2014'})
            finally:browser.close()

    def test_close_during_pending_read_does_not_reopen_area(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("()=>{AITDirectories.get=()=>new Promise(resolve=>window.finishRegister=resolve);}")
                page.locator('#folder-register-open').click()
                page.locator('#folder-register-close').click()
                page.evaluate('finishRegister(null)')
                self.assertTrue(page.locator('#folder-register-area').is_hidden())
                self.assertEqual(page.locator('#folder-register-open').get_attribute('aria-expanded'),'false')
            finally:browser.close()
