"""Read-only Folder Register browser regressions."""
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser, REPORTS


class FolderRegisterTests(unittest.TestCase):
    def test_numbered_register_availability_and_close_do_not_modify_folders(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory(),folder=await root.getDirectoryHandle('Almond register',{create:true});
                    const category=await folder.getDirectoryHandle('Meter Readings',{create:true});
                    await category.getDirectoryHandle('Reading History',{create:true});
                    await AITDirectories.set({user:'7',property:'26'},folder);
                    window.nativeDirectory=FileSystemDirectoryHandle.prototype.getDirectoryHandle;
                    FileSystemDirectoryHandle.prototype.getDirectoryHandle=function(name,options){if(options?.create)throw Error('Register must not create');return nativeDirectory.call(this,name,options);};
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=>{throw Error('Register must not prompt');};
                    FileSystemDirectoryHandle.prototype.getFileHandle=async()=>{throw Error('Register must not read file bytes');};
                    AITDirectories.set=async()=>{throw Error('Register must not assign');};
                }""")
                self.assertTrue(page.locator('#folder-register-area').is_hidden())
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                rows=page.locator('#folder-register-rows tr').all_text_contents()
                self.assertEqual(len(rows),len(REPORTS))
                numbers=page.locator('#folder-register-rows tr td:first-child').all_text_contents()
                self.assertEqual(numbers,[str(i) for i in range(1,len(REPORTS)+1)])
                self.assertEqual(page.locator('#folder-register-rows tr td:last-child').all_text_contents(),['Available']+['Not found']*(len(REPORTS)-1))
                unchanged=page.evaluate("""async()=>{const root=(await AITDirectories.get({user:'7',property:'26'})).handle;
                    return {roots:await Array.fromAsync(root.keys()),children:await Array.fromAsync((await root.getDirectoryHandle('Meter Readings')).keys())};}""")
                self.assertEqual(unchanged,dict(roots=['Meter Readings'],children=['Reading History']))
                page.locator('#folder-register-close').click()
                self.assertTrue(page.locator('#folder-register-area').is_hidden())
                self.assertEqual(page.locator('#folder-register-open').get_attribute('aria-expanded'),'false')
                self.assertTrue(page.locator('#folder-register-open').evaluate('(node)=>node===document.activeElement'))
            finally:browser.close()

    def test_property_switch_reads_only_its_assignment_and_not_html_name(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    for(const [property,name,source,title] of [['26','Almond only','Meter Readings','Reading History'],['27','Dale only','Municipal Accounts','Account Register']]){
                        const folder=await root.getDirectoryHandle(name,{create:true});
                        await(await folder.getDirectoryHandle(source,{create:true})).getDirectoryHandle(title,{create:true});
                        await AITDirectories.set({user:'7',property},folder);
                    }}""")
                for prop,root,available in [('26','Almond only','Meter Readings / Reading History'),('27','Dale only','Municipal Accounts / Account Register')]:
                    page.goto('https://ait.test/directory-register?property_id='+prop)
                    page.evaluate("document.getElementById('directory-folder').textContent='Unassigned HTML name'")
                    page.locator('#folder-register-open').click()
                    page.wait_for_function("root=>document.getElementById('folder-register-message').textContent.includes(root)",arg=root)
                    matches=page.locator('#folder-register-rows tr').evaluate_all("(rows)=>rows.filter(r=>r.lastChild.textContent==='Available').map(r=>r.children[1].textContent)")
                    self.assertEqual(matches,[available])
                    self.assertEqual(page.locator('#directory-folder').inner_text(),'Unassigned HTML name')
            finally:browser.close()

    def test_missing_assignment_and_expired_permission_are_clear_without_prompts(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('No report destination')")
                self.assertEqual(set(page.locator('#folder-register-rows tr td:last-child').all_text_contents()),{'No folder assigned'})
                page.locator('#folder-register-close').click()
                page.evaluate("""async()=>{await AITDirectories.set({user:'7',property:'26'},await navigator.storage.getDirectory());
                    FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=>{throw Error('No automatic prompts');};
                    FileSystemDirectoryHandle.prototype.getDirectoryHandle=async()=>{throw Error('No access while unauthorised');};
                }""")
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('not currently authorised')")
                self.assertEqual(set(page.locator('#folder-register-rows tr td:last-child').all_text_contents()),{'Access not authorised'})
            finally:browser.close()

    def test_close_during_pending_read_does_not_reopen_or_update_area(self):
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
