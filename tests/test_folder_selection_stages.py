"""Real Chromium storage/permission tests; the native OS picker is stubbed."""
import unittest
from playwright.sync_api import sync_playwright
from test_pm_directory_register import open_browser, REPORTS


@unittest.skip('Retired picker is inactive; replacement coverage is in test_pm_property_folders.py')
class FolderSelectionStagesTests(unittest.TestCase):
    def test_mismatched_readback_never_claims_success(self):
        with sync_playwright() as runtime:
            for mismatch in ('property','handle'):
                with self.subTest(mismatch=mismatch):
                    browser,page=open_browser(runtime)
                    try:
                        page.evaluate("""mismatch=>{const read=AITDirectories.get;AITDirectories.get=async scope=>{
                            const row=await read(scope);if(!row)return row;
                            if(mismatch==='property')return {...row,property:'27'};
                            const other=await(await navigator.storage.getDirectory()).getDirectoryHandle('Other parent',{create:true});
                            return {...row,handle:await other.getDirectoryHandle('Reports',{create:true})};
                        };}""",mismatch)
                        page.locator('#directory-select').click()
                        page.wait_for_function("document.getElementById('directory-message').textContent.includes('Could not confirm')")
                        self.assertNotIn('Folder saved.',page.locator('#directory-message').inner_text())
                    finally:browser.close()

    def test_already_granted_handle_does_not_repeat_permission_request(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""()=>{FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'granted';
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=>{throw new DOMException('Redundant request aborted','AbortError');};}""")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'26'})).handle.name"),'Reports')
            finally:browser.close()

    def test_late_restore_never_overrides_successful_replacement(self):
        init="""Object.defineProperty(window,'AITDirectories',{configurable:true,set(api){
            const original=api.get;let first=true;api.get=scope=>{if(!first)return original(scope);first=false;
                return new Promise(resolve=>window.restoreOld=async()=>resolve({handle:await (await navigator.storage.getDirectory()).getDirectoryHandle('Old assignment',{create:true})}));};
            Object.defineProperty(window,'AITDirectories',{value:api,configurable:true});
        }});
        window.showDirectoryPicker=async()=>(await navigator.storage.getDirectory()).getDirectoryHandle('New assignment',{create:true});"""
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init=init)
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                page.evaluate('restoreOld()')
                self.assertEqual(page.locator('#directory-folder').inner_text(),'New assignment')
            finally:browser.close()

    def test_almond_dale_legacy_keys_switch_reload_and_report_destinations(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    const old=await root.getDirectoryHandle('1Property Management',{create:true});
                    await AITDirectories.set({user:'7',property:'26'},old);
                    await new Promise((resolve,reject)=>{const open=indexedDB.open('ait-pm-directories',1);
                        open.onsuccess=()=>{const db=open.result,tx=db.transaction('assignments','readwrite'),store=tx.objectStore('assignments');
                            for(const key of [JSON.stringify(['7','26','metsoa_statement']),JSON.stringify(['7','__main__'])])store.put({key,user:'7',property:'26',handle:old});
                            tx.oncomplete=()=>{db.close();resolve();};tx.onerror=()=>reject(tx.error);};});
                }""")
                page.reload()
                page.wait_for_function("document.getElementById('directory-folder').textContent==='1Property Management'")
                for prop,name in [('26','New Almond'),('27','Dale folder')]:
                    if prop=='27':
                        with page.expect_navigation():page.locator('#directory-property').select_option(prop)
                        self.assertEqual(page.locator('#directory-folder').inner_text(),'No folder selected')
                    page.evaluate("name=>{window.showDirectoryPicker=async()=>(await navigator.storage.getDirectory()).getDirectoryHandle(name,{create:true});}",name)
                    page.locator('#directory-select').click()
                    page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                    page.reload()
                    page.wait_for_function("name=>document.getElementById('directory-folder').textContent===name",arg=name)
                    page.evaluate("""reports=>{const s=document.createElement('script');s.id='directory-reports';s.textContent=JSON.stringify(reports);document.body.append(s);
                        window.fetch=async()=>new Response('%PDF-report',{headers:{'Content-Type':'application/pdf'}});}""",REPORTS)
                    destination=page.evaluate("""async prop=>{const scope={user:'7',property:prop,period_end:'2026-07-31'};
                        await AITDirectories.save(scope,'metsoa_statement','/report.pdf');
                        const root=(await AITDirectories.get(scope)).handle;
                        const file=await(await(await root.getDirectoryHandle('METSOA')).getDirectoryHandle('METSOA Statement')).getFileHandle('310726_METSOA.pdf');
                        return {name:root.name,text:await(await file.getFile()).text()};}""",prop)
                    self.assertEqual(destination,dict(name=name,text='%PDF-report'))
                with page.expect_navigation():page.locator('#directory-property').select_option('26')
                page.wait_for_function("document.getElementById('directory-folder').textContent==='New Almond'")
                result=page.evaluate("""async()=>{const request=indexedDB.open('ait-pm-directories',1);
                    return new Promise(resolve=>{request.onsuccess=()=>{const db=request.result,tx=db.transaction('assignments'),r=tx.objectStore('assignments').getAll();
                        r.onsuccess=()=>resolve(r.result.map(x=>({key:x.key,name:x.handle.name})));tx.oncomplete=()=>db.close();};});}""")
                rows={r['key']:r['name'] for r in result}
                self.assertEqual(rows['["7","26","__main__"]'],'New Almond')
                self.assertEqual(rows['["7","27","__main__"]'],'Dale folder')
                self.assertEqual(rows['["7","26","metsoa_statement"]'],'1Property Management')
                self.assertEqual(rows['["7","__main__"]'],'1Property Management')
            finally:browser.close()

    def test_picker_errors_are_short_and_keep_previous_assignment(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                for error in ('AbortError','NotAllowedError','SecurityError','TypeError'):
                    with self.subTest(error=error):
                        page.evaluate("name=>{window.showDirectoryPicker=async()=>{throw new DOMException('RAW PRIVATE ERROR',name);};}",error)
                        page.locator('#directory-select').click()
                        page.wait_for_function("document.getElementById('directory-message').textContent!=='Folder saved.'")
                        message=page.locator('#directory-message').inner_text()
                        self.assertNotIn('RAW PRIVATE ERROR',message)
                        self.assertLess(len(message),140)
                        self.assertEqual(page.locator('#directory-folder').inner_text(),'Reports')
                        self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'26'})).handle.name"),'Reports')
            finally:browser.close()

    def test_permission_denial_and_retry_use_same_button(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init="""window.showDirectoryPicker=async()=>({
                kind:'directory',name:'Denied folder',requestPermission:async()=>'denied'
            });""")
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('not allowed')")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Denied folder')
                self.assertFalse(page.evaluate("AITDirectories.get({user:'7',property:'26'})"))
                page.evaluate("window.showDirectoryPicker=async()=>(await navigator.storage.getDirectory()).getDirectoryHandle('Allowed',{create:true})")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Allowed')
            finally:browser.close()

    def test_indexeddb_failure_keeps_old_assignment_and_retries(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                page.evaluate("""()=>{window.realPut=IDBObjectStore.prototype.put;
                    IDBObjectStore.prototype.put=function(){throw new DOMException('RAW STORAGE ERROR','AbortError');};
                    window.showDirectoryPicker=async()=>(await navigator.storage.getDirectory()).getDirectoryHandle('New folder',{create:true});}""")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('Could not remember')")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'New folder')
                self.assertNotIn('RAW STORAGE ERROR',page.locator('#directory-message').inner_text())
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'26'})).handle.name"),'Reports')
                page.evaluate("()=>{IDBObjectStore.prototype.put=realPut;}")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                page.reload()
                page.wait_for_function("document.getElementById('directory-folder').textContent==='New folder'")
            finally:browser.close()

    def test_report_save_retains_lazy_paths_and_original_files(self):
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                # Report pages supply the existing catalog; the compact Folders page does not.
                page.evaluate("""reports=>{const config=document.createElement('script');
                    config.id='directory-reports';config.type='application/json';config.textContent=JSON.stringify(reports);
                    document.body.append(config);
                    window.fetch=async()=>new Response('%PDF-existing report',{status:200,headers:{'Content-Type':'application/pdf'}});
                }""",REPORTS)
                result=page.evaluate("""async()=>{const scope={user:'7',property:'26',period_end:'2026-07-31'};
                    await AITDirectories.save(scope,'metsoa_statement','/test.pdf');
                    await AITDirectories.save(scope,'metsoa_statement','/test.pdf');
                    const root=(await AITDirectories.get(scope)).handle;
                    return {roots:await Array.fromAsync(root.keys()),files:(await AITDirectories.list(scope,'metsoa_statement',{prompt:false})).map(x=>({name:x.name,path:x.folder}))};
                }""")
                self.assertEqual(result['roots'],['METSOA'])
                self.assertEqual({x['name'] for x in result['files']},{'310726_METSOA.pdf','310726_METSOA(1).pdf'})
                self.assertTrue(all(x['path']=='METSOA/METSOA Statement' for x in result['files']))
            finally:browser.close()

