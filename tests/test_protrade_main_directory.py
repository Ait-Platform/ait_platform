"""New-shop directories on real browser handles; isolated PostgreSQL route fixtures."""
from urllib.parse import urlsplit
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import app.protrade_directory_setup as setup

registered_program_name_impl=setup.registered_program_name


@unittest.skip('ProTrade Folder Management is suspended after unresolved live Windows failures; retain tests for future authorised development.')
class MainDirectoryTests(unittest.TestCase):
    def setUp(self):
        self.o=base.ShopRegisterTests();self.o.setUp();self.addCleanup(self.o.doCleanups)
        self.app=self.o.app;self.client=self.o.client
        self.existing=self.o.shop(name='Existing shop',trade='mechanic',status='active')
        for name,value in [('MechShop',base.models.MechShop),('registered_program_name',lambda:'ProTrade')]:
            mock=patch.object(setup,name,value);mock.start();self.addCleanup(mock.stop)

    def browser(self,runtime):
        browser=runtime.chromium.launch(headless=True);page=browser.new_page()
        def serve(route):
            url=urlsplit(route.request.url)
            if url.path.startswith('/static/'):
                file=(base.ROOT/'app'/url.path.lstrip('/')).resolve()
                if file.is_relative_to((base.ROOT/'app/static').resolve()) and file.is_file():
                    route.fulfill(body=file.read_bytes(),content_type='text/javascript');return
            response=self.client.get(url.path+('?' + url.query if url.query else ''))
            route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
        page.route('**/*',serve)
        page.add_init_script("window.showDirectoryPicker=()=>{throw Error('Native picker must not be used');};")
        return browser,page

    def drop(self,page,name='Main'):
        page.wait_for_function("!document.getElementById('directory-select').disabled")
        page.locator('#directory-select').click()
        page.evaluate("""async name=>{
            const root=await navigator.storage.getDirectory();
            const handle=await root.getDirectoryHandle(name,{create:true});
            await document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve(handle)}]}});
        }""",name)

    def tree(self,page,name='Main'):
        return page.evaluate("""async name=>{
            async function walk(dir){const result={};for await(const [name,entry] of dir.entries())if(entry.kind==='directory')result[name]=await walk(entry);return result;}
            return walk(await(await navigator.storage.getDirectory()).getDirectoryHandle(name));
        }""",name)

    def new_shop(self,trade='painting',name='Happy Shop'):
        response=self.client.post('/mechanic/onboarding/process',data=dict(trade=trade,business_name=name,vat_registered='no'))
        self.assertEqual(response.status_code,302,response.text)
        self.assertIn('/mechanic/main-directory?registration=',response.location)
        return response.location

    def test_authorisation_new_shop_physical_tree_no_retroactive_or_duplicate_directories(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                self.assertEqual(self.tree(page),{'ProTrade':{}})
                location=self.new_shop()
                page.goto('https://ait.test'+location)
                page.wait_for_function("document.getElementById('directory-message').textContent.startsWith('Verified directories:')")
                expected={'ProTrade':{'Owner':{'Happy Shop':{'Invoice':{},'Statement':{}}}}}
                self.assertEqual(self.tree(page),expected)
                page.reload();page.wait_for_function("document.getElementById('directory-message').textContent.startsWith('Verified directories:')")
                self.assertEqual(self.tree(page),expected)
                # Editing an existing registration must never issue another creation event.
                edited=self.client.post('/mechanic/onboarding/process',data=dict(shop_id=self.existing.id,trade='mechanic',business_name='Existing renamed',vat_registered='no'))
                self.assertNotIn('registration=',edited.location)
            finally:browser.close()

    def test_replacement_preserves_old_tree_and_does_not_backfill_shops(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                page.goto('https://ait.test'+self.new_shop())
                page.wait_for_function("document.getElementById('directory-message').textContent.startsWith('Verified directories:')")
                before=self.tree(page)
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page,'Replacement')
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Replacement' && !document.getElementById('directory-modal').open")
                self.assertEqual(self.tree(page),before)
                self.assertEqual(self.tree(page,'Replacement'),{'ProTrade':{}})
                self.assertEqual(page.evaluate("async()=> (await AITDirectories.get({user:'1',property:'protrade'})).handle.name"),'Replacement')
            finally:browser.close()

    def test_denied_permission_or_failed_write_does_not_confirm_or_replace(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                page.evaluate("""() => {
                    FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'denied';
                    FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';
                }""")
                self.drop(page,'Denied');page.locator('#directory-drop-zone').click()
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('denied')")
                self.assertEqual(page.evaluate("async()=> (await AITDirectories.get({user:'1',property:'protrade'})).handle.name"),'Main')
                page.reload();page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.evaluate("() => {FileSystemFileHandle.prototype.createWritable=async()=>{throw Error('Write verification failed');};}")
                self.drop(page,'Broken')
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('Write verification failed')")
                self.assertEqual(page.evaluate("async()=> (await AITDirectories.get({user:'1',property:'protrade'})).handle.name"),'Main')
            finally:browser.close()

    def test_same_named_shops_and_consultants_remain_isolated(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                for trade in ('painting','plumbing'):
                    page.goto('https://ait.test'+self.new_shop(trade))
                    page.wait_for_function("document.getElementById('directory-message').textContent.startsWith('Verified directories:')")
                before=self.tree(page)
                self.assertEqual(len(before['ProTrade']['Owner']),2)
                other=self.o.db_session.get(base.models.User,2);other.name='Owner';self.o.db_session.commit()
                with self.client.session_transaction() as session:session['_user_id']='2'
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                page.goto('https://ait.test'+self.new_shop('painting'))
                page.wait_for_function("document.getElementById('directory-message').textContent.startsWith('Verified directories:')")
                after=self.tree(page)
                self.assertEqual(after['ProTrade']['Owner'],before['ProTrade']['Owner'])
                self.assertEqual(len(after['ProTrade']),2)
            finally:browser.close()

    def test_inaccessible_registration_can_be_replaced_and_concurrent_changes_are_preserved(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                # Only isolated browser test storage is removed to simulate an obsolete drive.
                page.evaluate("async()=> (await navigator.storage.getDirectory()).removeEntry('Main',{recursive:true})")
                page.reload();page.wait_for_function("!document.getElementById('directory-select').disabled")
                self.drop(page,'Accessible')
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Accessible' && !document.getElementById('directory-modal').open")
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();await AITDirectories.set({user:'1',property:'protrade'},await root.getDirectoryHandle('Other tab',{create:true}));}""")
                self.drop(page,'Stale replacement')
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('changed in another tab')")
                self.assertEqual(page.evaluate("async()=> (await AITDirectories.get({user:'1',property:'protrade'})).handle.name"),'Other tab')
            finally:browser.close()

    def test_new_shop_failure_is_reported_and_invalid_registration_creates_nothing(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                page.add_init_script("""const original=FileSystemDirectoryHandle.prototype.getDirectoryHandle;
                    FileSystemDirectoryHandle.prototype.getDirectoryHandle=function(name,options){if(name==='Statement')throw Error('Statement directory unavailable');return original.call(this,name,options);};""")
                page.goto('https://ait.test'+self.new_shop())
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('directory creation failed')")
                self.assertNotIn('Verified directories:',page.locator('#directory-message').text_content())
                invalid=self.client.post('/mechanic/onboarding/process',data=dict(trade='electrical',business_name='Invalid VAT shop',vat_registered='yes',tax_number='bad'))
                self.assertEqual(invalid.status_code,400)
                self.assertIsNone(base.models.MechShop.query.filter_by(business_name='Invalid VAT shop').first())
            finally:browser.close()

    def test_program_directory_name_comes_from_the_registered_program(self):
        with self.app.test_request_context('/'),patch('app.models.auth.AuthSubject') as model:
            model.query.filter_by.return_value.first.return_value=SimpleNamespace(name='ProTrade')
            self.assertEqual(registered_program_name_impl(),'ProTrade')
            model.query.filter_by.assert_called_once_with(slug='mechanic')
            model.query.filter_by.return_value.first.return_value=SimpleNamespace(name='Registered Program Name')
            self.assertEqual(registered_program_name_impl(),'Registered Program Name')

    def test_browse_selects_empty_folder_with_readwrite_access_and_continues_new_shop(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test'+self.new_shop())
                page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.evaluate("""() => {window.pickerCalls=[];window.showDirectoryPicker=async options=>{
                    pickerCalls.push({mode:options.mode,active:navigator.userActivation.isActive});
                    return (await navigator.storage.getDirectory()).getDirectoryHandle('Empty selected folder',{create:true});
                };}""")
                page.locator('#directory-select').click()
                self.assertTrue(page.get_by_role('button',name='Browse for Folder',exact=True).is_visible())
                self.assertEqual(page.locator('input[type=file]').count(),0)
                page.get_by_role('button',name='Browse for Folder',exact=True).click()
                page.wait_for_function("document.getElementById('directory-message').textContent.startsWith('Verified directories:')")
                self.assertFalse(page.locator('#directory-modal').is_visible())
                self.assertEqual(page.evaluate('pickerCalls'),[dict(mode='readwrite',active=True)])
                self.assertEqual(self.tree(page,'Empty selected folder'),{'ProTrade':{'Owner':{'Happy Shop':{'Invoice':{},'Statement':{}}}}})
            finally:browser.close()

    def test_browse_cancel_denial_and_write_failure_preserve_registration(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                for kind,expected in [('AbortError','cancelled'),('NotAllowedError','denied')]:
                    page.evaluate("kind=>{window.showDirectoryPicker=async()=>{throw new DOMException('Selection failed',kind);};}",kind)
                    page.locator('#directory-select').click()
                    page.locator('#directory-browse').click()
                    page.wait_for_function("text=>document.getElementById('directory-modal-message').textContent.includes(text)",arg=expected)
                    self.assertTrue(page.locator('#directory-modal').is_visible())
                    self.assertEqual(page.locator('#directory-folder').text_content(),'Main')
                    page.locator('#directory-modal-close').click()
                page.evaluate("""() => {window.showDirectoryPicker=async()=> (await navigator.storage.getDirectory()).getDirectoryHandle('Failed destination',{create:true});
                    FileSystemFileHandle.prototype.createWritable=async()=>{throw Error('Write verification failed');};}""")
                page.locator('#directory-select').click();page.locator('#directory-browse').click()
                page.wait_for_function("document.getElementById('directory-modal-message').textContent.includes('Write verification failed')")
                self.assertTrue(page.locator('#directory-modal').is_visible())
                self.assertEqual(page.evaluate("async()=> (await AITDirectories.get({user:'1',property:'protrade'})).handle.name"),'Main')
            finally:browser.close()

    def test_browse_unavailable_keeps_drag_drop_available_and_is_opt_in(self):
        from flask import render_template
        with self.app.test_request_context('/'):
            legacy=render_template('partials/directory_selection_modal.html')
        self.assertNotIn('id="directory-browse"',legacy)
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory')
                page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.evaluate('window.showDirectoryPicker=undefined')
                page.locator('#directory-select').click();page.locator('#directory-browse').click()
                page.wait_for_function("document.getElementById('directory-modal-message').textContent.includes('Chrome or Edge')")
                page.evaluate("""async()=>{const handle=await(await navigator.storage.getDirectory()).getDirectoryHandle('Drop fallback',{create:true});
                    await document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve(handle)}]}});
                }""")
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                self.assertFalse(page.locator('#directory-modal').is_visible())
                self.assertEqual(self.tree(page,'Drop fallback'),{'ProTrade':{}})
            finally:browser.close()

    def test_diagnostics_show_exact_pending_operations_without_aborting_them(self):
        cases=[
            ("FileSystemDirectoryHandle.prototype.queryPermission=()=>new Promise(()=>{});",'Selection: queryPermission(readwrite)'),
            ("window.allowed=false;FileSystemDirectoryHandle.prototype.queryPermission=async()=>allowed?'granted':'prompt';FileSystemDirectoryHandle.prototype.requestPermission=async()=>{allowed=true;return 'granted';};FileSystemFileHandle.prototype.createWritable=()=>new Promise(()=>{});",'Accessibility: createWritable(probe)'),
            ("AITDirectories.set=()=>new Promise(()=>{});",'IndexedDB: store directory handle and commit transaction'),
            ("navigator.locks.request('ait-pm-local-report-save',()=>new Promise(()=>{}));",'Registration: origin-wide lock request (includes time held)')]
        with sync_playwright() as runtime:
            for injected,stage in cases:
                with self.subTest(stage=stage):
                    browser,page=self.browser(runtime)
                    try:
                        page.goto('https://ait.test/mechanic/main-directory')
                        page.wait_for_function("!document.getElementById('directory-select').disabled")
                        page.clock.install()
                        page.evaluate("""() => {window.showDirectoryPicker=async()=> (await navigator.storage.getDirectory()).getDirectoryHandle('Diagnostic folder',{create:true});}""")
                        page.evaluate('() => {'+injected+'}')
                        page.locator('#directory-select').click();page.locator('#directory-browse').click()
                        page.wait_for_function("stage=>document.getElementById('directory-modal-diagnostics').textContent.includes(stage)",arg=stage)
                        page.clock.fast_forward(6000)
                        log=page.locator('#directory-modal-diagnostics').text_content()
                        self.assertTrue(any('WAIT #' in line and stage in line for line in log.splitlines()),log)
                        if stage=='Accessibility: createWritable(probe)':
                            self.assertIn('Permission: requestPermission(readwrite) -> granted',log)
                        self.assertTrue(page.locator('#directory-modal').is_visible())
                        self.assertNotIn('Main directory authorised.',page.locator('#directory-message').text_content())
                    finally:browser.close()

    def test_diagnostics_record_indexeddb_events_exceptions_and_completion(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                log=page.locator('#directory-diagnostic-log').text_content()
                for stage in ('HANDLE RECEIVED','PERMISSION RESPONSE: granted','LOCK ACQUIRED','SERVER RESPONSE: HTTP 200','Accessibility: close probe writer','INDEXEDDB indexeddb-put-start','INDEXEDDB indexeddb-transaction-complete','Completion callback','Modal closure'):
                    self.assertIn(stage,log)
                self.assertFalse(page.locator('#directory-modal').is_visible())
                page.evaluate("""() => {
                    window.dispatchEvent(new ErrorEvent('error',{error:new TypeError('Diagnostic JavaScript exception')}));
                    window.dispatchEvent(new PromiseRejectionEvent('unhandledrejection',{promise:Promise.resolve(),reason:new Error('Diagnostic rejected promise')}));
                }""")
                log=page.locator('#directory-diagnostic-log').text_content()
                self.assertIn('TypeError: Diagnostic JavaScript exception',log)
                self.assertIn('UNHANDLED PROMISE REJECTION: Error: Diagnostic rejected promise',log)
                page.evaluate("() => {FileSystemFileHandle.prototype.createWritable=async()=>{throw new DOMException('Diagnostic write denial','NotAllowedError');};}")
                self.drop(page,'Diagnostic failure')
                page.wait_for_function("document.getElementById('directory-modal-diagnostics').textContent.includes('NotAllowedError: Diagnostic write denial')")
                self.assertTrue(page.locator('#directory-modal').is_visible())
            finally:browser.close()

    def test_obsolete_handle_with_unresolved_permission_times_out_and_fresh_selection_recovers(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                # Delete only this test's isolated directory to reproduce an obsolete registration.
                page.evaluate("async()=> (await navigator.storage.getDirectory()).removeEntry('Main',{recursive:true})")
                page.add_init_script("""window.latePermission=[];window.freshAllowed=false;window.permissionActions=[];
                    const query=FileSystemDirectoryHandle.prototype.queryPermission;
                    FileSystemDirectoryHandle.prototype.queryPermission=function(options){
                        if(this.name==='Main')return new Promise(resolve=>latePermission.push(resolve));
                        if(this.name==='Fresh' && !freshAllowed)return Promise.resolve('prompt');
                        return query.call(this,options);
                    };
                    FileSystemDirectoryHandle.prototype.requestPermission=async function(options){permissionActions.push({mode:options.mode,active:navigator.userActivation.isActive});freshAllowed=true;return 'granted';};
                    window.showDirectoryPicker=async()=> (await navigator.storage.getDirectory()).getDirectoryHandle('Fresh',{create:true});""")
                page.clock.install()
                page.goto('https://ait.test'+self.new_shop())
                page.wait_for_function("document.getElementById('directory-diagnostic-log').textContent.includes('Shop creation: query main-directory permission')")
                self.assertTrue(page.evaluate("async()=>navigator.locks.request('ait-pm-local-report-save',{ifAvailable:true},lock=>!!lock)"))
                page.clock.fast_forward(11000)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('did not respond within 10 seconds')")
                self.assertTrue(page.locator('#directory-select').is_enabled())
                self.assertEqual(page.evaluate("async()=> (await AITDirectories.get({user:'1',property:'protrade'})).handle.name"),'Main')
                page.locator('#directory-select').click();page.locator('#directory-browse').click()
                page.wait_for_function("document.getElementById('directory-message').textContent.startsWith('Verified directories:')")
                self.assertEqual(self.tree(page,'Fresh'),{'ProTrade':{'Owner':{'Happy Shop':{'Invoice':{},'Statement':{}}}}})
                self.assertEqual(page.evaluate('permissionActions'),[dict(mode='readwrite',active=True)])
                # A late response from the obsolete handle cannot restart its stopped attempt.
                page.evaluate("() => {latePermission.forEach(resolve=>resolve('granted'));}")
                page.wait_for_function("document.getElementById('directory-diagnostic-log').textContent.includes('LATE PERMISSION RESPONSE IGNORED')")
                self.assertEqual(page.evaluate("async()=> (await AITDirectories.get({user:'1',property:'protrade'})).handle.name"),'Fresh')
                self.assertTrue(page.locator('#directory-message').text_content().startswith('Verified directories:'))
                self.assertTrue(page.evaluate("async()=>navigator.locks.request('ait-pm-local-report-save',{ifAvailable:true},lock=>!!lock)"))
            finally:browser.close()

    def test_permission_query_inside_accessibility_verification_cannot_hold_lock_indefinitely(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/main-directory');self.drop(page)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('read/write access verified')")
                page.add_init_script("""const query=FileSystemDirectoryHandle.prototype.queryPermission;
                    FileSystemDirectoryHandle.prototype.queryPermission=function(options){return this.name==='ProTrade'?new Promise(()=>{}):query.call(this,options);};""")
                page.clock.install();page.goto('https://ait.test'+self.new_shop())
                page.wait_for_function("document.getElementById('directory-diagnostic-log').textContent.includes('Accessibility: query read/write permission')")
                self.assertFalse(page.evaluate("async()=>navigator.locks.request('ait-pm-local-report-save',{ifAvailable:true},lock=>!!lock)"))
                page.clock.fast_forward(11000)
                page.wait_for_function("document.getElementById('directory-message').textContent.includes('did not respond within 10 seconds')")
                self.assertTrue(page.evaluate("async()=>navigator.locks.request('ait-pm-local-report-save',{ifAvailable:true},lock=>!!lock)"))
                self.assertEqual(self.tree(page),{'ProTrade':{}})
                self.assertTrue(page.locator('#directory-select').is_enabled())
            finally:browser.close()

    def test_signed_event_owner_expiry_and_existing_shop_exclusion(self):
        location=self.new_shop()
        query=urlsplit(location).query
        response=self.client.get('/mechanic/main-directory/context?'+query)
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['shop']['name'],'Happy Shop')
        self.assertIsNone(self.client.get('/mechanic/main-directory/context').json['shop'])
        self.assertIsNone(self.client.get('/mechanic/main-directory/context?shop_id='+str(self.existing.id)).json['shop'])
        self.assertEqual(self.client.get('/mechanic/main-directory/context?registration=forged').status_code,400)
        from itsdangerous import SignatureExpired
        with patch.object(setup,'signer') as signer:
            signer.return_value.loads.side_effect=SignatureExpired('expired')
            self.assertEqual(self.client.get('/mechanic/main-directory/context?'+query).status_code,400)
        with self.client.session_transaction() as session:session['_user_id']='2'
        self.assertEqual(self.client.get('/mechanic/main-directory/context?'+query).status_code,403)
        with self.client.session_transaction() as session:session.pop('_user_id',None)
        self.assertNotEqual(self.client.get('/mechanic/main-directory/context').status_code,200)


if __name__=='__main__':unittest.main()
