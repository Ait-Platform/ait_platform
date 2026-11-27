"""Active regression coverage while all ProTrade Folder Management is suspended."""
from unittest.mock import patch
import unittest
from flask_login import login_required
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_main_directory as retained
import app.protrade_directory_setup as setup
import app.protrade_folders as folders


class FolderSuspensionTests(unittest.TestCase):
    browser=retained.MainDirectoryTests.browser

    def setUp(self):
        self.o=base.ShopRegisterTests();self.o.setUp();self.addCleanup(self.o.doCleanups)
        self.app=self.o.app;self.client=self.o.client
        for path,function,methods in [('/mechanic/folders/access',folders.access_context,['GET']),('/mechanic/folders/email',folders.email_history,['POST'])]:
            self.app.add_url_rule(path,'suspended_'+function.__name__,login_required(function),methods=methods)

    def test_shop_registration_and_dashboard_have_no_folder_dependencies(self):
        with patch.object(setup,'registration_token',side_effect=AssertionError('No directory event may be issued')),patch.object(setup,'context_data',side_effect=AssertionError('No folder context may be requested')):
            response=self.client.post('/mechanic/onboarding/process',data=dict(trade='painting',business_name='Independent shop',vat_registered='no'))
            self.assertEqual(response.status_code,302)
            self.assertIn('/mechanic/dashboard',response.location)
            self.assertNotIn('registration=',response.location)
            dashboard=self.client.get(response.location)
            self.assertEqual(dashboard.status_code,200)
            self.assertNotIn('Main Directory',dashboard.text)
            self.assertNotIn('js/protrade_main_directory.js',dashboard.text)
            self.assertNotIn('js/protrade_directory_diagnostics.js',dashboard.text)
            self.assertEqual(base.models.MechShop.query.one().business_name,'Independent shop')

    def test_bookmarks_and_folder_apis_are_blocked_before_any_folder_or_mail_work(self):
        with patch.object(setup,'context_data',side_effect=AssertionError('Folder context is suspended')),patch.object(folders,'owned_shops',side_effect=AssertionError('Folder access is suspended')),patch('app.utils.mailer.send_pdf_email',side_effect=AssertionError('Historical folder email is suspended')):
            for path in ('/mechanic/main-directory','/mechanic/main-directory?registration=old-event','/mechanic/main-directory/context','/mechanic/folders','/mechanic/folders/access'):
                response=self.client.get(path)
                self.assertEqual(response.status_code,503,path)
                self.assertIn('Folder Management is suspended',response.text)
                self.assertNotIn('<script',response.text)
            response=self.client.post('/mechanic/folders/email',data={'email_to':'one@example.com'})
            self.assertEqual(response.status_code,503)

    def test_registered_handle_and_historical_bytes_survive_without_automatic_queries(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/dashboard')
                page.add_script_tag(path=str(base.ROOT/'app/static/js/pm_local_directories.js'))
                page.evaluate("""async()=>{
                    const root=await(await navigator.storage.getDirectory()).getDirectoryHandle('Preserved main',{create:true});
                    const writer=await(await root.getFileHandle('Original.pdf',{create:true})).createWritable();
                    await writer.write('%PDF-preserved-history');await writer.close();
                    await AITDirectories.set({user:'1',property:'protrade'},root);
                }""")
                page.add_init_script("""window.folderCalls=[];window.originalOpen=indexedDB.open.bind(indexedDB);
                    indexedDB.open=()=>{folderCalls.push('IndexedDB');throw Error('No automatic folder storage access');};
                    window.showDirectoryPicker=()=>{folderCalls.push('picker');throw Error('No folder picker');};
                    FileSystemDirectoryHandle.prototype.queryPermission=()=>{folderCalls.push('permission');throw Error('No automatic permission query');};""")
                response=self.client.post('/mechanic/onboarding/process',data=dict(trade='painting',business_name='Independent shop',vat_registered='no'))
                page.goto('https://ait.test'+response.location)
                self.assertEqual(page.evaluate('folderCalls'),[])
                self.assertTrue(page.evaluate('window.ProTradeDirectoryDiagnostics===undefined && window.AITDirectories===undefined'))
                self.assertEqual(page.get_by_role('link',name='Main Directory',exact=True).count(),0)
                preserved=page.evaluate("""async()=>{
                    const db=await new Promise((resolve,reject)=>{const req=originalOpen('ait-pm-directories',1);req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error);});
                    const row=await new Promise((resolve,reject)=>{const req=db.transaction('assignments').objectStore('assignments').get(JSON.stringify(['1','protrade','__main__']));req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error);});
                    db.close();return {name:row.handle.name,pdf:await(await(await row.handle.getFileHandle('Original.pdf')).getFile()).text()};
                }""")
                self.assertEqual(preserved,dict(name='Preserved main',pdf='%PDF-preserved-history'))
            finally:browser.close()


if __name__=='__main__':unittest.main()
