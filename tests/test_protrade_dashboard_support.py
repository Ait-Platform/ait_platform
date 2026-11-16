"""ProTrade Folders/Help navigation and shared persistent browser folder storage."""
import json
import unittest
from unittest.mock import patch
from urllib.parse import urlencode
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_output as reports


class DashboardSupportTests(unittest.TestCase):
    def setUp(self):
        reports.ProTradeOutputTests.setUp(self)
        self.app.jinja_loader.loaders[0].mapping.pop('program_mechanic/menu.html',None)
    shop=reports.ProTradeOutputTests.shop
    contact=reports.ProTradeOutputTests.contact
    boq_quote=reports.ProTradeOutputTests.boq_quote
    browser_route=reports.ProTradeOutputTests.browser_route

    def test_menu_routes_ownership_guide_and_no_accounting_writes(self):
        tables=('debtor','debtor_ledger','protrade_boq_quotes','mech_job_cards','mech_invoices','mech_catalog_parts','protrade_boq_items','ait_token_wallet')
        available=set(__import__('sqlalchemy').inspect(self.conn).get_table_names())
        before={name:self.conn.exec_driver_sql('SELECT * FROM '+name+' ORDER BY id').all() for name in tables if name in available}
        source=(base.ROOT/'templates/program_mechanic/menu.html').read_text(encoding='utf-8')
        for shop in (self.carpentry,self.second,self.mechanic):
            dashboard=self.client.get('/mechanic/dashboard',query_string={'shop_id':shop.id})
            self.assertEqual(dashboard.status_code,200)
            dashboard_tree=html.fromstring(dashboard.data)
            self.assertTrue(dashboard_tree.xpath('//nav[@aria-label="ProTrade"]//a[text()="Folders"]'))
            self.assertTrue(dashboard_tree.xpath('//nav[@aria-label="ProTrade"]//a[text()="Help"]'))
            with self.app.test_request_context('/'):
                menu=html.fromstring(self.app.jinja_env.from_string(source).render(active_shop=shop,protrade_menu_active='dashboard'))
            self.assertEqual(menu.xpath('//a/text()'),['Price List','Debtor','Tokens','Output','Folders','Help'])
            for label,path in [('Folders','folders'),('Help','help')]:
                link=menu.xpath('//a[text()="'+label+'"]')[0].get('href')
                self.assertEqual(link,f'/mechanic/{path}?shop_id={shop.id}')
                page=self.client.get(link);self.assertEqual(page.status_code,200)
                tree=html.fromstring(page.data)
                self.assertEqual(tree.xpath('//a[text()="Back to Dashboard"]/@href'),[f'/mechanic/dashboard?shop_id={shop.id}'])
                self.assertNotIn('Foreign Shop',tree.text_content())
        folders=html.fromstring(self.client.get(f'/mechanic/folders?shop_id={self.carpentry.id}').data)
        self.assertFalse(folders.xpath('//a[normalize-space()="Open Output"]'))
        self.assertTrue(folders.xpath('//nav[@aria-label="ProTrade"]//a[normalize-space()="Output"]'))
        context=json.loads(folders.get_element_by_id('output-directory-context').text)
        self.assertEqual(context,dict(user='1',property='protrade'))
        paths=json.loads(folders.get_element_by_id('directory-reports').text)
        self.assertEqual(len({row['code'] for row in paths}),len(paths))
        self.assertEqual(len(paths),12)
        for path in ('folders','help'):
            self.assertEqual(self.client.get(f'/mechanic/{path}?shop_id={self.foreign.id}').status_code,404)
            self.assertEqual(self.client.get(f'/mechanic/{path}?shop_id=bad').status_code,404)
            self.assertEqual(self.client.get(f'/mechanic/{path}').status_code,200)
        guide=html.fromstring(self.client.get('/mechanic/help').data)
        self.assertEqual(len(guide.xpath('//section/h2')),9)
        for phrase in ('Record Payment','Save Changes','Save Report','Download PDF','Send PDF','R2,148.32','Tokens','Shop Setup','Search central debtor directory'):
            self.assertIn(phrase,guide.text_content())
        for name,rows in before.items():self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM '+name+' ORDER BY id').all(),rows)
        with self.client.session_transaction() as session:session.clear()
        for path in ('folders','help'):self.assertEqual(self.client.get('/mechanic/'+path).status_code,401)

    def test_browser_native_folder_persists_between_folders_output_and_shops(self):
        # A real serializable browser DirectoryHandle and real IndexedDB persist
        # across reloads. Only the native chooser is replaced for headless testing.
        environment="""window.showDirectoryPicker=async()=>{const root=await navigator.storage.getDirectory();return root.getDirectoryHandle('ProTrade-Test',{create:true});};"""
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page(accept_downloads=True);page.add_init_script(environment)
                page.route('http://localhost/**',self.browser_route)
                errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                folder_url=f'http://localhost/mechanic/folders?shop_id={self.carpentry.id}'
                page.goto(folder_url)
                page.locator('#output-folder-select').click()
                page.locator('#output-folder-allow').click()
                page.wait_for_function("document.getElementById('output-folder-name').textContent==='ProTrade-Test'")
                self.assertEqual(page.evaluate("async()=>{const root=(await AITDirectories.get({user:'1',property:'protrade'})).handle;const names=[];for await(const [name] of root.entries())names.push(name);return names;}"),[])
                page.reload()
                page.wait_for_function("document.getElementById('output-folder-name').textContent==='ProTrade-Test'")
                page.locator('#output-folder-allow').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Folder saved.')")
                page.evaluate("window.showDirectoryPicker=async()=>({kind:'directory',name:'Denied',queryPermission:async()=> 'denied',requestPermission:async()=> 'denied'})")
                page.locator('#output-folder-select').click()
                page.locator('#output-folder-allow').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.includes('previous saved folder is unchanged')")
                self.assertEqual(page.locator('#output-folder-name').inner_text(),'Denied')
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get(JSON.parse(document.getElementById('output-directory-context').textContent))).handle.name"),'ProTrade-Test')
                with patch.object(reports.output,'pdf_bytes' ,side_effect=lambda data,claim:('%PDF-Shop-'+str(data['issuer']['id'])).encode()):
                    paths=[]
                    for shop,quote in ((self.carpentry,self.quote),(self.second,self.other_quote)):
                        page.goto('http://localhost/mechanic/output?'+urlencode(dict(shop_id=shop.id,selection=f'quote:boq-{quote.id}',preview='1')))
                        page.wait_for_function("document.getElementById('output-folder-name').textContent==='ProTrade-Test'")
                        page.locator('#output-save').click()
                        page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Saved ')")
                        result=page.evaluate("""async()=>{const scope=JSON.parse(document.getElementById('output-directory-context').textContent),entry=JSON.parse(document.getElementById('directory-reports').textContent).find(row=>row.code==='quote'),saved=await AITDirectories.list(scope,'quote'),files=[];for(const file of saved)files.push([file.name,await(await file.handle.getFile()).text()]);return {source:saved[0].folder,title:entry.title,files};}""")
                        self.assertEqual(result['title'],'Quotations');self.assertEqual(len(result['files']),1)
                        self.assertEqual(result['files'][0][1],f'%PDF-Shop-{shop.id}')
                        paths.append(result['source'])
                        # Existing Download PDF remains usable, independently of folders.
                        self.assertTrue(page.locator('#output-download').is_visible())
                        response=self.client.get(page.locator('#output-download').get_attribute('href'))
                        self.assertEqual(response.status_code,200);self.assertTrue(response.data.startswith(b'%PDF-'))
                    self.assertNotEqual(paths[0],paths[1])
                self.carpentry.business_name='Renamed Business';self.db_session.commit()
                page.goto(folder_url)
                self.assertEqual(page.locator('#document-history-refresh').count(),0)
                page.wait_for_function("document.getElementById('document-history-message').textContent.startsWith('2 saved PDF')")
                page.evaluate("""async()=>{
                    const scope=JSON.parse(document.getElementById('output-directory-context').textContent),root=(await AITDirectories.get(scope)).handle;
                    const entry=JSON.parse(document.getElementById('directory-reports').textContent)[0];
                    const legacy=await root.getDirectoryHandle('Shop-'+entry.owner.client+'-Old_Business_Name',{create:true});
                    const quotes=await legacy.getDirectoryHandle('Quotations',{create:true});
                    const writer=await(await quotes.getFileHandle('Original historical quote.pdf',{create:true})).createWritable();
                    await writer.write('%PDF-HISTORICAL');await writer.close();
                    window.open=()=>{window.pdfViewer={location:{href:''},close(){},opener:null};return window.pdfViewer;};
                }""")
                page.evaluate("window.dispatchEvent(new Event('focus'))")
                page.wait_for_function("document.getElementById('document-history-message').textContent.startsWith('3 saved PDF')")
                row=page.locator('#document-history-files li').filter(has_text='Original historical quote.pdf')
                self.assertEqual(row.count(),1)
                row.get_by_role('button',name='View / Print').click()
                page.wait_for_function("window.pdfViewer.location.href.startsWith('blob:')")
                self.assertIn('Print control',page.locator('#document-history-message').inner_text())
                directories=page.evaluate("""async()=>{const root=(await AITDirectories.get({user:'1',property:'protrade'})).handle,user=await root.getDirectoryHandle('Owner'),paths=[];for await(const [,client] of user.entries())if(client.kind==='directory')for await(const [name,child] of client.entries())if(child.kind==='directory')paths.push(name);return paths;}""")
                self.assertEqual(directories,['Quotes & Invoices','Quotes & Invoices'])
                with self.client.session_transaction() as session:session['_user_id']='2'
                page.goto(f'http://localhost/mechanic/folders?shop_id={self.foreign.id}')
                self.assertTrue(page.evaluate("async()=>!(await AITDirectories.get(JSON.parse(document.getElementById('output-directory-context').textContent)))"))
                self.assertEqual(page.locator('#output-folder-name').inner_text(),'Not selected on this browser')
                page.locator('#output-folder-select').click()
                page.locator('#output-folder-allow').click()
                page.wait_for_function("document.getElementById('output-folder-name').textContent==='ProTrade-Test'")
                page.evaluate("window.dispatchEvent(new Event('focus'))")
                page.wait_for_function("document.getElementById('document-history-message').textContent.startsWith('No saved PDFs')")
                self.assertEqual(page.locator('#document-history-files li').count(),0)
                self.assertEqual(errors,[])
            finally:browser.close()

    def test_selection_displays_before_storage_and_failed_save_can_retry(self):
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.add_init_script("window.showDirectoryPicker=async()=>{const root=await navigator.storage.getDirectory();return root.getDirectoryHandle('Chosen Windows Folder',{create:true});};")
                page.goto(f'http://localhost/mechanic/folders?shop_id={self.carpentry.id}')
                page.locator('#output-folder-select').click()
                page.wait_for_function("document.getElementById('output-folder-name').textContent==='Chosen Windows Folder'")
                self.assertTrue(page.evaluate("async()=>!(await AITDirectories.get({user:'1',property:'protrade'}))"))
                page.evaluate("()=>{window.originalFolderSet=AITDirectories.set;AITDirectories.set=async()=>{throw new DOMException('Storage transaction failed','AbortError');};}")
                page.locator('#output-folder-allow').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.includes('Storage transaction failed')")
                self.assertNotIn('cancelled',page.locator('#output-save-message').inner_text())
                self.assertEqual(page.locator('#output-folder-name').inner_text(),'Chosen Windows Folder')
                page.evaluate('()=>{AITDirectories.set=window.originalFolderSet;}')
                page.locator('#output-folder-allow').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Folder saved.')")
                page.reload()
                page.wait_for_function("document.getElementById('output-folder-name').textContent==='Chosen Windows Folder'")
                self.assertEqual(page.locator('#output-folder-allow').inner_text(),'Save')
            finally:browser.close()

    def test_selection_does_not_require_write_permission_and_only_one_picker_opens(self):
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.add_init_script("""window.pickerCalls=0;window.pickerMode=null;
                    window.showDirectoryPicker=async options=>{
                        pickerCalls++;pickerMode=options.mode;
                        if(options.mode!=='read')throw new DOMException('Write permission was not approved during selection','AbortError');
                        await new Promise(resolve=>{window.completePicker=resolve;});
                        const root=await navigator.storage.getDirectory();
                        return root.getDirectoryHandle('Chosen ProTrade Folder',{create:true});
                    };""")
                page.goto(f'http://localhost/mechanic/folders?shop_id={self.carpentry.id}')
                page.evaluate("()=>{const button=document.getElementById('output-folder-select');button.click();button.click();}")
                self.assertEqual(page.evaluate('pickerCalls'),1)
                self.assertEqual(page.evaluate('pickerMode'),'read')
                self.assertTrue(page.locator('#output-folder-select').is_disabled())
                self.assertTrue(page.locator('#output-folder-allow').is_disabled())
                page.evaluate('()=>window.completePicker()')
                page.wait_for_function("document.getElementById('output-folder-name').textContent==='Chosen ProTrade Folder'")
                self.assertTrue(page.evaluate("async()=>!(await AITDirectories.get({user:'1',property:'protrade'}))"))
                page.locator('#output-folder-allow').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Folder saved.')")
                page.reload()
                page.wait_for_function("document.getElementById('output-folder-name').textContent==='Chosen ProTrade Folder'")
            finally:browser.close()

    def test_picker_error_preserves_the_actual_browser_reason(self):
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.add_init_script("window.showDirectoryPicker=async()=>{throw new DOMException('Directory access denied by browser','AbortError');};")
                page.goto(f'http://localhost/mechanic/folders?shop_id={self.carpentry.id}')
                page.locator('#output-folder-select').click()
                page.wait_for_function("document.getElementById('output-save-message').textContent.includes('Directory access denied by browser')")
                self.assertFalse(page.locator('#output-folder-select').is_disabled())
                self.assertTrue(page.locator('#output-folder-allow').is_disabled())
            finally:browser.close()

if __name__=='__main__':unittest.main()
