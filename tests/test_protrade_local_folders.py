"""ProTrade local folders: isolated PostgreSQL receipts and real browser handles."""
import importlib.util,json,mimetypes
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import patch
import unittest
from urllib.parse import urlsplit
from alembic.migration import MigrationContext
from alembic.operations import Operations
from flask_login import login_required
from flask_wtf.csrf import CSRFProtect,generate_csrf
from playwright.sync_api import sync_playwright
from sqlalchemy.exc import IntegrityError
from lxml import html
import fitz
import test_protrade_output as fixture
import test_protrade_shop_register as base
import app.protrade_folders as folders

class ProTradeLocalFoldersTests(unittest.TestCase):
    def setUp(self):
        self.o=fixture.ProTradeOutputTests(methodName='test_catalog_and_preview_are_shop_and_owner_scoped');self.o.setUp();self.addCleanup(self.o.doCleanups)
        for name,value in dict(db=SimpleNamespace(session=self.o.db_session),MechShop=base.models.MechShop,ProTradeOutputEmail=base.ProTradeOutputEmail).items():
            mock=patch.object(folders,name,value);mock.start();self.addCleanup(mock.stop)
        self.app=self.o.app;self.client=self.o.client
        self.app.add_url_rule('/mechanic/folders/access',endpoint='mechanic_bp.protrade_folder_access',view_func=login_required(folders.access_context))
        self.app.add_url_rule('/mechanic/folders/email',endpoint='mechanic_bp.protrade_folder_email',view_func=login_required(folders.email_history),methods=['POST'])
        spec=importlib.util.spec_from_file_location('local_files_migration',base.ROOT/'migrations/versions/protrade_local_files_020.py')
        self.migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
        self.migration.op=Operations(MigrationContext.configure(self.o.conn));self.migration.upgrade()
        pdf=fitz.open()
        for number in range(2):page=pdf.new_page();page.insert_text((72,72),'Original saved ProTrade page '+str(number+1))
        self.pdf=pdf.tobytes();pdf.close()
        self.request=str(uuid4())
    def snapshot(self):
        return {table:self.o.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in ['users','mech_shops','mech_clients','debtor','debtor_ledger','protrade_boq_quotes','mech_invoices']}
    def send(self,**changes):
        values=dict(property_id='protrade',report=str(self.o.carpentry.id)+':quote',request_id=self.request,email_to='recipient@example.com',document=(BytesIO(self.pdf),'Original quote with spaces.pdf'))
        values.update(changes);return self.client.post('/mechanic/folders/email',data=values)
    def browser(self,runtime):
        browser=runtime.chromium.launch(headless=True);page=browser.new_page()
        def serve(route):
            url=urlsplit(route.request.url)
            if url.path.startswith('/static/'):
                file=(base.ROOT/'app'/url.path.lstrip('/')).resolve()
                if file.is_relative_to((base.ROOT/'app/static').resolve()) and file.is_file():route.fulfill(body=file.read_bytes(),content_type='text/javascript' if file.suffix in ['.js','.mjs'] else mimetypes.guess_type(str(file))[0] or 'application/octet-stream');return
            if route.request.method=='POST':
                route.abort();return
            response=self.client.get(url.path+('?' + url.query if url.query else ''))
            route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
        page.route('**/*',serve)
        page.add_init_script("""window.showDirectoryPicker=()=>{throw Error('Retired picker forbidden');};window.printed=[];
            if(window!==window.top)window.print=()=>window.parent.printed.push(document.querySelectorAll('canvas').length);""")
        return browser,page
    def drop(self,page,name='ProTrade'):
        page.wait_for_function("!document.getElementById('directory-select').disabled")
        page.locator('#directory-select').click()
        page.evaluate("""async name=>{const handle=await(await navigator.storage.getDirectory()).getDirectoryHandle(name,{create:true});await document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve(handle)}]}});}""",name)
        page.wait_for_function("document.getElementById('directory-select').hidden")

    def test_route_registry_only_owned_businesses_and_new_migration_preserves_records(self):
        before=self.snapshot();response=self.client.get('/mechanic/folders');self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data);reports=json.loads(tree.xpath('//script[@id="directory-reports"]/text()')[0])
        self.assertTrue(reports);self.assertEqual({r['shop'] for r in reports},{str(self.o.carpentry.id),str(self.o.second.id),str(self.o.mechanic.id)})
        self.assertNotIn('Foreign Shop',response.text)
        self.assertTrue(all(r['owner']['user']=='1' for r in reports))
        self.assertNotEqual(next(r['directories'] for r in reports if r['code']==str(self.o.carpentry.id)+':quote'),next(r['directories'] for r in reports if r['code']==str(self.o.second.id)+':quote'))
        self.assertEqual(self.client.get('/mechanic/folders?shop_id='+str(self.o.foreign.id)).status_code,404)
        self.assertEqual(self.migration.down_revision,'protrade_trade_codes_019');self.assertEqual(before,self.snapshot())
        # Ordinary sends still cannot lose their debtor linkage.
        with self.assertRaises(IntegrityError):
            with self.o.conn.begin_nested():self.o.conn.exec_driver_sql("INSERT INTO protrade_output_email(request_key,user_id,shop_id,debtor_id,selection,recipient,status) VALUES('bad',1,%s,NULL,'quote:any','r@example.com','attempted')",(self.o.carpentry.id,))

    def test_history_email_exact_bytes_duplicate_prevention_scope_and_tokens(self):
        before=self.snapshot()
        with patch('app.utils.mailer.send_pdf_email',return_value=True) as send,patch('app.protrade_documents.charge_document',side_effect=AssertionError('Historical access must not charge tokens')):
            response=self.send();self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(send.call_args.args[3:],(self.pdf,'Original quote with spaces.pdf'))
            self.assertEqual(self.send().status_code,409);self.assertEqual(send.call_count,1)
            self.assertEqual(self.send(report=str(self.o.foreign.id)+':quote').status_code,404)
        self.assertEqual(before,self.snapshot())
        receipt=base.ProTradeOutputEmail.query.filter_by(user_id=1).one();self.assertIsNone(receipt.debtor_id);self.assertEqual(receipt.status,'accepted')
    def test_history_email_csrf_and_uncertain_delivery_do_not_retry(self):
        CSRFProtect(self.app);self.app.add_url_rule('/test-folder-csrf',view_func=lambda:generate_csrf())
        self.assertEqual(self.send().status_code,400)
        token=self.client.get('/test-folder-csrf').text
        with patch('app.utils.mailer.send_pdf_email',side_effect=RuntimeError('SMTP timeout')) as send:
            self.assertEqual(self.send(csrf_token=token).status_code,503)
            self.assertEqual(self.send(csrf_token=token).status_code,409);send.assert_called_once()
    def test_selection_lazy_saving_register_original_view_print_email_and_replacement_guard(self):
        before=self.snapshot()
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                self.assertEqual(page.evaluate("async()=>{const root=await(await navigator.storage.getDirectory()).getDirectoryHandle('ProTrade');return Array.fromAsync(root.keys());}"),['Owner'])
                page.reload();page.wait_for_function("document.getElementById('directory-select').hidden")
                page.route('**/snapshot.pdf',lambda route:route.fulfill(body=self.pdf,content_type='application/pdf'))
                shops=[self.o.carpentry,self.o.second]
                for shop in shops:
                    for kind in ['quote','invoice','statement']:
                        page.evaluate("async value=>AITDirectories.save({user:'1',property:'protrade',period_end:'2026-11-22'},value,'/snapshot.pdf')",str(shop.id)+':'+kind)
                page.evaluate("async value=>AITDirectories.save({user:'1',property:'protrade',period_end:'2026-11-22'},value,'/snapshot.pdf')",str(shops[0].id)+':quote')
                structure=page.evaluate("""async()=>{const root=await(await navigator.storage.getDirectory()).getDirectoryHandle('ProTrade'),users=[];for await(const [,user]of root.entries())if(user.kind==='directory'){const businesses=[];for await(const [,business]of user.entries())if(business.kind==='directory')businesses.push({name:business.name,children:(await Array.fromAsync(business.keys())).filter(n=>!n.startsWith('.')).sort()});users.push(businesses);}return users;}""")
                self.assertEqual(len(structure),1);self.assertEqual(len(structure[0]),2)
                self.assertNotEqual(structure[0][0]['name'],structure[0][1]['name'])
                self.assertTrue(all(b['children']==['Invoices','Quotes','Statements'] for b in structure[0]))
                page.locator('#folder-register-open').click();page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                quotes=page.locator('#folder-register-rows tr').filter(has=page.get_by_role('button',name='Owner / Happy Shop (business '+str(shops[0].id)+') / Quotes',exact=True))
                self.assertEqual(quotes.locator('td').nth(2).inner_text(),'2')
                quotes.locator('button').click();page.wait_for_function("document.getElementById('folder-register-files').children.length===2")
                with patch.object(fixture.output,'pdf_bytes',side_effect=AssertionError('Do not regenerate history')),patch('app.protrade_documents.charge_document',side_effect=AssertionError('Do not charge history')):
                    page.locator('#folder-register-files button').first.click();page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('2 page(s)')")
                    url=page.locator('#folder-view-pages').get_attribute('data-object-url')
                    self.assertEqual(bytes(page.evaluate('async url=>Array.from(new Uint8Array(await(await fetch(url)).arrayBuffer()))',url)),self.pdf)
                    page.locator('#folder-view-print').click();page.wait_for_function('printed.length===1');self.assertEqual(page.evaluate('printed'),[2])
                    page.evaluate("document.getElementById('folder-view-pdf').contentWindow.dispatchEvent(new Event('afterprint'))")
                    page.locator('#folder-view-email').click();page.wait_for_function("document.getElementById('folder-email-to').value==='one@example.test'");self.assertEqual(page.locator('#folder-email-to').input_value(),'one@example.test')
                    page.locator('#folder-email-to').fill('recipient@example.com');sent=[]
                    def capture(route):sent.append(route.request.post_data_buffer);route.fulfill(json=dict(message='Original accepted'))
                    page.route('**/mechanic/folders/email',capture);page.locator('#folder-view-email-form button').click();page.wait_for_function("document.getElementById('folder-register-message').textContent==='Original accepted'")
                    self.assertIn(self.pdf,sent[0])
                self.assertEqual(before,self.snapshot())
            finally:browser.close()
    def test_output_save_buttons_use_shared_saver_for_all_document_types(self):
        from urllib.parse import urlencode
        selections=[(f'quote:boq-{self.o.quote.id}',self.o.carpentry,'Quotes'),
                    (f'invoice:stored-{self.o.invoice.id}',self.o.mechanic,'Invoices'),
                    (f'statement:debtor-{self.o.debtor.id}',self.o.carpentry,'Statements'),
                    ('price_list:all',self.o.carpentry,'Price Lists')]
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                with patch.object(fixture.output,'pdf_bytes',return_value=self.pdf):
                    for selection,shop,destination in selections:
                        fields=self.o.fields(self.o.preview(selection,shop))
                        page.goto('https://ait.test/mechanic/output?'+urlencode(dict(fields,preview='1')))
                        page.wait_for_function('window.AITDirectories !== undefined')
                        self.assertTrue(page.locator('#output-save').is_enabled())
                        page.locator('#output-save').click()
                        page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Saved ')")
                        first=page.locator('#output-save-message').text_content()
                        page.locator('#output-save').click()
                        page.wait_for_function("document.getElementById('output-save-message').textContent.includes('(1).pdf')")
                        self.assertNotIn('(1).pdf',first)
                        files=page.evaluate("""async () => {
                            const scope=JSON.parse(document.getElementById('output-directory-context').textContent);
                            return await Promise.all((await AITDirectories.list(scope,scope.report,{prompt:false})).map(async file=>({
                                folder:file.folder,bytes:Array.from(new Uint8Array(await(await file.handle.getFile()).arrayBuffer()))
                            })));
                        }""")
                        self.assertEqual(len(files),2)
                        self.assertTrue(all(file['folder'].endswith('/'+destination) for file in files))
                        self.assertTrue(all(bytes(file['bytes'])==self.pdf for file in files))
                        # Force a write failure; a successful save must never be claimed.
                        page.evaluate("() => {FileSystemFileHandle.prototype.createWritable=async()=>{throw new Error('Disk write failed');};}")
                        page.locator('#output-save').click()
                        page.wait_for_function("document.getElementById('output-save-message').textContent.includes('Disk write failed')")
            finally:browser.close()

    def test_denied_permission_creates_no_assignment_or_directories(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.locator('#directory-select').click()
                page.evaluate("""async()=>{FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';const root=await navigator.storage.getDirectory(),h=await root.getDirectoryHandle('Denied',{create:true});await document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve(h)}]}});FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';}""")
                page.locator('#directory-drop-zone').click();page.wait_for_function("document.getElementById('directory-message').textContent.includes('not granted')")
                self.assertFalse(page.evaluate("AITDirectories.get({user:'1',property:'protrade'})"))
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await(await navigator.storage.getDirectory()).getDirectoryHandle('Denied')).keys())"),[])
            finally:browser.close()
    def test_stale_consultant_tab_cannot_print_another_sessions_history(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                page.route('**/snapshot.pdf',lambda route:route.fulfill(body=self.pdf,content_type='application/pdf'))
                page.evaluate("async code=>AITDirectories.save({user:'1',property:'protrade',period_end:'2026-11-22'},code,'/snapshot.pdf')",str(self.o.carpentry.id)+':quote')
                page.locator('#folder-register-open').click();page.get_by_role('button',name='Owner / Happy Shop (business '+str(self.o.carpentry.id)+') / Quotes',exact=True).click();page.locator('#folder-register-files button').first.click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('page(s)')")
                with self.client.session_transaction() as session:session['_user_id']='2'
                page.locator('#folder-view-print').click();page.wait_for_function("document.getElementById('directory-message').textContent.includes('access changed')")
                self.assertTrue(page.locator('#folder-register-area').is_hidden());self.assertEqual(page.evaluate('printed'),[])
            finally:browser.close()

    def test_output_save_button_uses_owned_reference_and_preserves_financial_records(self):
        before=self.snapshot()
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                with patch.object(fixture.output,'pdf_bytes',return_value=self.pdf),patch('app.protrade_documents.charge_document',side_effect=AssertionError('No additional charges')):
                    page.goto('https://ait.test/mechanic/output?shop_id='+str(self.o.carpentry.id)+'&selection=quote:boq-'+str(self.o.quote.id)+'&preview=1')
                    page.locator('#output-save').click();page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Saved ')")
                self.assertIn('CAR-Q-OUR',page.locator('#output-save-message').inner_text())
                self.assertEqual(before,self.snapshot())
            finally:browser.close()

    def test_same_named_consultants_sharing_base_remain_isolated_after_business_rename(self):
        other=self.o.db_session.get(base.models.User,2);other.name='Owner';self.o.db_session.commit()
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.route('**/snapshot.pdf',lambda route:route.fulfill(body=self.pdf,content_type='application/pdf'))
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                page.evaluate("async code=>AITDirectories.save({user:'1',property:'protrade',period_end:'2026-11-22'},code,'/snapshot.pdf')",str(self.o.carpentry.id)+':quote')
                with self.client.session_transaction() as session:session['_user_id']='2'
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                page.evaluate("async code=>AITDirectories.save({user:'2',property:'protrade',period_end:'2026-11-22'},code,'/snapshot.pdf')",str(self.o.foreign.id)+':quote')
                page.locator('#folder-register-open').click();page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                self.assertNotIn('Happy Shop',page.locator('#folder-register-rows').inner_text())
                self.assertIn('Foreign Shop',page.locator('#folder-register-rows').inner_text())
                denied=page.evaluate("async()=>{try{await AITDirectories.get({user:'1',property:'protrade'});return false;}catch(_){return true;}}")
                self.assertTrue(denied)
                keys=page.evaluate("async()=>Array.fromAsync((await(await navigator.storage.getDirectory()).getDirectoryHandle('ProTrade')).keys())")
                self.assertEqual(len(keys),2)
                self.o.carpentry.business_name='Renamed Business';self.o.db_session.commit()
                with self.client.session_transaction() as session:session['_user_id']='1'
                page.goto('https://ait.test/mechanic/folders');page.locator('#folder-register-open').click()
                row=page.locator('#folder-register-rows tr').filter(has=page.get_by_role('button',name='Owner / Renamed Business (business '+str(self.o.carpentry.id)+') / Quotes',exact=True))
                page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                self.assertEqual(row.locator('td').nth(2).inner_text(),'1')
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await(await navigator.storage.getDirectory()).getDirectoryHandle('ProTrade')).keys())"),keys)
            finally:browser.close()

    def test_operational_migration_preserves_other_alembic_heads_and_receipts(self):
        from alembic.config import Config
        from alembic import command
        receipt=base.ProTradeOutputEmail(request_key='existing-output-receipt',user_id=1,shop_id=self.o.carpentry.id,debtor_id=self.o.debtor.id,selection='quote:existing',recipient='recipient@example.com',status='accepted')
        self.o.db_session.add(receipt);self.o.db_session.commit()
        old_receipts=self.o.conn.exec_driver_sql('SELECT * FROM protrade_output_email ORDER BY id').all()
        self.o.conn.exec_driver_sql('ALTER TABLE protrade_output_email DROP CONSTRAINT ck_protrade_email_history_debtor')
        self.o.conn.exec_driver_sql('ALTER TABLE protrade_output_email ALTER COLUMN debtor_id SET NOT NULL')
        self.o.conn.exec_driver_sql('CREATE TEMP TABLE alembic_version(version_num varchar(32) PRIMARY KEY)')
        self.o.conn.exec_driver_sql("INSERT INTO alembic_version VALUES('ait_franchisee_001'),('pm_bank_id_003')")
        before=self.snapshot()
        config=Config(str(base.ROOT/'migrations/alembic.ini'));config.set_main_option('script_location',str(base.ROOT/'migrations'))
        config.attributes.update(connection=self.o.conn,skip_app_metadata=True)
        command.upgrade(config,'protrade_local_files_020')
        heads=set(self.o.conn.exec_driver_sql('SELECT version_num FROM alembic_version').scalars())
        self.assertEqual(heads,{'ait_franchisee_001','pm_bank_id_003','protrade_local_files_020'})
        self.assertEqual(before,self.snapshot())
        self.assertEqual(old_receipts,self.o.conn.exec_driver_sql('SELECT * FROM protrade_output_email ORDER BY id').all())
        self.assertEqual(self.o.conn.exec_driver_sql("SELECT relpersistence FROM pg_class WHERE oid='alembic_version'::regclass").scalar(),'t')

    def test_earlier_marked_folders_are_read_without_moving_or_regenerating_files(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                page.route('**/earlier.pdf',lambda route:route.fulfill(body=self.pdf,content_type='application/pdf'))
                page.evaluate("async code=>AITDirectories.save({user:'1',property:'protrade',period_end:'2026-11-22'},code,'/earlier.pdf')",str(self.o.carpentry.id)+':legacy_quotes')
                page.locator('#folder-register-open').click()
                legacy=page.get_by_role('button',name='Owner / Happy Shop (business '+str(self.o.carpentry.id)+') / Earlier / Quotes & Invoices',exact=True)
                legacy.click();page.wait_for_function("document.getElementById('folder-register-files').children.length===1")
                page.locator('#folder-register-files button').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.includes('2 page(s)')")
                url=page.locator('#folder-view-pages').get_attribute('data-object-url')
                self.assertEqual(bytes(page.evaluate('async url=>Array.from(new Uint8Array(await(await fetch(url)).arrayBuffer()))',url)),self.pdf)
            finally:browser.close()

    def test_anonymous_access_and_history_email_are_blocked(self):
        with self.client.session_transaction() as session:session.pop('_user_id',None)
        self.assertEqual(self.client.get('/mechanic/folders').status_code,401)
        self.assertEqual(self.client.get('/mechanic/folders/access').status_code,401)
        self.assertEqual(self.send().status_code,401)

    def test_dropped_folder_authorisation_preserves_lazy_destinations(self):
        before=self.snapshot()
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders')
                page.wait_for_function("!document.getElementById('directory-select').disabled")
                self.assertTrue(page.locator('#folder-register-open').is_disabled())
                self.assertEqual(page.locator('#folder-register-rows tr').count(),0)
                self.drop(page,'Empty ProTrade')
                page.evaluate("async()=>{window.emptyHandle=await(await navigator.storage.getDirectory()).getDirectoryHandle('Empty ProTrade');}")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Empty ProTrade')
                consultant=page.evaluate("""async()=>{const root=await(await navigator.storage.getDirectory()).getDirectoryHandle('Empty ProTrade'),user=await root.getDirectoryHandle('Owner');return {children:await Array.fromAsync(user.keys()),marker:JSON.parse(await(await(await user.getFileHandle('.ait-folder-owner.json')).getFile()).text())};}""")
                self.assertEqual(consultant,dict(children=['.ait-folder-owner.json'],marker=dict(program='protrade',user='1',kind='user',id='1')))

                self.assertEqual(page.evaluate('async()=>Array.fromAsync(emptyHandle.keys())'),['Owner'])
                page.locator('#folder-register-open').click();page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                self.assertEqual(page.locator('#folder-register-rows tr').count(),0)
                self.assertTrue(page.locator('#protrade-register-empty').is_visible())
                self.assertEqual(page.evaluate('async()=>Array.fromAsync(emptyHandle.keys())'),['Owner'])
                page.route('**/snapshot.pdf',lambda route:route.fulfill(body=self.pdf,content_type='application/pdf'))
                for shop,kind,count in [(self.o.carpentry,'quote',1),(self.o.second,'statement',2)]:
                    page.evaluate("async code=>AITDirectories.save({user:'1',property:'protrade',period_end:'2026-11-22'},code,'/snapshot.pdf')",str(shop.id)+':'+kind)
                    page.wait_for_function("count=>document.getElementById('folder-register-rows').children.length===count",arg=count)
                self.assertEqual(page.locator('#folder-register-rows td:first-child').all_text_contents(),['1','2'])
                page.wait_for_function("Array.from(document.querySelectorAll('#folder-register-rows td:last-child')).every(cell=>cell.textContent==='1')")
                self.assertEqual(page.locator('#folder-register-rows td:last-child').all_text_contents(),['1','1'])
                labels=page.locator('#folder-register-rows button').all_text_contents()
                self.assertTrue(any(label.endswith('/ Quotes') for label in labels))
                self.assertTrue(any(label.endswith('/ Statements') for label in labels))
                self.assertFalse(any('/ Invoices' in label or '/ Earlier /' in label for label in labels))
                self.assertTrue(page.locator('#protrade-register-empty').is_hidden())
                page.reload();page.wait_for_function("document.getElementById('directory-select').hidden")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Empty ProTrade')
                page.evaluate("()=>{window.pickerCalls=0;window.showDirectoryPicker=()=>{pickerCalls++;throw Error('Must preserve assignment');};document.getElementById('directory-select').onclick();}")
                self.assertEqual(page.evaluate('pickerCalls'),0)
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                self.assertEqual(page.locator('#folder-register-rows tr').count(),2)
                self.assertEqual(before,self.snapshot())
            finally:browser.close()

    def test_modal_cancel_does_not_assign_or_change_financial_records(self):
        before=self.snapshot()
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders')
                page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.locator('#directory-select').click()
                self.assertTrue(page.get_by_role('heading',name='Select Main Folder').is_visible())
                self.assertEqual(page.locator('input[type=file]').count(),0)
                page.locator('#directory-modal-close').click()
                self.assertFalse(page.evaluate("AITDirectories.get({user:'1',property:'protrade'})"))
                self.assertTrue(page.locator('#folder-register-open').is_disabled())
                self.assertEqual(page.locator('#folder-register-rows tr').count(),0)
                self.assertEqual(before,self.snapshot())
            finally:browser.close()

    def test_existing_and_earlier_destinations_remain_registered_when_read_access_expires(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');self.drop(page)
                page.route('**/snapshot.pdf',lambda route:route.fulfill(body=self.pdf,content_type='application/pdf'))
                for kind in ['quote','legacy_quotes']:
                    page.evaluate("async code=>AITDirectories.save({user:'1',property:'protrade',period_end:'2026-11-22'},code,'/snapshot.pdf')",str(self.o.carpentry.id)+':'+kind)
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                self.assertEqual(page.locator('#folder-register-rows tr').count(),2)
                page.wait_for_function("Array.from(document.querySelectorAll('#folder-register-rows td:last-child')).every(cell=>cell.textContent==='1')")
                self.assertEqual(page.locator('#folder-register-rows td:last-child').all_text_contents(),['1','1'])
                labels=page.locator('#folder-register-rows button').all_text_contents()
                self.assertTrue(any('/ Earlier / Quotes & Invoices' in label for label in labels))
                page.evaluate("()=>{window.originalReadPermission=FileSystemDirectoryHandle.prototype.queryPermission;FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';}")
                page.locator('#folder-register-open').click();page.locator('#folder-register-access').wait_for(state='visible')
                self.assertEqual(page.locator('#folder-register-rows tr').count(),2)
                self.assertTrue(all(count==chr(0x2014) for count in page.locator('#folder-register-rows td:last-child').all_text_contents()))
                self.assertTrue(page.locator('#protrade-register-empty').is_hidden())
                page.evaluate('()=>{FileSystemDirectoryHandle.prototype.queryPermission=originalReadPermission;}')
                page.locator('#folder-register-open').click()
                page.wait_for_function("document.getElementById('folder-register-message').textContent.startsWith('Existing report folders')")
                page.wait_for_function("Array.from(document.querySelectorAll('#folder-register-rows td:last-child')).every(cell=>cell.textContent==='1')")
                self.assertEqual(page.locator('#folder-register-rows td:last-child').all_text_contents(),['1','1'])
            finally:browser.close()

    def test_authorisation_initialises_consultant_without_any_businesses(self):
        before=self.snapshot()
        with patch.object(folders,'owned_shops',return_value=[]),sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');self.drop(page,'Consultant without shops')
                self.assertEqual(page.evaluate("async()=>{const root=await(await navigator.storage.getDirectory()).getDirectoryHandle('Consultant without shops');return Array.fromAsync(root.keys());}"),['Owner'])
                self.assertEqual(page.evaluate("async()=>{const root=await(await navigator.storage.getDirectory()).getDirectoryHandle('Consultant without shops');return Array.fromAsync((await root.getDirectoryHandle('Owner')).keys());}"),['.ait-folder-owner.json'])
                page.reload();page.wait_for_function("document.getElementById('directory-select').hidden")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Consultant without shops')
                self.assertEqual(before,self.snapshot())
            finally:browser.close()

    def test_dropped_empty_folder_requires_actual_write_permission(self):
        with sync_playwright() as runtime:
            browser,page=self.browser(runtime)
            try:
                page.goto('https://ait.test/mechanic/folders');page.wait_for_function("!document.getElementById('directory-select').disabled")
                page.locator('#directory-select').click()
                page.evaluate("""async()=>{FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';window.deniedEmpty=await(await navigator.storage.getDirectory()).getDirectoryHandle('Empty without permission',{create:true});await document.getElementById('directory-drop-zone').ondrop({preventDefault(){},dataTransfer:{items:[{kind:'file',getAsFileSystemHandle:()=>Promise.resolve(deniedEmpty)}]}});
                    FileSystemDirectoryHandle.prototype.queryPermission=async()=> 'prompt';FileSystemDirectoryHandle.prototype.requestPermission=async()=> 'denied';}""")
                page.locator('#directory-drop-zone').click();page.wait_for_function("document.getElementById('directory-message').textContent.includes('not granted')")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Empty without permission')
                self.assertTrue(page.locator('#folder-register-open').is_disabled())
                self.assertFalse(page.evaluate("AITDirectories.get({user:'1',property:'protrade'})"))
                self.assertEqual(page.evaluate('async()=>Array.fromAsync(deniedEmpty.keys())'),[])
            finally:browser.close()
