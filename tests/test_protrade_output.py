"""Stage 3I Output: actual adapters/routes on temporary PostgreSQL; no live mail."""
import importlib.util
import sys
import unittest
from unittest.mock import patch
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from lxml import html
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_shop_register as base

spec=importlib.util.spec_from_file_location('app.protrade_output',base.ROOT/'app/protrade_output.py')
output=importlib.util.module_from_spec(spec);spec.loader.exec_module(output)
sys.modules['app.protrade_output']=output


class ProTradeOutputTests(unittest.TestCase):
    shop=base.ShopRegisterTests.shop

    def setUp(self):
        base.ShopRegisterTests.setUp(self)
        for name,value in dict(db=SimpleNamespace(session=self.db_session),MechShop=base.models.MechShop,
            MechClient=base.models.MechClient,MechVehicle=base.models.MechVehicle,MechJobCard=base.models.MechJobCard,
            MechInvoice=base.catalog_globals['MechInvoice'],MechLaborLine=base.catalog_globals['MechLaborLine'],
            MechPartLine=base.catalog_globals['MechPartLine'],MechCatalogPart=base.MechCatalogPart,
            ProTradeBoqItem=base.ProTradeBoqItem,ProTradeBoqQuote=base.ProTradeBoqQuote,
            ProTradeOutputEmail=base.ProTradeOutputEmail,Debtor=base.models.Debtor,DebtorLedger=base.DebtorLedger).items():
            setattr(output,name,value)
        output.price_items=self.ns['selected_price_list']
        self.carpentry=self.shop(name='Happy Shop',trade='carpentry',status='active')
        self.second=self.shop(name='Happy Shop',trade='carpentry',status='active')
        self.mechanic=self.shop(name='Mechanic Shop',trade='mechanic',status='active')
        self.foreign=self.shop(owner=2,name='Foreign Shop',trade='carpentry',status='active')
        self.debtor=self.contact(self.carpentry,'Debtor Jane','jane@example.com')
        self.other_debtor=self.contact(self.second,'Other Shop Debtor','other@example.com')
        self.foreign_debtor=self.contact(self.foreign,'Foreign Debtor','foreign@example.com')
        self.mech_debtor=self.contact(self.mechanic,'Mechanic Customer','mechanic@example.com')
        self.quote=self.boq_quote(self.carpentry,self.debtor,'CAR-Q-OUR')
        self.other_quote=self.boq_quote(self.second,self.other_debtor,'CAR-Q-OTHER')
        self.foreign_quote=self.boq_quote(self.foreign,self.foreign_debtor,'CAR-Q-FOREIGN')
        self.price=base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',code='CAR-004',description='Install skirting',unit='m',material_rate=85,labour_rate=45)
        self.db_session.add(self.price)
        self.db_session.add_all([base.DebtorLedger(debtor_id=self.debtor.id,txn_date=date(2026,11,1),description='Existing charge',kind='debit',amount=10000),base.DebtorLedger(debtor_id=self.debtor.id,txn_date=date(2026,11,2),description='Existing payment',kind='credit',amount=2500)])
        vehicle=base.models.MechVehicle(client_id=self.mech_debtor.reference_id,make='Stored vehicle')
        self.db_session.add(vehicle);self.db_session.flush()
        self.job=base.models.MechJobCard(shop_id=self.mechanic.id,debtor_id=self.mech_debtor.id,job_number='JOB-OUR',vehicle_id=vehicle.id,status='Billed',vat_rate=15)
        self.db_session.add(self.job);self.db_session.flush()
        self.invoice=base.catalog_globals['MechInvoice'](job_card_id=self.job.id,subtotal=100,tax=15,total=115,status='Unpaid')
        self.db_session.add(self.invoice);self.db_session.commit()
        # Apply actual production migration, preserving all source records.
        self.conn.exec_driver_sql('DROP TABLE protrade_output_email')
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user" (id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES (1),(2)')
        spec=importlib.util.spec_from_file_location('output_migration',base.ROOT/'migrations/versions/protrade_output_007.py')
        self.migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
        self.migration.op=Operations(MigrationContext.configure(self.conn));self.migration.upgrade()

    def contact(self,shop,name,email):
        client=base.models.MechClient(user_id=shop.user_id,shop_id=shop.id,name=name,phone='0123456789',email=email,address='Saved debtor address')
        self.db_session.add(client);self.db_session.flush()
        debtor=base.models.Debtor(user_id=shop.user_id,shop_id=shop.id,reference_id=client.id,name=name,phone=client.phone,email=email,address=client.address,slug_reference='protrade')
        self.db_session.add(debtor);self.db_session.commit()
        return debtor

    def boq_quote(self,shop,debtor,number):
        quote=base.ProTradeBoqQuote(user_id=shop.user_id,shop_id=shop.id,client_id=debtor.reference_id,debtor_id=debtor.id,trade='carpentry',quote_number=number,
            client_name=debtor.name,client_phone=debtor.phone,client_email=debtor.email,client_address=debtor.address,
            items=[dict(source_item_id=1,code='CAR-004',description='Install skirting',unit='m',quantity='2.5',material_rate='85.00',labour_rate='45.00',total='325.00')],grand_total=Decimal('325.00'))
        self.db_session.add(quote);self.db_session.commit();return quote

    def preview(self,selection=None,shop=None,recipient_id=''):
        return self.client.get('/mechanic/output',query_string=dict(shop_id=(shop or self.carpentry).id,selection=selection or f'quote:boq-{self.quote.id}',recipient_id=recipient_id,preview='1'))

    def fields(self,response):
        tree=html.fromstring(response.data)
        return {node.get('name'):node.get('value','') for node in tree.xpath('//form[@id="output-email-form"]/input')}

    def email(self,fields,expected=302,**changes):
        data=dict(fields,confirm_email='yes');data.update(changes)
        response=self.client.post('/mechanic/output',data=data)
        self.assertEqual(response.status_code,expected,response.get_data(as_text=True)[:300]);return response

    def test_catalog_and_preview_are_shop_and_owner_scoped(self):
        response=self.preview()
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.data)
        report=tree.get_element_by_id('protrade-output-document').text_content()
        self.assertIn('Happy Shop',report);self.assertIn('Debtor Jane',report);self.assertIn('325.00',report)
        choices=tree.xpath('//select[@id="output-selection"]/optgroup/@label')
        self.assertEqual(choices,['Quotations','Debtor Statements','Price Lists'])
        self.assertNotIn('CAR-Q-OTHER',response.get_data(as_text=True));self.assertNotIn('CAR-Q-FOREIGN',response.get_data(as_text=True))
        self.assertEqual(self.preview(shop=self.second).status_code,404)
        self.assertEqual(self.preview(shop=self.foreign).status_code,404)
        self.assertEqual(self.preview(recipient_id=str(self.other_debtor.id)).status_code,400)

    def test_invoice_ledger_and_price_list_use_existing_values(self):
        response=self.preview(f'invoice:stored-{self.invoice.id}',self.mechanic)
        text=html.fromstring(response.data).get_element_by_id('protrade-output-document').text_content()
        self.assertIn('JOB-OUR',text);self.assertIn('Mechanic Customer',text);self.assertIn('115.00',text)
        response=self.preview(f'statement:debtor-{self.debtor.id}')
        text=html.fromstring(response.data).get_element_by_id('protrade-output-document').text_content()
        self.assertIn('Existing charge',text);self.assertIn('Existing payment',text);self.assertIn('75.00',text)
        response=self.preview('price_list:all',recipient_id=str(self.debtor.id))
        text=html.fromstring(response.data).get_element_by_id('protrade-output-document').text_content()
        self.assertIn('Install skirting',text);self.assertIn('130.00',text)
        self.assertEqual(base.catalog_globals['MechInvoice'].query.count(),1)

    def test_pdf_requires_current_signed_preview_and_correct_scope(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test') as renderer:
            response=self.client.get('/mechanic/output',query_string=dict(fields,format='pdf'))
            self.assertEqual(response.status_code,200);self.assertEqual(response.mimetype,'application/pdf')
            self.assertTrue(response.data.startswith(b'%PDF-'));self.assertIn('Shop-',response.headers['Content-Disposition'])
            self.assertEqual(self.client.get('/mechanic/output',query_string=dict(fields,format='pdf',shop_id=self.second.id)).status_code,404)
            self.quote.grand_total=Decimal('999');self.db_session.commit()
            self.assertEqual(self.client.get('/mechanic/output',query_string=dict(fields,format='pdf')).status_code,409)
            self.assertEqual(renderer.call_count,1)

    def test_email_confirmation_saved_address_and_durable_duplicate_prevention(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_pdf_email',return_value=True) as sender:
            self.email(fields,expected=400,confirm_email='')
            self.assertEqual(sender.call_count,0);self.assertEqual(base.ProTradeOutputEmail.query.count(),0)
            self.email(fields,email_to='forged@example.com')
            self.assertEqual(sender.call_args.args[0],'jane@example.com')
            self.email(fields,expected=409)
            self.assertEqual(sender.call_count,1)
            self.assertEqual(base.ProTradeOutputEmail.query.one().status,'accepted')

    def test_failed_smtp_is_not_automatically_retried(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_pdf_email',side_effect=RuntimeError('timeout')) as sender:
            self.email(fields,expected=503)
            self.email(fields,expected=409)
            self.assertEqual(sender.call_count,1)
            self.assertEqual(base.ProTradeOutputEmail.query.one().status,'uncertain')

    def test_changed_debtor_or_bad_tokens_never_send(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_pdf_email') as sender:
            self.email(fields,expected=400,preview_token='forged')
            self.debtor.email='changed@example.com';self.db_session.commit()
            self.email(fields,expected=409)
            self.assertEqual(sender.call_count,0)

    def test_pdf_failure_and_receipt_failure_never_send(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',side_effect=RuntimeError('renderer unavailable')),patch('app.utils.mailer.send_pdf_email') as sender:
            with self.assertLogs(self.app.logger,level='ERROR'):
                self.email(fields,expected=503)
            self.assertEqual(sender.call_count,0);self.assertEqual(base.ProTradeOutputEmail.query.count(),0)
        self.conn.exec_driver_sql("ALTER TABLE protrade_output_email ADD CONSTRAINT reject_output_mail CHECK (status<>'attempted')")
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_pdf_email') as sender:
            self.email(fields,expected=503)
            self.assertEqual(sender.call_count,0)

    def test_smtp_success_then_receipt_failure_still_blocks_duplicates(self):
        fields=self.fields(self.preview())
        self.conn.exec_driver_sql("ALTER TABLE protrade_output_email ADD CONSTRAINT reject_accept CHECK (status<>'accepted')")
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_pdf_email',return_value=True) as sender:
            self.email(fields,expected=503)
            self.email(fields,expected=409)
            self.assertEqual(sender.call_count,1)
            self.assertEqual(base.ProTradeOutputEmail.query.one().status,'attempted')

    def test_existing_ait_pdf_renderer_creates_readable_report_documents(self):
        import fitz
        folder=base.ROOT/'tmp'/'pdfs';folder.mkdir(parents=True,exist_ok=True)
        selections=[(f'quote:boq-{self.quote.id}',self.carpentry,'quote'),(f'invoice:stored-{self.invoice.id}',self.mechanic,'invoice'),(f'statement:debtor-{self.debtor.id}',self.carpentry,'statement'),('price_list:all',self.carpentry,'price_list')]
        for selection,shop,label in selections:
            fields=self.fields(self.preview(selection,shop))
            response=self.client.get('/mechanic/output',query_string=dict(fields,format='pdf'))
            self.assertEqual(response.status_code,200)
            self.assertTrue(response.data.startswith(b'%PDF-'))
            pdf=fitz.open(stream=response.data,filetype='pdf')
            text=''.join(page.get_text() for page in pdf)
            self.assertIn(shop.business_name,text)
            self.assertNotIn('Foreign Debtor',text)
            self.assertEqual(len(pdf),1)
            (folder/f'protrade-output-{label}.pdf').write_bytes(response.data)
            pdf[0].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).save(str(folder/f'protrade-output-{label}.png'))
            pdf.close()

    def test_expired_preview_and_missing_saved_email_do_not_send(self):
        from itsdangerous import SignatureExpired
        fields=self.fields(self.preview())
        with patch.object(output,'signer') as signer,patch('app.utils.mailer.send_pdf_email') as sender:
            signer.return_value.loads.side_effect=SignatureExpired('expired')
            self.email(fields,expected=400)
            sender.assert_not_called()
        self.debtor.email='';self.db_session.commit()
        preview=self.preview()
        self.assertTrue(html.fromstring(preview.data).xpath('//button[@id="output-email" and @disabled]'))
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_pdf_email') as sender:
            self.email(self.fields(preview),expected=400)
            sender.assert_not_called()
        self.assertEqual(base.ProTradeOutputEmail.query.count(),0)

    def browser_route(self,route):
        from urllib.parse import urlsplit
        url=urlsplit(route.request.url)
        if url.path.startswith('/static/'):
            path=base.ROOT/'app'/url.path.lstrip('/')
            route.fulfill(status=200,content_type='text/javascript',body=path.read_text(encoding='utf-8'))
        else:
            response=self.client.get(url.path+('?' + url.query if url.query else ''))
            route.fulfill(status=response.status_code,content_type=response.content_type,body=response.data)

    def test_browser_folder_structure_collision_safety_print_and_confirmation(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlencode
        environment=r"""
        (()=>{
          function directory(name){
            const node={kind:'directory',name,children:new Map(),files:new Map()};
            node.queryPermission=async()=> 'granted';node.requestPermission=async()=> 'granted';
            node.getDirectoryHandle=async(name,options={})=>{
              if(!node.children.has(name)){if(!options.create)throw new DOMException('Missing','NotFoundError');node.children.set(name,directory(name));}
              return node.children.get(name);
            };
            node.getFileHandle=async(name,options={})=>{
              if(!node.files.has(name)){if(!options.create)throw new DOMException('Missing','NotFoundError');node.files.set(name,'');}
              return {kind:'file',name,getFile:async()=>new Blob([node.files.get(name)]),createWritable:async(options)=>{
                if(options.mode!=='exclusive')throw new Error('Must use exclusive writer');
                let content='';return {write:async blob=>{content=await blob.text();},close:async()=>node.files.set(name,content),abort:async()=>{}};
              }};
            };
            return node;
          }
          window.folderRoot=directory('AIT-ProTrade');window.assignments=new Map();
          const store={createIndex(){}};
          const database={createObjectStore:()=>store,close(){},transaction(){
            const tx={objectStore:()=>({get(key){const req={};setTimeout(()=>{req.result=assignments.get(key);req.onsuccess?.();tx.oncomplete?.();},0);return req;},put(value){const req={};setTimeout(()=>{assignments.set(value.key,value);req.result=value.key;req.onsuccess?.();tx.oncomplete?.();},0);return req;}})};return tx;
          }};
          Object.defineProperty(window,'indexedDB',{value:{open(){const req={};setTimeout(()=>{req.result=database;req.onupgradeneeded?.();req.onsuccess?.();},0);return req;}}});
          window.showDirectoryPicker=async()=>folderRoot;
          window.print=()=>{window.didPrint=true;};
        })();
        """
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.add_init_script(environment)
                page.route('http://localhost/**',self.browser_route)
                with patch.object(output,'pdf_bytes',side_effect=lambda data,claim: ('%PDF-Shop-'+str(data['issuer']['id'])).encode()):
                    page.goto('http://localhost/mechanic/output?'+urlencode(dict(shop_id=self.carpentry.id,selection=f'quote:boq-{self.quote.id}',preview='1')))
                    page.locator('#output-email').click()
                    self.assertTrue(page.locator('#output-email-form').is_visible())
                    self.assertFalse(page.locator('[name=confirm_email]').is_checked())
                    page.locator('#output-print').click();self.assertTrue(page.evaluate('window.didPrint'))
                    page.locator('#output-folder-select').click()
                    page.wait_for_function("document.getElementById('output-folder-name').textContent==='AIT-ProTrade'")
                    prefix=page.evaluate("async()=>{const scope=JSON.parse(document.getElementById('output-directory-context').textContent),entry=JSON.parse(document.getElementById('directory-reports').textContent).find(item=>item.code==='quote');const folder=await folderRoot.getDirectoryHandle(entry.source);const type=await folder.getDirectoryHandle(entry.title);const d=scope.period_end;const prefix=d.slice(8,10)+d.slice(5,7)+d.slice(2,4)+'_Quotations';type.files.set(prefix+'.pdf','ORIGINAL');return prefix;}")
                    page.locator('#output-save').click()
                    page.wait_for_function("document.getElementById('output-save-message').textContent.startsWith('Saved ')")
                    result=page.evaluate("async()=>{const scope=JSON.parse(document.getElementById('output-directory-context').textContent);const second=await AITDirectories.save(scope,'quote',scope.pdf_url);const entry=JSON.parse(document.getElementById('directory-reports').textContent).find(item=>item.code==='quote'),folder=await (await folderRoot.getDirectoryHandle(entry.source)).getDirectoryHandle(entry.title);return {files:Array.from(folder.files.entries()),second,assignments:Array.from(window.assignments.values()).map(row=>({user:row.user,property:row.property}))};}")
                    files=dict(result['files']);self.assertEqual(files[prefix+'.pdf'],'ORIGINAL')
                    self.assertEqual(files[prefix+'(1).pdf'],f'%PDF-Shop-{self.carpentry.id}')
                    self.assertEqual(result['second'],prefix+'(2).pdf')
                    self.assertEqual(result['assignments'],[dict(user='1',property='protrade')])
                    other=self.preview(f'quote:boq-{self.other_quote.id}',self.second)
                    tree=html.fromstring(other.data)
                    reports=tree.get_element_by_id('directory-reports').text
                    other_url=tree.get_element_by_id('output-download').get('href')
                    page.evaluate("reports=>document.getElementById('directory-reports').textContent=reports",reports)
                    page.evaluate("async url=>{const scope=JSON.parse(document.getElementById('output-directory-context').textContent);await AITDirectories.save(scope,'quote',url);}",other_url)
                    sources=page.evaluate('Array.from(folderRoot.children.keys())')
                    self.assertEqual(len(sources),2)
                    self.assertTrue(any(f'Shop-{self.carpentry.id}-' in source for source in sources))
                    self.assertTrue(any(f'Shop-{self.second.id}-' in source for source in sources))
                self.assertEqual(base.ProTradeOutputEmail.query.count(),0)
            finally:browser.close()

    def test_browser_download_fallback_uses_distinct_filenames(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlencode
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page(accept_downloads=True)
                page.add_init_script("Object.defineProperty(window,'showDirectoryPicker',{value:undefined});")
                page.route('http://localhost/**',self.browser_route)
                with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'):
                    page.goto('http://localhost/mechanic/output?'+urlencode(dict(shop_id=self.carpentry.id,selection=f'quote:boq-{self.quote.id}',preview='1')))
                    self.assertTrue(page.locator('#output-folder-select').is_disabled())
                    names=[]
                    for _ in range(2):
                        with page.expect_download() as download:
                            page.locator('#output-save').click()
                        names.append(download.value.suggested_filename)
                    self.assertNotEqual(names[0],names[1]);self.assertTrue(all(name.endswith('.pdf') for name in names))
            finally:browser.close()

    def test_output_does_not_change_documents_ledgers_prices_or_debtors(self):
        tables=('mech_shops','debtor','mech_clients','mech_job_cards','mech_invoices','protrade_boq_quotes','protrade_boq_items','debtor_ledger')
        before={table:self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all() for table in tables}
        self.preview();self.preview(f'statement:debtor-{self.debtor.id}');self.preview('price_list:all')
        for table,rows in before.items():self.assertEqual(self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all(),rows)
        self.assertEqual(base.ProTradeOutputEmail.query.count(),0)


if __name__=='__main__':
    unittest.main(verbosity=2)
