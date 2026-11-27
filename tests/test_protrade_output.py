from pathlib import Path
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

import app.protrade_output as output


class ProTradeOutputTests(unittest.TestCase):
    shop=base.ShopRegisterTests.shop

    def setUp(self):
        base.ShopRegisterTests.setUp(self)
        for name,value in dict(db=SimpleNamespace(session=self.db_session),MechShop=base.models.MechShop,
            MechClient=base.models.MechClient,MechVehicle=base.models.MechVehicle,MechJobCard=base.models.MechJobCard,
            MechInvoice=base.catalog_globals['MechInvoice'],MechLaborLine=base.catalog_globals['MechLaborLine'],
            MechPartLine=base.catalog_globals['MechPartLine'],MechCatalogPart=base.MechCatalogPart,
            ProTradeBoqItem=base.ProTradeBoqItem,ProTradeBoqQuote=base.ProTradeBoqQuote,
            ProTradeDocument=base.ProTradeDocument,ProTradeOutputEmail=base.ProTradeOutputEmail,Debtor=base.models.Debtor,DebtorLedger=base.DebtorLedger).items():
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

        import test_protrade_quote_ledger as ledger
        ledger.register_soa_routes(self)

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
        if selection and selection.startswith('statement:debtor-'):
            debtor_id=int(selection.split('-')[-1])
            generated=self.client.post(f'/debtor/{debtor_id}/soa',data={'action':'generate'})
            self.assertEqual(generated.status_code,302)
            saved=base.ProTradeDocument.query.filter_by(debtor_id=debtor_id,kind='soa').order_by(base.ProTradeDocument.id.desc()).first()
            selection='statement:generated-'+str(saved.id)
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
        choices=tree.xpath('//select[@id="output-category"]/option/text()')
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
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_standard_report_email',return_value=True) as sender:
            self.email(fields,expected=400,confirm_email='')
            self.assertEqual(sender.call_count,0);self.assertEqual(base.ProTradeOutputEmail.query.count(),0)
            self.email(fields,email_to='forged@example.com')
            self.assertEqual(sender.call_args.kwargs['to_email'],'jane@example.com')
            self.email(fields,expected=409)
            self.assertEqual(sender.call_count,1)
            self.assertEqual(base.ProTradeOutputEmail.query.one().status,'accepted')

    def test_failed_smtp_is_not_automatically_retried(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_standard_report_email',side_effect=RuntimeError('timeout')) as sender:
            self.email(fields,expected=503)
            self.email(fields,expected=409)
            self.assertEqual(sender.call_count,1)
            self.assertEqual(base.ProTradeOutputEmail.query.one().status,'uncertain')

    def test_changed_debtor_or_bad_tokens_never_send(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_standard_report_email') as sender:
            self.email(fields,expected=400,preview_token='forged')
            self.debtor.email='changed@example.com';self.db_session.commit()
            self.email(fields,expected=409)
            self.assertEqual(sender.call_count,0)

    def test_pdf_failure_and_receipt_failure_never_send(self):
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',side_effect=RuntimeError('renderer unavailable')),patch('app.utils.mailer.send_standard_report_email') as sender:
            with self.assertLogs(self.app.logger,level='ERROR'):
                self.email(fields,expected=503)
            self.assertEqual(sender.call_count,0);self.assertEqual(base.ProTradeOutputEmail.query.count(),0)
        self.conn.exec_driver_sql("ALTER TABLE protrade_output_email ADD CONSTRAINT reject_output_mail CHECK (status<>'attempted')")
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_standard_report_email') as sender:
            self.email(fields,expected=503)
            self.assertEqual(sender.call_count,0)

    def test_smtp_success_then_receipt_failure_still_blocks_duplicates(self):
        fields=self.fields(self.preview())
        self.conn.exec_driver_sql("ALTER TABLE protrade_output_email ADD CONSTRAINT reject_accept CHECK (status<>'accepted')")
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_standard_report_email',return_value=True) as sender:
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
            self.assertIn(shop.business_name.casefold(),text.casefold())
            self.assertNotIn('Foreign Debtor',text)
            self.assertEqual(len(pdf),1)
            (folder/f'protrade-output-{label}.pdf').write_bytes(response.data)
            pdf[0].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).save(str(folder/f'protrade-output-{label}.png'))
            pdf.close()

    def test_expired_preview_and_missing_saved_email_do_not_send(self):
        from itsdangerous import SignatureExpired
        fields=self.fields(self.preview())
        with patch.object(output,'signer') as signer,patch('app.utils.mailer.send_standard_report_email') as sender:
            signer.return_value.loads.side_effect=SignatureExpired('expired')
            self.email(fields,expected=400)
            sender.assert_not_called()
        self.debtor.email='';self.db_session.commit()
        preview=self.preview()
        self.assertFalse(html.fromstring(preview.data).xpath('//button[@id="output-email" and @disabled]'))
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-test'),patch('app.utils.mailer.send_standard_report_email') as sender:
            self.email(self.fields(preview),expected=400)
            sender.assert_not_called()
        self.assertEqual(base.ProTradeOutputEmail.query.count(),0)

    def test_composer_overrides_and_duplicate_nonce_across_document_types(self):
        selections=[(f'quote:boq-{self.quote.id}',self.carpentry,''),
                    (f'invoice:stored-{self.invoice.id}',self.mechanic,''),
                    (f'statement:debtor-{self.debtor.id}',self.carpentry,''),
                    ('price_list:all',self.carpentry,str(self.debtor.id))]
        snapshot=lambda: {table:self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all() for table in ('debtor','mech_clients','protrade_boq_items','debtor_ledger')}
        before=snapshot()
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-selected'),patch('app.utils.mailer.send_standard_report_email',return_value=True) as sender:
            for selection,shop,recipient in selections:
                fields=self.fields(self.preview(selection,shop,recipient))
                self.email(fields,to_email='override@example.com',subject='Selected document',body_text='Custom message')
                self.assertEqual(tuple(sender.call_args.kwargs[key] for key in ('to_email','subject','message','pdf_bytes')),('override@example.com','Selected document','Custom message',b'%PDF-selected'))
                calls=sender.call_count
                self.email(fields,expected=409,to_email='another@example.com')
                self.assertEqual(sender.call_count,calls)
        self.assertEqual(before,snapshot())

    def test_email_validation_and_previous_receipts_are_preserved(self):
        import hashlib
        fields=self.fields(self.preview())
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-selected'),patch('app.utils.mailer.send_standard_report_email',return_value=True) as sender:
            for changes in ({'to_email':'invalid'}, {'subject':'bad\r\nheader'}, {'body_text':'x'*5001}):
                self.email(fields,expected=400,**changes)
            sender.assert_not_called()
            self.email(fields)
            with self.app.test_request_context('/'):
                claim=output.signer().loads(fields['preview_token'])
            receipt=base.ProTradeOutputEmail.query.one()
            receipt.request_key=hashlib.sha256(f'1:{self.carpentry.id}:{fields["selection"]}:{claim["nonce"]}:{self.debtor.email}'.encode()).hexdigest()
            self.db_session.commit()
            self.email(fields,expected=409,to_email='override@example.com')
            self.assertEqual(sender.call_count,1)

    def test_output_actions_without_folder_registration_or_authorisation(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlencode
        selections=[(f'quote:boq-{self.quote.id}',self.carpentry,''),
                    (f'invoice:stored-{self.invoice.id}',self.mechanic,''),
                    (f'statement:debtor-{self.debtor.id}',self.carpentry,''),
                    ('price_list:all',self.carpentry,str(self.debtor.id))]
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(accept_downloads=True)
                page.route('http://localhost/**',self.browser_route)
                page.add_init_script("""Object.defineProperty(window,'indexedDB',{get(){throw Error('Folder storage must not be accessed');}});
                    window.showDirectoryPicker=()=>{throw Error('Folder authorisation forbidden');};
                    window.print=()=>{window.didPrint=true;};""")
                with patch('app.protrade_folders.local_reports',side_effect=AssertionError('Output must not require folder metadata')),patch.object(output,'pdf_bytes',return_value=b'%PDF-independent'):
                    for selection,shop,recipient in selections:
                        fields=self.fields(self.preview(selection,shop,recipient))
                        page.goto('http://localhost/mechanic/output?'+urlencode(dict(fields,preview='1')))
                        self.assertEqual(page.locator('#output-save').count(),0)
                        self.assertEqual(page.get_by_role('link',name='Folders',exact=True).count(),0)
                        self.assertEqual(page.locator('script[src*="pm_local_directories"],script[src*="pm_output_save"],#output-directory-context').count(),0)
                        self.assertTrue(page.evaluate('window.AITDirectories === undefined'))
                        page.locator('#output-print').click()
                        self.assertTrue(page.evaluate('didPrint'))
                        with page.expect_download() as download:
                            page.locator('#output-download').click()
                        self.assertEqual(Path(download.value.path()).read_bytes(),b'%PDF-independent')
                        page.locator('#output-email').click()
                        self.assertTrue(page.locator('[name=to_email]').input_value())
                        self.assertTrue(page.locator('[name=subject]').input_value())
                        self.assertTrue(page.locator('[name=body_text]').input_value())
                        page.locator('[name=to_email]').fill('one-off@example.com')
                        page.locator('#output-email-cancel').click()
                        self.assertTrue(page.locator('#output-email-form').is_hidden())
            finally:browser.close()

    def test_auto_electrician_unified_report_same_in_preview_pdf_print_and_email(self):
        import fitz
        from urllib.parse import urlencode
        from playwright.sync_api import sync_playwright
        shop=self.shop(name='Short Circuit',trade='auto_elec',status='active')
        debtor=self.contact(shop,'Auto customer','auto@example.com')
        item=base.ProTradeBoqItem(user_id=1,shop_id=shop.id,trade='auto_elec',code='AUTO-STD-004',description='Repair starter motor',unit='Each',material_rate=Decimal('123.45'),labour_rate=Decimal('67.89'))
        self.db_session.add(item);self.db_session.commit()
        preview=self.preview('price_list:all',shop)
        tree=html.fromstring(preview.data);report=tree.get_element_by_id('protrade-output-document')
        self.assertEqual(report.xpath('.//th/text()'),['Item Code','Description','Unit','Material Rate','Labour Rate','Total Rate'])
        self.assertEqual(report.xpath('.//tbody/tr/td/text()'),['AUTO-STD-004','Repair starter motor','Each','123.45','67.89','191.34'])
        self.assertEqual(report.xpath('.//tbody/tr/td[position()>3]/@style'),['text-align:right']*3)
        self.assertFalse(tree.xpath('//nav[@aria-label="ProTrade"]|//select[@id="output-recipient"]'))
        fields=self.fields(preview)
        download=self.client.get('/mechanic/output',query_string=dict(fields,format='pdf'))
        self.assertEqual(download.status_code,200)
        def pdf_text(pdf):
            with fitz.open(stream=pdf,filetype='pdf') as document:return ''.join(page.get_text() for page in document)
        downloaded_text=pdf_text(download.data)
        for value in ['AUTO-STD-004','Repair starter motor','123.45','67.89','191.34']:self.assertIn(value,downloaded_text)
        with patch('app.utils.mailer.send_standard_report_email',return_value=True) as sender:
            self.email(fields,expected=404,email_debtor_id=str(self.other_debtor.id),to_email='once@example.com')
            sender.assert_not_called()
            self.email(fields,email_debtor_id=str(debtor.id),to_email='once@example.com',subject='Selected price list',body_text='Auto price list attached')
            self.assertEqual(pdf_text(sender.call_args.kwargs['pdf_bytes']),downloaded_text)
            self.assertEqual(debtor.email,'auto@example.com')
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.add_init_script("window.print=()=>{window.printedContent=document.getElementById('protrade-output-document').innerText;};")
                page.goto('http://localhost/mechanic/output?'+urlencode(dict(shop_id=shop.id,selection='price_list:all',preview='1')))
                page.locator('#output-print').click()
                self.assertIn('191.34',page.evaluate('printedContent'))
                page.locator('#output-email').click()
                page.locator('#email-debtor').select_option(str(debtor.id))
                self.assertEqual(page.locator('[name=to_email]').input_value(),'auto@example.com')
                self.assertTrue(page.locator('#output-month').is_disabled())
                self.assertTrue(page.locator('#output-from').is_disabled())
                self.assertTrue(page.locator('#output-to').is_disabled())
            finally:browser.close()

    def test_price_list_report_uses_unified_rate_columns_for_every_registered_trade(self):
        from app.protrade_boq_catalog import TRADE_CATALOGUES
        for trade in TRADE_CATALOGUES:
            with self.subTest(trade=trade):
                shop=self.shop(name='Report '+trade,trade=trade,status='active')
                self.db_session.add(base.ProTradeBoqItem(user_id=1,shop_id=shop.id,trade=trade,code='OWN-'+trade.upper(),description='Configured '+trade,unit='Each',material_rate=Decimal('10.25'),labour_rate=Decimal('5.75')))
                self.db_session.commit()
                response=self.preview('price_list:all',shop)
                self.assertEqual(response.status_code,200)
                report=html.fromstring(response.data).get_element_by_id('protrade-output-document')
                self.assertEqual(report.xpath('.//th/text()'),['Item Code','Description','Unit','Material Rate','Labour Rate','Total Rate'])
                rows=report.xpath('.//tbody/tr[td[1]="OWN-'+trade.upper()+'"]')
                self.assertEqual(len(rows),1)
                self.assertEqual(rows[0].xpath('./td/text()')[-3:],['10.25','5.75','16.00'])
                self.assertNotIn('Install skirting',report.text_content())

    def test_unconfigured_price_list_shows_actionable_message(self):
        shop=self.shop(name='New Auto shop',trade='auto_elec',status='active')
        response=self.preview('price_list:all',shop)
        self.assertEqual(response.status_code,200)
        report=html.fromstring(response.data).get_element_by_id('protrade-output-document')
        self.assertIn('No rates have been configured for this shop',report.text_content())
        self.assertFalse(report.xpath('.//table'))
        self.assertNotIn('Unit Price (R)',report.text_content())

    def test_category_and_period_controls_filter_saved_documents_without_changing_them(self):
        from datetime import datetime
        from urllib.parse import urlencode
        from playwright.sync_api import sync_playwright
        self.quote.created_at=datetime(2026,11,10);self.db_session.commit()
        before=self.quote.items.copy()
        response=self.client.get('/mechanic/output',query_string=dict(shop_id=self.carpentry.id,category='quote',by_month='2026-10'))
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//select[@id="output-selection"]/option/@value'),[''])
        self.assertEqual(self.client.get('/mechanic/output',query_string=dict(shop_id=self.carpentry.id,category='quote',date_from='2026-12-01',date_to='2026-11-01')).status_code,400)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost/mechanic/output?'+urlencode(dict(shop_id=self.carpentry.id,selection=f'quote:boq-{self.quote.id}',preview='1')))
                self.assertTrue(page.locator('#output-month').is_enabled())
                page.locator('#output-month').fill('2026-10');page.locator('#output-month').dispatch_event('change')
                self.assertEqual(page.locator('#output-selection option').count(),1)
                self.assertTrue(page.locator('#output-print').is_disabled())
                page.locator('#output-category').select_option('price_list')
                self.assertTrue(page.locator('#output-month').is_disabled())
                self.assertEqual(page.locator('#output-selection option').count(),2)
                self.assertEqual(page.locator('#output-document-controls select').count(),3)
                self.assertEqual(page.locator('#output-period-controls input').count(),3)
                self.assertEqual(page.locator('#output-actions').inner_text().split(),['Print','Download','PDF','Email'])
            finally:browser.close()
        self.assertEqual(self.quote.items,before)

    def apply_output_email_migration(self):
        spec=importlib.util.spec_from_file_location('output_email_022',base.ROOT/'migrations/versions/protrade_output_email_022.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.conn));migration.upgrade()
        return migration

    def test_price_list_without_debtors_opens_shared_composer_and_sends_through_standard_dispatcher(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlencode
        shop=self.shop(name='Short Circuit without debtors',trade='auto_elec',status='active')
        shop.email='registered@example.com';self.db_session.commit()
        preview=self.preview('price_list:all',shop);fields=self.fields(preview)
        self.assertFalse(html.fromstring(preview.data).xpath('//button[@id="output-email" and @disabled]'))
        # Existing databases fail closed until the receipt constraint is upgraded.
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-selected-preview'),patch('app.utils.mailer.send_standard_report_email',return_value=True) as sender:
            self.email(fields,expected=503,to_email='once@example.com')
            sender.assert_not_called()
            before=self.conn.exec_driver_sql('SELECT * FROM debtor ORDER BY id').all()
            self.apply_output_email_migration()
            self.assertEqual(before,self.conn.exec_driver_sql('SELECT * FROM debtor ORDER BY id').all())
            with sync_playwright() as runtime:
                browser=runtime.chromium.launch(headless=True)
                try:
                    page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                    page.goto('http://localhost/mechanic/output?'+urlencode(dict(shop_id=shop.id,selection='price_list:all',preview='1')))
                    page.locator('#output-email').click()
                    self.assertTrue(page.locator('#output-email-form').is_visible())
                    self.assertEqual(page.locator('[name=to_email]').input_value(),'registered@example.com')
                    self.assertEqual(page.locator('#email-debtor').count(),0)
                    page.locator('[name=to_email]').fill('once@example.com')
                    page.locator('[name=subject]').fill('Selected PDF subject')
                    page.locator('[name=body_text]').fill('Editable message')
                    page.locator('#output-email-cancel').click()
                    self.assertTrue(page.locator('#output-email-form').is_hidden())
                    page.locator('#output-email').click()
                    self.assertEqual(page.locator('[name=to_email]').input_value(),'once@example.com')
                finally:browser.close()
            self.email(fields,to_email='once@example.com',subject='Selected PDF subject',body_text='Editable message')
            self.assertEqual(sender.call_args.kwargs,dict(to_email='once@example.com',subject='Selected PDF subject',pdf_url='',pdf_bytes=b'%PDF-selected-preview',filename=sender.call_args.kwargs['filename'],message='Editable message',document_label='Price List'))
            self.email(fields,expected=409,to_email='changed@example.com')
            self.assertEqual(sender.call_count,1)
        receipt=base.ProTradeOutputEmail.query.one()
        self.assertIsNone(receipt.debtor_id)
        self.assertEqual(receipt.selection,'price_list:all')
        self.assertEqual(receipt.status,'accepted')
        self.assertEqual(shop.email,'registered@example.com')
        self.assertFalse(base.models.Debtor.query.filter_by(shop_id=shop.id).all())
        from sqlalchemy.exc import IntegrityError
        with self.assertRaises(IntegrityError):
            with self.conn.begin_nested():
                self.conn.exec_driver_sql("INSERT INTO protrade_output_email(request_key,user_id,shop_id,debtor_id,selection,recipient,status,created_at) VALUES ('blocked-without-debtor',1,%s,NULL,'quote:unlinked','once@example.com','attempted',now())",(shop.id,))

    def test_standard_dispatcher_builds_pdf_attachment_with_edited_message(self):
        from flask_mail import Mail
        self.app.config.update(MAIL_DEFAULT_SENDER='ait@example.com',MAIL_SUPPRESS_SEND=False)
        Mail(self.app)
        selections=[(f'quote:boq-{self.quote.id}',self.carpentry,''),
                    (f'invoice:stored-{self.invoice.id}',self.mechanic,''),
                    (f'statement:debtor-{self.debtor.id}',self.carpentry,''),
                    ('price_list:all',self.carpentry,str(self.debtor.id))]
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-current-preview'),patch('app.utils.mailer.mail.send') as transport:
            for count,(selection,shop,recipient) in enumerate(selections,1):
                fields=self.fields(self.preview(selection,shop,recipient))
                self.email(fields,to_email='edited@example.com',subject='Edited subject',body_text='Edited message')
                self.assertEqual(transport.call_count,count)
                message=transport.call_args.args[0]
                self.assertEqual(message.recipients,['edited@example.com'])
                self.assertEqual(message.subject,'Edited subject')
                self.assertIn('Edited message',message.body)
                self.assertEqual(len(message.attachments),1)
                self.assertEqual(message.attachments[0].data,b'%PDF-current-preview')
                self.assertEqual(message.attachments[0].content_type,'application/pdf')
                self.email(fields,expected=409,to_email='edited@example.com')
                self.assertEqual(transport.call_count,count)

    def test_email_migration_preserves_existing_history_and_linked_receipts(self):
        from sqlalchemy.exc import IntegrityError
        spec=importlib.util.spec_from_file_location('existing_email_history_020',base.ROOT/'migrations/versions/protrade_local_files_020.py')
        history=importlib.util.module_from_spec(spec);spec.loader.exec_module(history)
        history.op=Operations(MigrationContext.configure(self.conn));history.upgrade()
        for key,selection,debtor in [('ordinary','quote:existing',self.debtor.id),('history','history:quote',None)]:
            self.db_session.add(base.ProTradeOutputEmail(request_key=key,user_id=1,shop_id=self.carpentry.id,debtor_id=debtor,selection=selection,recipient='saved@example.com',status='accepted'))
        self.db_session.commit()
        before=self.conn.exec_driver_sql('SELECT * FROM protrade_output_email ORDER BY id').all()
        migration=self.apply_output_email_migration()
        self.assertEqual(migration.down_revision,'protrade_unified_prices_021')
        self.assertEqual(migration.depends_on,'protrade_local_files_020')
        self.assertEqual(before,self.conn.exec_driver_sql('SELECT * FROM protrade_output_email ORDER BY id').all())
        shop_id=self.carpentry.id
        with self.assertRaises(IntegrityError):
            with self.conn.begin_nested():
                self.conn.exec_driver_sql("INSERT INTO protrade_output_email(request_key,user_id,shop_id,debtor_id,selection,recipient,status,created_at) VALUES ('bad-invoice',1,%s,NULL,'invoice:any','once@example.com','attempted',now())",(shop_id,))

    def test_browser_send_and_cancel_use_standard_email_for_all_document_types(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlencode,urlsplit,parse_qs
        selections=[(f'quote:boq-{self.quote.id}',self.carpentry,''),
                    (f'invoice:stored-{self.invoice.id}',self.mechanic,''),
                    (f'statement:debtor-{self.debtor.id}',self.carpentry,''),
                    ('price_list:all',self.carpentry,str(self.debtor.id))]
        posted=[]
        def serve(route):
            if route.request.method!='POST':return self.browser_route(route)
            values={key:items[0] for key,items in parse_qs(route.request.post_data,keep_blank_values=True).items()}
            response=self.client.post(urlsplit(route.request.url).path,data=values)
            posted.append(response.status_code)
            # Resolve the app redirect inside this in-process browser fixture; no real localhost server is running.
            displayed=self.client.get(response.headers['Location']) if response.status_code==302 else response
            route.fulfill(status=displayed.status_code,body=displayed.data,content_type=displayed.content_type)
        with patch.object(output,'pdf_bytes',return_value=b'%PDF-displayed-preview'),patch('app.utils.mailer.send_standard_report_email',return_value=True) as sender,sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',serve)
                for count,(selection,shop,recipient) in enumerate(selections,1):
                    fields=self.fields(self.preview(selection,shop,recipient))
                    page.goto('http://localhost/mechanic/output?'+urlencode(dict(fields,preview='1')))
                    page.locator('#output-email').click()
                    self.assertTrue(page.locator('#output-email-form').is_visible())
                    page.locator('#output-email-cancel').click()
                    self.assertEqual(sender.call_count,count-1)
                    page.locator('#output-email').click()
                    page.locator('[name=to_email]').fill('browser@example.com')
                    page.locator('[name=subject]').fill('Browser subject')
                    page.locator('[name=body_text]').fill('Browser message')
                    page.get_by_role('button',name='Send Email',exact=True).click()
                    page.locator('#output-email[disabled]').wait_for()
                    self.assertEqual(sender.call_count,count)
                    self.assertEqual(sender.call_args.kwargs['pdf_bytes'],b'%PDF-displayed-preview')
                    self.assertEqual(sender.call_args.kwargs['to_email'],'browser@example.com')
                self.assertEqual(posted,[302]*4)
            finally:browser.close()

    def browser_route(self,route):
        from urllib.parse import urlsplit
        url=urlsplit(route.request.url)
        if url.path.startswith('/static/'):
            path=base.ROOT/'app'/url.path.lstrip('/')
            route.fulfill(status=200,content_type='text/javascript',body=path.read_text(encoding='utf-8'))
        else:
            response=self.client.get(url.path+('?' + url.query if url.query else ''))
            route.fulfill(status=response.status_code,content_type=response.content_type,body=response.data)

    def test_browser_normal_download_print_and_confirmation_without_folder_api(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlencode
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(accept_downloads=True)
                page.add_init_script("window.showDirectoryPicker=()=>{throw Error('Folder picker must not be called');};window.print=()=>{window.didPrint=true;};")
                page.route('http://localhost/**',self.browser_route)
                with patch.object(output,'pdf_bytes',return_value=b'%PDF-release-download'):
                    page.goto('http://localhost/mechanic/output?'+urlencode(dict(shop_id=self.carpentry.id,selection=f'quote:boq-{self.quote.id}',preview='1')))
                    self.assertEqual(page.get_by_role('heading',name='ProTrade Output',exact=True).inner_text(),'ProTrade Output')
                    self.assertEqual(page.locator('#output-folder-select, #output-folder-allow, #document-history-files').count(),0)
                    self.assertEqual(page.get_by_text('Shop Setup',exact=True).count(),0)
                    page.locator('#output-email').click()
                    self.assertTrue(page.locator('#output-email-form').is_visible())
                    self.assertEqual(page.locator('[name=to_email]').input_value(),self.debtor.email)
                    self.assertIn(self.quote.display_quote_number,page.locator('[name=subject]').input_value())
                    page.locator('[name=to_email]').fill('override@example.com')
                    page.locator('#output-email-cancel').click()
                    self.assertTrue(page.locator('#output-email-form').is_hidden())
                    page.locator('#output-email').click()
                    self.assertEqual(page.locator('[name=to_email]').input_value(),'override@example.com')
                    page.evaluate("""() => {
                        const form=document.getElementById('output-email-form');window.submissions=0;
                        form.addEventListener('submit',e=>{if(!e.defaultPrevented)window.submissions++;e.preventDefault();});
                        form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));
                        form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));
                    }""")
                    self.assertEqual(page.evaluate('submissions'),1)
                    page.locator('#output-print').click();self.assertTrue(page.evaluate('window.didPrint'))
                    with page.expect_download() as downloaded:page.locator('#output-download').click()
                    self.assertTrue(downloaded.value.suggested_filename.endswith('.pdf'))
                    self.assertEqual(Path(downloaded.value.path()).read_bytes(),b'%PDF-release-download')
                    page.locator('#output-shop').select_option(str(self.second.id))
                    page.wait_for_url('**/mechanic/output?shop_id='+str(self.second.id))
                    self.assertIn('CAR-Q-OTHER',page.locator('#output-selection').inner_text())
                    self.assertNotIn('CAR-Q-OUR',page.locator('#output-selection').inner_text())
                    self.assertNotIn('CAR-Q-FOREIGN',page.content())
                    self.assertTrue(page.locator('#output-download').is_disabled())
            finally:browser.close()

    def test_output_does_not_change_documents_ledgers_prices_or_debtors(self):
        tables=('mech_shops','debtor','mech_clients','mech_job_cards','mech_invoices','protrade_boq_quotes','protrade_boq_items','debtor_ledger')
        before={table:self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all() for table in tables}
        self.preview();self.preview(f'statement:debtor-{self.debtor.id}');self.preview('price_list:all')
        for table,rows in before.items():self.assertEqual(self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all(),rows)
        self.assertEqual(base.ProTradeOutputEmail.query.count(),0)


if __name__=='__main__':
    unittest.main(verbosity=2)
