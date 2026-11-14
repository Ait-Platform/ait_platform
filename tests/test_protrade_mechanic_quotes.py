"""Mechanic register, specific item editing, VAT and unique ledger posting."""
import importlib.util
from copy import deepcopy
from datetime import datetime,date
from decimal import Decimal
import unittest
from unittest.mock import Mock,patch
from urllib.parse import urlsplit
import sqlalchemy as sa
from sqlalchemy.orm import Query
from alembic.operations import Operations
from alembic.migration import MigrationContext
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_quote_ledger as ledger
import test_protrade_payment_methods as payments
import app.protrade_mechanic_quotes as mechanic


class Communication(base.models.Base):
    __tablename__='mech_communications'
    id=sa.Column(sa.Integer,primary_key=True)
    job_card_id=sa.Column(sa.Integer)
    comm_type=sa.Column(sa.String(50));recipient=sa.Column(sa.String(150))
    message=sa.Column(sa.Text);status=sa.Column(sa.String(50))
    created_at=sa.Column(sa.DateTime,default=datetime.utcnow)


class MechanicQuotationTests(unittest.TestCase):
    setUp=payments.PaymentMethodTests.setUp
    shop=payments.PaymentMethodTests.shop
    save_debtor=payments.PaymentMethodTests.save_debtor
    debtor=payments.PaymentMethodTests.debtor
    save_quote=payments.PaymentMethodTests.save_quote
    quote_url=payments.PaymentMethodTests.quote_url
    ledger_url=payments.PaymentMethodTests.ledger_url

    def fixtures(self,legacy=True,adopt=False,new_account=False,legacy_identity=False):
        self.app.config.update(MAIL_DEFAULT_SENDER='test@example.test',MAIL_SUPPRESS_SEND=True)
        from app.extensions import mail
        mail.init_app(self.app)
        payments.PaymentMethodTests.fixtures(self)
        mechanic.BusinessBankAccount=ledger.Bank
        def get_or_404(query,identifier):
            result=query.filter_by(id=identifier).first()
            if result is None:
                from flask import abort
                abort(404)
            return result
        patcher=patch.object(Query,'get_or_404',get_or_404,create=True);patcher.start();self.addCleanup(patcher.stop)
        self.save_debtor(self.mechanic,name='Mechanic Tomas')
        self.customer=self.debtor(self.mechanic)
        self.vehicle=base.models.MechVehicle(client_id=self.customer.reference_id,make='Toyota',model='Hilux',vin='VIN-UNCHANGED',license_plate='ABC123')
        self.db_session.add(self.vehicle);self.db_session.flush()
        self.job=base.models.MechJobCard(id=18,shop_id=self.mechanic.id,debtor_id=None if legacy else self.customer.id,quotation_seq=1,vehicle_id=self.vehicle.id,job_number='JOB-LEGACY-18',status='Quote',vat_rate=15,created_at=datetime(2026,11,14,8))
        self.db_session.add(self.job);self.db_session.flush()
        self.part=base.catalog_globals['MechPartLine'](job_card_id=18,part_number='PART-1',description='Existing part',quantity=1,unit_cost=300,markup_price=660.32)
        self.labour=base.catalog_globals['MechLaborLine'](job_card_id=18,mechanic_name='Tech',description='Existing labour',time_in='08:00',time_out='10:00',hours=2,rate_per_hour=100)
        self.db_session.add_all([self.part,self.labour]);self.db_session.commit()
        self.old_charge=None
        if adopt:
            self.old_charge=base.DebtorLedger(debtor_id=self.customer.id,txn_date=date(2026,11,14),kind='debit',amount=98937,description='Original legacy charge',ref='JOB-'+self.job.job_number)
            self.db_session.add(self.old_charge);self.db_session.commit()
        if legacy_identity:
            self.customer.shop_id=None;self.customer.reference_id=None;self.customer.slug_reference='mechanic'
            self.db_session.commit()
        if new_account:
            self.db_session.delete(self.customer);self.db_session.commit()
        self.conn.exec_driver_sql('ALTER TABLE debtor_ledger DROP COLUMN mech_job_card_id CASCADE')
        spec=importlib.util.spec_from_file_location('mechanic_migration',base.ROOT/'migrations/versions/protrade_mechanic_quotes_012.py')
        self.migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
        self.migration.op=Operations(MigrationContext.configure(self.conn));self.migration.upgrade()
        self.db_session.expire_all()
        if new_account:self.customer=base.models.Debtor.query.filter_by(reference_id=self.vehicle.client_id,shop_id=self.mechanic.id).one()
        self.wallet=base.models.AitTokenWallet(user_id=1,balance=50);self.db_session.add(self.wallet);self.db_session.commit()
        self.url=f'/mechanic/shop/{self.mechanic.id}/debtor/{self.customer.id}/mechanic-quotes/18/items'

    def edit_values(self,kind='part',identifier=None):
        identifier=identifier or (self.part.id if kind=='part' else self.labour.id)
        response=self.client.get(self.url,query_string=dict(kind=kind,item_id=identifier))
        self.assertEqual(response.status_code,200)
        return {node.name:node.value for node in html.fromstring(response.data).xpath('//form//input[@name]')}

    def test_legacy_18_register_saved_items_single_edit_vat_debit_tokens(self):
        self.fixtures()
        self.assertEqual(self.job.debtor_id,self.customer.id)
        register=self.client.get(self.quote_url(self.customer));self.assertEqual(register.status_code,200)
        tree=html.fromstring(register.data)
        self.assertEqual(tree.xpath('//table/thead/tr/th/text()'),['Quote No.','Date','Amount'])
        self.assertIn('989.37',tree.text_content());self.assertIn('Q-0001',tree.text_content())
        self.assertEqual(tree.xpath('//a[text()="Q-0001"]/@href'),[self.url])
        old=self.client.get('/mechanic/job_card/18');self.assertEqual(old.status_code,302);self.assertEqual(old.location,self.url)
        page=html.fromstring(self.client.get(self.url).data)
        self.assertEqual(len(page.xpath('//table//a[text()="Edit"]')),2)
        self.assertNotIn('Quote / Tax Invoice',page.text_content())
        self.assertIn('ABC123',page.text_content());self.assertIn('VIN-UNCHANGED',page.text_content())
        before=(self.labour.id,self.labour.description,self.labour.hours,self.labour.rate_per_hour,self.labour.time_in,self.labour.time_out)
        identity=(self.job.id,self.job.job_number,self.job.quotation_seq,self.job.vehicle_id)
        debit=base.DebtorLedger.query.filter_by(mech_job_card_id=18).one();debit_id=debit.id
        self.assertEqual(debit.amount,98937)
        values=self.edit_values();values['quantity']='2'
        self.assertEqual(self.client.post(self.url,data=values).location,self.url)
        self.db_session.expire_all()
        self.assertEqual((self.labour.id,self.labour.description,self.labour.hours,self.labour.rate_per_hour,self.labour.time_in,self.labour.time_out),before)
        self.assertEqual((self.job.id,self.job.job_number,self.job.quotation_seq,self.job.vehicle_id),identity)
        self.assertEqual(self.part.quantity,2);self.assertEqual(self.part.unit_cost,300)
        self.assertEqual(mechanic.totals(self.job)['total'],Decimal('1748.74'))
        debit=base.DebtorLedger.query.filter_by(mech_job_card_id=18).one()
        self.assertEqual((debit.id,debit.amount),(debit_id,174874))
        self.assertIn('1748.74',self.client.get(self.url).get_data(as_text=True))
        self.assertEqual(self.edit_values()['quantity'],'2')
        self.assertEqual(self.client.post(self.url,data=values).status_code,409)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id).count(),1)
        self.assertEqual(base.models.MechJobCard.query.count(),1);self.assertEqual(self.wallet.balance,50)

    def test_labour_edit_cancel_invalid_and_isolation(self):
        self.fixtures()
        before=self.conn.exec_driver_sql('SELECT * FROM mech_part_lines ORDER BY id').all()
        values=self.edit_values('labour');values.update(quantity='3.5',rate='125')
        self.assertEqual(self.client.post(self.url,data=values).status_code,302)
        self.assertEqual(self.labour.hours,3.5);self.assertEqual(self.labour.rate_per_hour,125)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM mech_part_lines ORDER BY id').all(),before)
        data=self.edit_values('labour');data['quantity']='NaN'
        self.assertEqual(self.client.post(self.url,data=data).status_code,400)
        self.assertEqual(self.labour.hours,3.5)
        response=self.client.get(self.url,query_string=dict(kind='labour',item_id=self.labour.id))
        cancel=html.fromstring(response.data).xpath('//a[text()="Cancel"]/@href')[0]
        self.assertEqual(cancel,self.url);self.assertEqual(self.client.get(cancel).status_code,200)
        self.assertEqual(self.client.get(self.url,query_string=dict(kind='part',item_id=99999)).status_code,404)
        self.assertEqual(self.client.get(self.url.replace(f'/debtor/{self.customer.id}/',f'/debtor/{self.tomas.id}/')).status_code,404)
        with self.client.session_transaction() as session:session['_user_id']='2'
        for url in (self.url,'/mechanic/job_card/18','/mechanic/download/18','/mechanic/email/18'):
            self.assertEqual(self.client.get(url).status_code,404)
        self.assertEqual(self.wallet.balance,50)

    def test_adopts_existing_debit_without_rewriting_history(self):
        self.fixtures(adopt=True)
        row=base.DebtorLedger.query.filter_by(mech_job_card_id=18).one()
        self.assertEqual((row.id,row.description,row.amount,row.ref),(self.old_charge.id,'Original legacy charge',98937,'JOB-'+self.job.job_number))
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id).count(),1)
        with self.conn.begin_nested() as transaction:
            with self.assertRaises(sa.exc.IntegrityError):
                self.conn.exec_driver_sql("INSERT INTO debtor_ledger(debtor_id,txn_date,description,kind,amount,mech_job_card_id,ref) VALUES("+str(self.customer.id)+",current_date,'duplicate','debit',98937,18,'JOB-JOB-LEGACY-18')")
            transaction.rollback()

    def test_exact_legacy_charge_links_original_unassigned_debtor(self):
        self.fixtures(adopt=True,legacy_identity=True)
        self.assertEqual(self.job.debtor_id,self.customer.id)
        self.assertEqual(self.customer.reference_id,self.vehicle.client_id)
        self.assertEqual(self.customer.shop_id,self.mechanic.id)
        self.assertEqual(base.DebtorLedger.query.filter_by(mech_job_card_id=18).one().id,self.old_charge.id)
        self.assertEqual(base.models.Debtor.query.filter_by(shop_id=self.mechanic.id).count(),1)

    def test_legacy_client_without_account_gets_one_permanent_shop_account(self):
        self.fixtures(new_account=True)
        self.assertEqual(self.job.debtor_id,self.customer.id)
        self.assertEqual((self.customer.user_id,self.customer.shop_id,self.customer.reference_id),(1,self.mechanic.id,self.vehicle.client_id))
        self.assertEqual(base.models.Debtor.query.filter_by(shop_id=self.mechanic.id,reference_id=self.vehicle.client_id).count(),1)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id).one().amount,98937)
        self.assertEqual(self.client.get(self.quote_url(self.customer)).status_code,200)

    def test_pdf_and_email_keep_existing_documents_and_job_shop(self):
        self.fixtures()
        self.mechanic.bank_details='Existing mechanic bank details';self.db_session.commit()
        from test_protrade_trade_quotes import InviteLog
        self.ns.update(BusinessBankAccount=ledger.Bank,MechCommunication=Communication,InviteLog=InviteLog)
        with self.client.session_transaction() as session:session['active_shop_id']=self.carpentry.id
        response=self.client.get('/mechanic/download/18');self.assertEqual(response.status_code,200)
        self.assertTrue(response.data.startswith(b'%PDF-'))
        import fitz
        pdf=fitz.open(stream=response.data,filetype='pdf');text=''.join(page.get_text() for page in pdf)
        (base.ROOT/'tmp/mechanic-quote.pdf').write_bytes(response.data)
        pdf[0].get_pixmap().save(str(base.ROOT/'tmp/mechanic-quote.png'));pdf.close()
        for value in ('Q-0001','989.37','ABC123','Toyota'):self.assertIn(value,text)
        self.assertIn(self.mechanic.business_name,text)
        public=self.client.get('/mechanic/public/job_card/'+self.job.job_number)
        self.assertEqual(public.status_code,200);self.assertIn(self.mechanic.business_name,public.get_data(as_text=True))
        self.assertEqual(self.client.get('/mechanic/email/18').status_code,200)
        with patch('app.extensions.mail.send') as sender,patch('app.utils.mailer.send_email',side_effect=AssertionError('No email retry expected')):
            result=self.client.post('/mechanic/email/18',data={'email':self.customer.email})
            self.assertEqual(result.status_code,302);self.assertEqual(sender.call_count,1)
            message=sender.call_args.args[0]
            self.assertIn('Q-0001',message.subject);self.assertIn(self.mechanic.business_name,message.subject)
            self.assertEqual(len(message.attachments),1)
        self.assertEqual(self.wallet.balance,50)
        self.assertEqual(base.DebtorLedger.query.filter_by(mech_job_card_id=18).count(),1)

    def test_invoice_approval_and_deposit_reuse_existing_quote_charge(self):
        self.fixtures()
        self.ns.update(SenderProfile=base.models.SenderProfile)
        self.ns['db'].func=sa.func
        # Existing approval and deposit actions are retained, with the exact
        # permanently linked debtor account instead of a name-based duplicate.
        self.assertEqual(self.client.post('/mechanic/job/18/approve',data={'pop_amount':'0'}).status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id,kind='debit').count(),1)
        self.assertEqual(self.client.post('/mechanic/job_card/18/record_deposit',data={'deposit_amount':'100'}).status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id,kind='credit').one().amount,10000)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id,kind='debit').count(),1)
        self.assertEqual(base.models.Debtor.query.filter_by(shop_id=self.mechanic.id).count(),1)
        self.assertEqual(self.wallet.balance,50)
        with self.conn.begin_nested() as transaction:
            with self.assertRaises(sa.exc.DBAPIError):
                self.conn.execute(sa.text("INSERT INTO debtor_ledger(debtor_id,txn_date,description,kind,amount,ref) VALUES(:debtor,current_date,'old duplicate','debit',98937,:ref)"),dict(debtor=self.customer.id,ref='JOB-'+self.job.job_number))
            transaction.rollback()

    def test_new_quote_posts_one_debit_and_existing_token_cost_once(self):
        self.fixtures()
        import test_protrade_quote_numbering as numbering
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes DROP COLUMN quotation_seq CASCADE')
        self.conn.exec_driver_sql('ALTER TABLE mech_job_cards DROP COLUMN quotation_seq CASCADE')
        numbering.migrate(self.conn);self.db_session.expire_all()
        self.assertEqual(self.job.display_quote_number,'Q-0001')
        from test_protrade_trade_quotes import TokenTransaction,InviteLog
        self.ns.update(text=sa.text,AitTokenTransaction=TokenTransaction,InviteLog=InviteLog)
        self.conn.exec_driver_sql('CREATE TEMP TABLE system_settings(key text PRIMARY KEY,value text)')
        self.mechanic.vat_rate=15;self.db_session.commit()
        data=dict(shop_id=self.mechanic.id,debtor_id=self.customer.id,client_id=self.customer.reference_id,customer_name=self.customer.name,vehicle_reg='NEW123')
        data.update({'part_qty[]':['2'],'part_desc[]':['New part'],'part_rate[]':['100'],'labor_desc[]':['New labour'],'labor_in[]':['08:00'],'labor_out[]':['09:00'],'labor_rate[]':['50']})
        for _ in range(2):self.assertEqual(self.client.post('/mechanic/quote/new',data=data).status_code,302)
        self.db_session.expire_all()
        created=base.models.MechJobCard.query.filter(base.models.MechJobCard.id!=18).one()
        self.assertEqual(created.display_quote_number,'Q-0002')
        self.assertEqual(base.DebtorLedger.query.filter_by(mech_job_card_id=created.id).one().amount,28750)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id,kind='debit').count(),2)
        self.assertEqual(self.wallet.balance,45)
        self.assertEqual(self.job.display_quote_number,'Q-0001')

    def test_existing_tax_invoice_action_keeps_one_debit_and_correct_shop(self):
        self.fixtures()
        from test_protrade_trade_quotes import TokenTransaction
        self.ns.update(SenderProfile=base.models.SenderProfile,BusinessBankAccount=ledger.Bank,AitTokenTransaction=TokenTransaction)
        self.conn.exec_driver_sql('CREATE TEMP TABLE system_settings(key text PRIMARY KEY,value text)')
        with self.client.session_transaction() as session:session['active_shop_id']=self.carpentry.id
        with patch('app.utils.mailer.send_pdf_email',return_value=True) as sender:
            result=self.client.post('/mechanic/job_card/18/accept')
            self.assertEqual(result.status_code,302)
            self.assertEqual(sender.call_count,1)
            self.assertIn(self.mechanic.business_name,sender.call_args.kwargs['subject'])
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.customer.id,kind='debit').count(),1)
        self.assertEqual(base.models.Debtor.query.filter_by(shop_id=self.mechanic.id).count(),1)
        self.assertEqual(self.wallet.balance,45)  # Existing invoice action fee, never an editing fee.

    def browser_route(self,route):
        req=route.request;url=urlsplit(req.url)
        if url.path.startswith('/static/'):
            return route.fulfill(path=str(base.ROOT/'app'/url.path.lstrip('/')),content_type='text/javascript')
        response=self.client.open(url.path+('?' +url.query if url.query else ''),method=req.method,data=req.post_data,content_type=req.headers.get('content-type'),follow_redirects=True)
        route.fulfill(status=response.status_code,headers={k:v for k,v in response.headers if k.lower() not in ('content-length','set-cookie')},body=response.data)

    def test_browser_register_item_save_return_reopen_cancel(self):
        self.fixtures()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.goto(f'http://localhost/mechanic/shop/{self.mechanic.id}/debtors')
                page.get_by_role('link',name='Quote',exact=True).click()
                self.assertTrue(page.locator('#mechanic-quote-register').is_visible())
                page.get_by_role('link',name='Q-0001',exact=True).click()
                page.locator('#mechanic-quote-items tr').filter(has_text='Existing part').get_by_role('link',name='Edit',exact=True).click()
                self.assertEqual(page.locator('input[name=quantity]').input_value(),'1')
                page.locator('input[name=quantity]').fill('2')
                self.assertEqual(page.locator('#mechanic-line-total').inner_text(),'1320.64')
                page.get_by_role('button',name='Save Changes',exact=True).click()
                self.assertEqual(urlsplit(page.url).path,self.url)
                self.assertEqual(page.locator('#mechanic-grand-total').inner_text(),'1748.74')
                page.locator('#mechanic-quote-items tr').filter(has_text='Existing part').get_by_role('link',name='Edit',exact=True).click()
                self.assertEqual(page.locator('input[name=quantity]').input_value(),'2')
                page.locator('input[name=quantity]').fill('3');page.get_by_role('link',name='Cancel',exact=True).click()
                self.assertEqual(page.locator('#mechanic-grand-total').inner_text(),'1748.74')
                self.assertEqual(self.wallet.balance,50)
            finally:browser.close()

if __name__=='__main__':unittest.main()
