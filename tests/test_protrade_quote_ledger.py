"""Real PostgreSQL quotation posting/backfill, payments, ledger and existing SOA."""
import ast
from copy import deepcopy
from datetime import datetime,date
from decimal import Decimal
import importlib.util
import io
import re
import unittest
from unittest.mock import patch,Mock
from uuid import uuid4
import sqlalchemy as sa
from sqlalchemy.orm import Query
from alembic.migration import MigrationContext
from alembic.operations import Operations
from flask import Blueprint,abort,send_file
from lxml import html
import test_protrade_shop_register as base
import test_protrade_debtor_foundation as foundation

class Bank(base.models.Base):
    __tablename__='business_bank_account'
    id=sa.Column(sa.Integer,primary_key=True)
    user_id=sa.Column(sa.Integer,sa.ForeignKey('users.id'))
    is_default=sa.Column(sa.Boolean,default=False)


def old_ledger_schema(conn):
    conn.exec_driver_sql('ALTER TABLE debtor_ledger DROP COLUMN boq_quote_id CASCADE')
    conn.exec_driver_sql('ALTER TABLE debtor_ledger DROP COLUMN payment_key CASCADE')
    conn.exec_driver_sql('ALTER TABLE debtor_ledger ALTER COLUMN amount TYPE integer USING amount::integer')


def migrate(conn):
    spec=importlib.util.spec_from_file_location('ledger_migration',base.ROOT/'migrations/versions/protrade_quote_ledger_009.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    module.op=Operations(MigrationContext.configure(conn));module.upgrade();return module


class QuoteLedgerTests(unittest.TestCase):
    shop=foundation.DebtorFoundationTests.shop
    save_debtor=foundation.DebtorFoundationTests.save_debtor
    debtor=foundation.DebtorFoundationTests.debtor
    save_quote=foundation.DebtorFoundationTests.save_quote
    quote_url=foundation.DebtorFoundationTests.quote_url

    def setUp(self):
        foundation.DebtorFoundationTests.setUp(self)
        def first_or_404(query):
            row=query.first()
            if row is None:abort(404)
            return row
        patcher=patch.object(Query,'first_or_404',first_or_404,create=True);patcher.start();self.addCleanup(patcher.stop)
        bp=Blueprint('debtors_bp',__name__)
        from app.utils.pdf_render import html_to_pdf_bytes
        namespace=dict(self.ns,debtors_bp=bp,Debtor=base.models.Debtor,DebtorLedger=base.DebtorLedger,SenderProfile=base.models.SenderProfile,BusinessBankAccount=Bank,io=io,send_file=send_file,html_to_pdf_bytes=html_to_pdf_bytes,send_pdf_email=Mock(side_effect=AssertionError('No automatic SOA email')))
        tree=ast.parse((base.ROOT/'app/program_debtors/routes.py').read_text(encoding='utf-8'))
        nodes=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in ('generate_soa','email_soa')]
        exec(compile(ast.fix_missing_locations(base.ProductionImports().visit(ast.Module(body=nodes,type_ignores=[]))),'production_soa_routes','exec'),namespace)
        self.app.register_blueprint(bp)
        templates=['templates/program_mechanic/client_ledger.html','templates/program_debtors/soa_template.html']
        for endpoint in set(re.findall(r"url_for\(['\"]([^'\"]+)",''.join((base.ROOT/name).read_text(encoding='utf-8') for name in templates))):
            if endpoint not in self.app.view_functions:self.app.add_url_rule('/stub/'+endpoint,endpoint,lambda **kw:'stub')

    def fixtures(self,adopt=False,upgrade=True):
        self.save_debtor(name='Tomas');self.tomas=self.debtor()
        self.save_debtor(self.second,name='Other debtor');self.other=self.debtor(self.second)
        self.quotes=[]
        for sequence,amount,day in [(5,'1800.00',12),(6,'1473.32',13)]:
            line=dict(source_item_id='template:CAR-003',code='CAR-003',description='Install door lock',unit='Each',quantity='1',material_rate=amount,labour_rate='0.00',total=amount)
            quote=base.ProTradeBoqQuote(user_id=1,shop_id=self.carpentry.id,debtor_id=self.tomas.id,client_id=self.tomas.reference_id,trade='carpentry',quotation_seq=sequence,quote_number='CAR-Q-'+uuid4().hex.upper(),client_name='Tomas',items=[line],grand_total=Decimal(amount),created_at=datetime(2026,11,day,10),is_draft=sequence==6)
            self.db_session.add(quote);self.quotes.append(quote)
        self.db_session.flush()
        if adopt:self.db_session.add(base.DebtorLedger(debtor_id=self.tomas.id,txn_date=date(2026,11,12),ref='Q-0005',description='Existing quote charge',kind='debit',amount=180000))
        self.db_session.commit()
        before=[(q.id,q.quote_number,deepcopy(q.items),q.grand_total,q.quotation_seq) for q in self.quotes]
        old_ledger_schema(self.conn)
        if upgrade:migrate(self.conn)
        self.assertEqual([(q.id,q.quote_number,q.items,q.grand_total,q.quotation_seq) for q in self.quotes],before)

    def ledger_url(self,debtor=None):return f'/mechanic/client_ledger/{(debtor or self.tomas).id}'

    def payment(self,key=None,**changes):
        data=dict(amount='500.00',date='2026-11-14',ref='EFT-500',kind='credit',payment_key=key or uuid4().hex)
        data.update(changes);return self.client.post(self.ledger_url()+'/add_payment',data=data)

    def test_backfill_balances_payment_edit_and_replays(self):
        self.fixtures(adopt=True)
        debits=base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='debit').order_by(base.DebtorLedger.id).all()
        self.assertEqual(len(debits),2);self.assertEqual(sum(row.amount for row in debits),327332)
        self.assertTrue(all(isinstance(row.amount,int) for row in debits))
        self.assertIn('3273.32',self.client.get(self.ledger_url()).get_data(as_text=True))
        key=uuid4().hex
        for _ in range(3):self.assertEqual(self.payment(key).status_code,302)
        self.assertEqual(self.payment(ref='EFT-500').status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),1)
        self.assertIn('2773.32',self.client.get(self.ledger_url()).get_data(as_text=True))
        second=self.quotes[1];identity=(second.id,second.quote_number,second.quotation_seq)
        debit_id=base.DebtorLedger.query.filter_by(boq_quote_id=second.id).one().id
        url=f'/mechanic/shop/{self.carpentry.id}/debtor/{self.tomas.id}/quotes/{second.id}/items'
        item_id=self.ns['carpentry_quote_item_id'](second,second.items[0])
        tree=html.fromstring(self.client.get(url+'?item_id='+item_id).data)
        data={node.name:node.value for node in tree.xpath('//form//input[@name]')};data.update(unit='Each',quantity='2')
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.db_session.expire_all()
        self.assertEqual(base.DebtorLedger.query.filter_by(boq_quote_id=second.id).one().id,debit_id)
        self.assertEqual(base.DebtorLedger.query.filter_by(boq_quote_id=second.id).one().amount,294664)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='debit').count(),2)
        self.assertIn('4246.64',self.client.get(self.ledger_url()).get_data(as_text=True))
        self.assertEqual(self.client.post(url,data=data).status_code,409)
        self.assertEqual((second.id,second.quote_number,second.quotation_seq),identity)
        self.assertEqual(self.payment(key,amount='501').status_code,409)
        self.assertEqual(self.payment(kind='debit',ref='BAD').status_code,400)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.other.id).count(),0)
        # The same bank reference on a different date is a distinct legitimate payment.
        self.assertEqual(self.payment(date='2026-11-15').status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),2)

    def test_new_debtor_and_quote_saves_are_zero_start_atomic_and_single_charge(self):
        self.fixtures()
        self.save_debtor(self.carpentry,name='New debtor',phone='987',email='new@example.test')
        new=base.models.Debtor.query.filter_by(name='New debtor').one()
        self.assertEqual(new.opening_balance,0)
        self.assertIn('0.00',self.client.get(self.ledger_url(new)).get_data(as_text=True))
        key=uuid4().hex;url=self.quote_url(new)
        for _ in range(3):self.save_quote(url=url,key=key)
        saved=base.ProTradeBoqQuote.query.filter_by(debtor_id=new.id).one()
        self.assertEqual(base.DebtorLedger.query.filter_by(boq_quote_id=saved.id).count(),1)
        self.assertEqual(base.DebtorLedger.query.filter_by(boq_quote_id=saved.id).one().amount,32500)
        snapshot=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        for _ in range(3):self.client.get(self.ledger_url(new));self.client.get(url)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),snapshot)
        with self.client.session_transaction() as session:session['_user_id']='2'
        self.assertEqual(self.client.get(self.ledger_url()).status_code,404)
        self.assertEqual(self.payment().status_code,404)

    def test_soa_same_balances_shop_branding_and_period_carry_forward(self):
        self.fixtures();self.payment()
        self.tomas.slug_reference='mechanic'  # A legacy tag must not override the saved shop's trade.
        self.carpentry.business_name='Happy Carpentry';self.carpentry.bank_details='Happy account only';self.db_session.commit()
        url=f'/debtor/{self.tomas.id}/soa'
        response=self.client.get(url);self.assertEqual(response.status_code,200)
        text=html.fromstring(response.data).text_content()
        for value in ('Happy Carpentry','Tomas','Q-0005','Q-0006','EFT-500','2,773.32'):self.assertIn(value,text)
        response=self.client.get(url+'?start_date=2026-11-14&end_date=2026-11-14')
        text=html.fromstring(response.data).text_content()
        self.assertIn('3,273.32',text);self.assertIn('2,773.32',text)
        ledger=self.client.get(self.ledger_url()+'?start_date=2026-11-14&end_date=2026-11-14')
        self.assertIn('2773.32',ledger.get_data(as_text=True))
        prior=html.fromstring(self.client.get(url+'?end_date=2026-11-13').data).text_content()
        self.assertIn('3,273.32',prior);self.assertNotIn('EFT-500',prior)
        pdf=self.client.get(url+'?pdf=1')
        self.assertEqual(pdf.mimetype,'application/pdf');self.assertTrue(pdf.data.startswith(b'%PDF'))
        (base.ROOT/'tmp/protrade-quote-ledger-soa.pdf').write_bytes(pdf.data)

    def test_browser_existing_debtor_ledger_payment_soa_and_quote_correction(self):
        from threading import Thread
        from werkzeug.serving import make_server
        from playwright.sync_api import sync_playwright
        self.fixtures()
        self.db_session.add(base.models.AitTokenWallet(user_id=1,balance=50));self.db_session.commit()
        tables=('ait_token_wallets','quote_test_token_transactions','mech_invoices','protrade_output_email','protrade_boq_items')
        before={table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in tables}
        count=base.ProTradeBoqQuote.query.count();identity=(self.quotes[1].id,self.quotes[1].quote_number,self.quotes[1].quotation_seq)
        self.app.static_folder=str(base.ROOT/'app/static')
        self.app.jinja_loader.loaders[0].mapping['layout.html']='<style>.hidden{display:none!important}</style>{% block head %}{% endblock %}{% block content %}{% endblock %}'
        server=make_server('127.0.0.1',0,self.app)
        def serve():
            try:server.serve_forever(poll_interval=0.1)
            finally:self.db_session.remove()
        thread=Thread(target=serve,daemon=True);thread.start();origin=f'http://127.0.0.1:{server.server_port}'
        try:
            with patch('flask_mail.Mail.send',side_effect=AssertionError('No automatic email')),patch('smtplib.SMTP',side_effect=AssertionError('No SMTP')),sync_playwright() as playwright:
                browser=playwright.chromium.launch(headless=True)
                try:
                    context=browser.new_context(viewport={'width':1400,'height':1000});context.add_cookies([{'name':'session','value':self.client.get_cookie('session').value,'url':origin}])
                    page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                    page.goto(origin+f'/mechanic/shop/{self.carpentry.id}/debtors')
                    page.locator('#shop-debtors tr').filter(has_text='Tomas').get_by_role('link',name='Ledger',exact=True).click()
                    self.assertIn('3273.32',page.locator('tfoot').text_content())
                    self.assertNotIn('Vehicle',page.locator('body').text_content())
                    page.get_by_role('button',name='Record Payment',exact=True).click()
                    page.locator('input[name=amount]').fill('500.00');page.locator('input[name=date]').fill('2026-11-14');page.locator('input[name=ref]').fill('EFT-500')
                    key=page.locator('input[name=payment_key]').input_value()
                    page.get_by_role('button',name='Save Payment',exact=True).click()
                    self.assertIn('2773.32',page.locator('tfoot').text_content())
                    response=context.request.post(origin+self.ledger_url()+'/add_payment',form=dict(amount='500.00',date='2026-11-14',ref='EFT-500',payment_key=key))
                    self.assertEqual(response.status,200)
                    page.reload();self.assertEqual(page.locator('table tbody tr').count(),3)
                    page.screenshot(path=str(base.ROOT/'tmp/protrade-ledger-paid.png'),full_page=True)
                    page.locator('a[title="Print / Download SOA"]').click()
                    self.assertIn('2,773.32',page.locator('body').text_content())
                    self.assertIn('Happy Shop',page.locator('body').text_content())
                    page.screenshot(path=str(base.ROOT/'tmp/protrade-ledger-soa.png'),full_page=True)
                    page.goto(origin+self.quote_url()+'?area=register')
                    self.assertEqual(page.locator('#debtor-quote-register th').all_text_contents(),['Quote No.','Date','Amount'])
                    page.get_by_role('link',name='Items Q-0006',exact=True).click()
                    page.get_by_role('link',name='Edit',exact=True).click()
                    page.get_by_label('Quantity',exact=True).fill('2')
                    page.get_by_role('button',name='Save Changes',exact=True).click()
                    page.goto(origin+self.ledger_url())
                    self.assertIn('4246.64',page.locator('tfoot').text_content())
                    self.assertEqual(page.locator('table tbody tr').count(),3)
                    self.assertEqual(errors,[])
                finally:browser.close()
        finally:server.shutdown();thread.join(timeout=5);server.server_close()
        self.db_session.expire_all()
        self.assertEqual(base.ProTradeBoqQuote.query.count(),count)
        self.assertEqual((self.quotes[1].id,self.quotes[1].quote_number,self.quotes[1].quotation_seq),identity)
        self.assertEqual({table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in tables},before)

    def test_mechanic_statement_and_manual_payments_keep_existing_behavior(self):
        self.fixtures()
        self.save_debtor(self.mechanic,name='Mechanic debtor',phone='789')
        debtor=self.debtor(self.mechanic)
        profile=base.models.SenderProfile(user_id=1,business_name='Existing Workshop',is_default=True)
        self.db_session.add_all([profile,base.DebtorLedger(debtor_id=debtor.id,txn_date=date(2026,11,12),kind='debit',amount=10000,description='Existing invoice',ref='INV-OLD')]);self.db_session.commit()
        response=self.client.post(self.ledger_url(debtor)+'/add_payment',data=dict(amount='30',date='2026-11-14',ref='MECHANIC-PAYMENT'))
        self.assertEqual(response.status_code,302)
        text=html.fromstring(self.client.get(f'/debtor/{debtor.id}/soa').data).text_content()
        self.assertIn('Existing Workshop',text);self.assertIn('70.00',text)
        self.assertIn('Only genuine parts used',text)
        self.assertNotIn('Carpentry quotation',text)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=debtor.id,kind='credit').one().payment_key,None)

    def test_large_existing_quote_values_retain_exact_cent_precision(self):
        self.fixtures()
        value=Decimal('12345678901234567890123456.78')
        self.quotes[1].grand_total=value;self.db_session.commit()
        charge=base.DebtorLedger.query.filter_by(boq_quote_id=self.quotes[1].id).one()
        self.assertEqual(charge.amount,1234567890123456789012345678)
        ledger=self.client.get(self.ledger_url()).get_data(as_text=True)
        self.assertIn('12345678901234567890123456.78',ledger)
        statement=self.client.get(f'/debtor/{self.tomas.id}/soa').get_data(as_text=True)
        self.assertIn('12,345,678,901,234,567,890,123,456.78',statement)

    def test_database_guards_and_failed_quote_save_are_atomic(self):
        self.fixtures()
        before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        with self.assertRaises(sa.exc.DBAPIError),self.conn.begin_nested():
            self.conn.exec_driver_sql('DELETE FROM debtor_ledger WHERE boq_quote_id IS NOT NULL')
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),before)
        with self.assertRaises(sa.exc.DBAPIError),self.conn.begin_nested():
            self.conn.execute(sa.text('UPDATE debtor_ledger SET amount=1 WHERE boq_quote_id=:id'),{'id':self.quotes[1].id})
        with self.assertRaises(sa.exc.DBAPIError),self.conn.begin_nested():
            self.conn.execute(sa.text('UPDATE protrade_boq_quotes SET debtor_id=:debtor WHERE id=:id'),{'debtor':self.other.id,'id':self.quotes[1].id})
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),before)
        with self.conn.begin_nested() as savepoint:
            self.conn.execute(sa.text('UPDATE protrade_boq_quotes SET grand_total=99 WHERE id=:id'),{'id':self.quotes[1].id})
            savepoint.rollback()
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),before)

    def test_ambiguous_backfill_rolls_back_without_deleting_history(self):
        self.fixtures(adopt=True,upgrade=False)
        self.conn.exec_driver_sql("INSERT INTO debtor_ledger(debtor_id,txn_date,description,kind,amount,ref) VALUES(1,'2026-11-12','Ambiguous old charge','debit',180000,'Q-0005')")
        before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        with self.assertRaises(sa.exc.DBAPIError),self.conn.begin_nested():migrate(self.conn)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),before)
        self.assertNotIn('boq_quote_id',{column['name'] for column in sa.inspect(self.conn).get_columns('debtor_ledger')})


class LedgerConcurrencyTests(unittest.TestCase):
    def test_parallel_quote_updates_and_payment_inserts(self):
        from concurrent.futures import ThreadPoolExecutor
        from dotenv import dotenv_values
        url=dotenv_values(base.ROOT/'.env')['DATABASE_URL'].replace('postgres://','postgresql://',1)
        parsed=sa.engine.make_url(url);self.assertEqual(parsed.database,'ait_local_db');self.assertIn(parsed.host,('localhost','127.0.0.1','::1'))
        engine=sa.create_engine(url);prefix='ql_'+uuid4().hex[:16]+'_'
        names=['mech_shops','debtor','protrade_boq_quotes','debtor_ledger','protrade_quote_backfill','protrade_sync_quote_debit','protrade_guard_quote_debit','protrade_quote_debit','protrade_quote_debit_guard','fk_debtor_ledger_boq_quote','uq_debtor_ledger_boq_quote','uq_debtor_payment_key','uq_debtor_payment_fingerprint','ck_debtor_quote_debit','ck_debtor_payment_credit']
        @sa.event.listens_for(engine,'before_cursor_execute',retval=True)
        def isolate(conn,cursor,statement,parameters,context,executemany):
            for name in names:statement=re.sub(r'(?<![\w])'+name+r'(?![\w])',prefix+name,statement)
            return statement,parameters
        try:
            with engine.begin() as conn:
                identity=conn.exec_driver_sql('SELECT current_database(),current_user').one();self.assertEqual(identity[0],'ait_local_db');self.assertTrue(identity[1])
                conn.exec_driver_sql('CREATE TABLE mech_shops(id integer PRIMARY KEY,user_id integer)')
                conn.exec_driver_sql('CREATE TABLE debtor(id integer PRIMARY KEY,user_id integer,shop_id integer,opening_balance integer DEFAULT 0)')
                conn.exec_driver_sql('CREATE TABLE protrade_boq_quotes(id serial PRIMARY KEY,user_id integer,shop_id integer,debtor_id integer,trade varchar(50),quote_number varchar(50),quotation_seq integer,grand_total numeric(28,2),created_at timestamp DEFAULT now())')
                conn.exec_driver_sql('CREATE TABLE debtor_ledger(id serial PRIMARY KEY,debtor_id integer REFERENCES debtor(id),txn_date date,description varchar(255),kind varchar(20),amount integer,ref varchar(100),created_at timestamp DEFAULT now())')
                conn.exec_driver_sql('INSERT INTO mech_shops VALUES(1,1)');conn.exec_driver_sql('INSERT INTO debtor VALUES(1,1,1,0)')
                migrate(conn)
                conn.execute(sa.text("INSERT INTO protrade_boq_quotes(id,user_id,shop_id,debtor_id,trade,quote_number,grand_total) VALUES(1,1,1,1,'carpentry',:number,1473.32)"),{'number':'CAR-Q-'+uuid4().hex.upper()})
            def update(_):
                with engine.begin() as conn:conn.exec_driver_sql('UPDATE protrade_boq_quotes SET grand_total=1473.32 WHERE id=1')
            with ThreadPoolExecutor(max_workers=6) as pool:list(pool.map(update,range(20)))
            key=uuid4().hex
            def payment(_):
                try:
                    with engine.begin() as conn:conn.execute(base.DebtorLedger.__table__.insert().values(debtor_id=1,txn_date=date(2026,11,14),description='Payment',kind='credit',amount=50000,ref='EFT-SAME',payment_key=key))
                    return True
                except sa.exc.IntegrityError:return False
            with ThreadPoolExecutor(max_workers=6) as pool:results=list(pool.map(payment,range(12)))
            self.assertEqual(sum(results),1)
            with engine.begin() as conn:
                rows=conn.exec_driver_sql('SELECT kind,amount FROM debtor_ledger ORDER BY id').all()
                self.assertEqual(rows,[('debit',Decimal('147332')),('credit',Decimal('50000'))])
        finally:
            self.assertRegex(prefix,r'^ql_[0-9a-f]{16}_$')
            with engine.begin() as conn:
                self.assertEqual(conn.exec_driver_sql('SELECT current_database()').scalar_one(),'ait_local_db')
                for table in ('debtor_ledger','protrade_boq_quotes','debtor','mech_shops'):conn.exec_driver_sql('DROP TABLE IF EXISTS '+table+' CASCADE')
                conn.exec_driver_sql('DROP FUNCTION IF EXISTS protrade_sync_quote_debit()')
                conn.exec_driver_sql('DROP FUNCTION IF EXISTS protrade_guard_quote_debit()')
            engine.dispose()

if __name__=='__main__':unittest.main()
