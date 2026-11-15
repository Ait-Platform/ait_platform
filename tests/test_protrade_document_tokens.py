"""Atomic document pricing, immutable SOAs and free exports on PostgreSQL."""
import importlib.util
import json
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4
from lxml import html
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_shop_register as base
import test_protrade_payment_methods as payments
import test_protrade_quote_ledger as ledger
import app.protrade_output as output
import app.protrade_statements as statements


class DocumentTokenTests(unittest.TestCase):
    setUp=payments.PaymentMethodTests.setUp
    shop=payments.PaymentMethodTests.shop
    save_debtor=payments.PaymentMethodTests.save_debtor
    debtor=payments.PaymentMethodTests.debtor
    save_quote=payments.PaymentMethodTests.save_quote
    quote_url=payments.PaymentMethodTests.quote_url
    ledger_url=payments.PaymentMethodTests.ledger_url

    def fixtures(self,balance=50):
        payments.PaymentMethodTests.fixtures(self)
        self.tomas.email='jane@example.com'
        self.wallet=base.models.test_wallet(self.db_session,balance=balance)
        self.db_session.add(self.wallet);self.db_session.commit()
        statements.BusinessBankAccount=ledger.Bank
        from importlib import import_module
        global output
        output=import_module('app.protrade_output')
        for name,value in dict(self.ns,ProTradeDocument=base.ProTradeDocument,ProTradeOutputEmail=base.ProTradeOutputEmail,MechInvoice=base.catalog_globals['MechInvoice']).items():
            if name=='db' or name.startswith(('Mech','ProTrade','Debtor')):setattr(output,name,value)
        from datetime import datetime
        output.datetime=datetime
        output.price_items=self.ns['selected_price_list']
        self.soa='/debtor/'+str(self.tomas.id)+'/soa'

    def new_quote(self,key=None,**changes):
        data={'area':'new','quote_key':key or uuid4().hex,'item_id[]':[str(self.item.id)],f'quantity_{self.item.id}':'2'}
        data.update(changes);return self.client.post(self.quote_url(self.tomas),data=data)

    def counts(self):
        return (base.ProTradeBoqQuote.query.count(),base.DebtorLedger.query.count(),base.TokenTransaction.query.count(),base.ProTradeDocument.query.count())

    def test_quote_ten_tokens_once_edit_and_read_free(self):
        self.fixtures();before=self.counts();key=uuid4().hex
        for _ in range(3):self.assertEqual(self.new_quote(key).status_code,302)
        self.db_session.expire_all()
        self.assertEqual(self.wallet.balance,40)
        self.assertEqual(self.counts(),(before[0]+1,before[1]+1,1,1))
        quote=base.ProTradeBoqQuote.query.filter_by(quote_number='CAR-Q-'+key.upper()).one()
        self.assertEqual(base.TokenTransaction.query.one().amount,-10)
        item_id=self.ns['carpentry_quote_item_id'](quote,quote.items[0])
        url=f'/mechanic/shop/{quote.shop_id}/debtor/{quote.debtor_id}/quotes/{quote.id}/items'
        tree=html.fromstring(self.client.get(url,query_string=dict(item_id=item_id)).data)
        data={node.name:node.value for node in tree.xpath('//form//input[@name]')};data.update(unit='m',quantity='3')
        self.assertEqual(self.client.post(url,data=data).status_code,302)
        self.assertEqual(self.wallet.balance,40);self.assertEqual(base.ProTradeDocument.query.count(),1)
        self.assertEqual(self.item.material_rate,Decimal('85.00'))
        self.assertEqual(self.client.get(url).status_code,200)

    def test_insufficient_and_failed_save_leave_no_partial_records_or_charge(self):
        self.fixtures(balance=9);before=self.counts()
        self.assertEqual(self.new_quote().status_code,402)
        self.assertEqual(self.counts(),before);self.assertEqual(self.wallet.balance,9)
        self.wallet.balance=50;self.db_session.commit()
        self.assertEqual(self.new_quote(**{f'quantity_{self.item.id}':'0'}).status_code,400)
        self.assertEqual(self.counts(),before);self.assertEqual(self.wallet.balance,50)
        self.conn.exec_driver_sql('ALTER TABLE protrade_document ADD CONSTRAINT fail_save CHECK(false)')
        self.assertEqual(self.new_quote().status_code,409)
        self.assertEqual(self.counts(),before);self.assertEqual(self.wallet.balance,50)

    def generate(self,**changes):
        data=dict(action='generate');data.update(changes)
        return self.client.post(self.soa,data=data)

    def test_soa_2148_32_immutable_reuse_period_and_free_exports(self):
        self.fixtures()
        self.assertEqual(payments.PaymentMethodTests.payment(self,method='eWallet',amount='1125',ref='EW1125').status_code,302)
        ledger_before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        self.assertEqual(self.client.get(self.soa).status_code,200);self.assertEqual(self.wallet.balance,50)
        result=self.generate();self.assertEqual(result.status_code,302)
        document=base.ProTradeDocument.query.filter_by(kind='soa').one();identity=document.id;pdf=bytes(document.pdf)
        self.assertEqual(document.snapshot['closing'],214832);self.assertEqual(self.wallet.balance,40)
        import fitz
        rendered=fitz.open(stream=pdf,filetype='pdf');text=''.join(page.get_text() for page in rendered);rendered.close()
        for value in ('Tomas','HAPPY SHOP','2,148.32','eWallet'):self.assertIn(value,text)
        self.assertEqual(self.generate().status_code,302);self.assertEqual(base.ProTradeDocument.query.count(),1)
        self.wallet.balance=0;self.db_session.commit()
        self.assertEqual(self.generate().status_code,302)
        for _ in range(2):
            response=self.client.get(self.soa,query_string=dict(document_id=identity,pdf=1))
            self.assertEqual(response.status_code,200);self.assertEqual(response.data,pdf)
        preview=self.client.get('/mechanic/output',query_string=dict(shop_id=self.carpentry.id,selection='statement:generated-'+str(identity),preview=1))
        self.assertEqual(preview.status_code,200)
        fields={node.get('name'):node.get('value','') for node in html.fromstring(preview.data).xpath('//form[@id="output-email-form"]/input')}
        self.assertEqual(self.client.get('/mechanic/output',query_string=dict(fields,format='pdf')).data,pdf)
        self.app.config['MAIL_SUPPRESS_SEND']=False;self.app.extensions['mail']=SimpleNamespace(suppress=False)
        with patch('app.utils.mailer.send_pdf_email',return_value=True) as sender:
            self.assertEqual(self.client.post('/mechanic/output',data=dict(fields,action='email',confirm_email='yes')).status_code,302)
            self.assertEqual(sender.call_count,1);self.assertEqual(sender.call_args.args[3],pdf)
        self.assertEqual(self.wallet.balance,0);self.assertEqual(base.TokenTransaction.query.count(),1)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),ledger_before)
        self.wallet.balance=30;self.db_session.commit()
        self.assertEqual(payments.PaymentMethodTests.payment(self,amount='100',ref='NEXT').status_code,302)
        self.assertEqual(self.generate().status_code,302)
        self.assertEqual(self.wallet.balance,20);self.assertEqual(base.ProTradeDocument.query.count(),2)
        self.assertEqual(self.client.get(self.soa,query_string=dict(document_id=identity,pdf=1)).data,pdf)
        self.assertEqual(self.generate(start_date='2026-11-13',end_date='2026-11-14').status_code,302)
        self.assertEqual(self.wallet.balance,10)
        period=base.ProTradeDocument.query.order_by(base.ProTradeDocument.id.desc()).first()
        self.assertEqual(period.snapshot['opening'],180000);self.assertEqual(period.snapshot['closing'],204832)

    def test_soa_renderer_failure_insufficient_and_isolation(self):
        self.fixtures(balance=9);before=self.counts()
        self.assertEqual(self.generate().status_code,402);self.assertEqual(self.counts(),before)
        self.wallet.balance=50;self.db_session.commit()
        with patch.object(statements,'snapshot_pdf',side_effect=RuntimeError('renderer failed')):
            self.assertEqual(self.generate().status_code,503)
        self.assertEqual(self.counts(),before);self.assertEqual(self.wallet.balance,50)
        self.assertEqual(self.generate().status_code,302)
        document=base.ProTradeDocument.query.one()
        self.assertEqual(self.client.get('/debtor/'+str(self.other.id)+'/soa',query_string={'document_id':document.id}).status_code,404)
        with self.client.session_transaction() as session:session['_user_id']='2'
        self.assertEqual(self.client.get(self.soa,query_string={'document_id':document.id,'pdf':1}).status_code,404)

    def test_migration_is_empty_preserves_wallets_and_immutable_receipts(self):
        self.fixtures()
        self.conn.exec_driver_sql('DROP TABLE protrade_document')
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes DROP COLUMN boq_sections')
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)');self.conn.exec_driver_sql('INSERT INTO "user" VALUES(1),(2)')
        before={table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in ('debtor_ledger','protrade_boq_quotes','ait_token_wallets','ait_token_transaction')}
        spec=importlib.util.spec_from_file_location('tokens_migration',base.ROOT/'migrations/versions/protrade_document_tokens_013.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration);migration.op=Operations(MigrationContext.configure(self.conn));migration.upgrade()
        for table,rows in before.items():
            selected = ','.join(column['name'] for column in sa.inspect(self.conn).get_columns(table) if table != 'protrade_boq_quotes' or column['name'] != 'boq_sections')
            self.assertEqual(self.conn.exec_driver_sql('SELECT '+selected+' FROM '+table+' ORDER BY id').all(),rows)
        self.assertEqual(base.ProTradeDocument.query.count(),0)
        self.assertEqual(self.generate().status_code,302)
        with self.conn.begin_nested() as transaction:
            with self.assertRaises(sa.exc.DBAPIError):self.conn.exec_driver_sql('DELETE FROM protrade_document')
            transaction.rollback()


    def test_concurrent_retries_and_distinct_charges_keep_wallet_consistent(self):
        from concurrent.futures import ThreadPoolExecutor
        from sqlalchemy.orm import scoped_session, sessionmaker
        import app.protrade_documents as documents
        import re
        prefix = 'pt_' + uuid4().hex[:10] + '_'
        isolated = sa.create_engine(self.engine.url)
        names = set(base.models.Base.metadata.tables)
        for table in base.models.Base.metadata.tables.values():
            names.update(item.name for item in list(table.constraints) + list(table.indexes) if item.name)

        @sa.event.listens_for(isolated, 'before_cursor_execute', retval=True)
        def isolate(connection, cursor, statement, parameters, context, executemany):
            for name in names:
                statement = re.sub(r'(?<![\w])' + re.escape(name) + r'(?![\w])', prefix + name, statement)
            return statement, parameters
        sessions = scoped_session(sessionmaker(bind=isolated))
        try:
            base.models.Base.metadata.create_all(isolated, checkfirst=False)
            session = sessions()
            session.add(base.models.User(id=1, name='Concurrent owner', email='concurrent@example.test'))
            session.flush()
            session.add(base.models.AitTokenWallet(user_id=1, balance=100))
            session.commit()
            sessions.remove()

            def charge(key):
                try:
                    documents.charge_document(1, None, None, key, 'quote')
                    sessions.commit()
                    return True
                except documents.InsufficientTokens:
                    sessions.rollback()
                    return False
                finally:
                    sessions.remove()

            with patch.object(documents, 'db', SimpleNamespace(session=sessions)), patch.object(base.models.Base, 'query', sessions.query_property()):
                with ThreadPoolExecutor(max_workers=8) as workers:
                    self.assertTrue(all(workers.map(charge, ['same-document'] * 8)))
                self.assertEqual(sessions.query(base.models.AitTokenWallet).one().balance, 90)
                self.assertEqual(sessions.query(base.ProTradeDocument).count(), 1)
                sessions.remove()
                with ThreadPoolExecutor(max_workers=8) as workers:
                    results = list(workers.map(charge, [f'new-document-{index}' for index in range(12)]))
                self.assertEqual(sum(results), 9)
                sessions.expire_all()
                self.assertEqual(sessions.query(base.models.AitTokenWallet).one().balance, 0)
                self.assertEqual(sessions.query(base.ProTradeDocument).count(), 10)
                self.assertEqual(sessions.query(base.TokenTransaction).count(), 10)
        finally:
            sessions.remove()
            with isolated.begin() as connection:
                for table in reversed(base.models.Base.metadata.sorted_tables):
                    connection.execute(sa.schema.DropTable(table, if_exists=True))
            isolated.dispose()

if __name__=='__main__':unittest.main()
