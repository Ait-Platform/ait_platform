"""Payment metadata preservation, exact SOA PDF layout, menu-only cleanup."""
from copy import deepcopy
from datetime import date
from decimal import Decimal
import importlib.util
import re
from types import SimpleNamespace
import unittest
from uuid import uuid4
from flask import render_template
from lxml import html
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
import test_protrade_shop_register as base
import test_protrade_quote_ledger as ledger


def migrate(conn):
    spec=importlib.util.spec_from_file_location('payment_method_migration',base.ROOT/'migrations/versions/protrade_payment_method_010.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    module.op=Operations(MigrationContext.configure(conn));module.upgrade();return module


class PaymentMethodTests(unittest.TestCase):
    setUp=ledger.QuoteLedgerTests.setUp
    shop=ledger.QuoteLedgerTests.shop
    save_debtor=ledger.QuoteLedgerTests.save_debtor
    debtor=ledger.QuoteLedgerTests.debtor
    save_quote=ledger.QuoteLedgerTests.save_quote
    quote_url=ledger.QuoteLedgerTests.quote_url
    ledger_url=ledger.QuoteLedgerTests.ledger_url

    def fixtures(self,legacy=False):
        ledger.QuoteLedgerTests.fixtures(self)
        if legacy:
            self.old=base.DebtorLedger(debtor_id=self.tomas.id,txn_date=date(2026,11,13),kind='credit',amount=12345,description='Original receipt description',ref='Original eWallet / receipt 007',payment_key=uuid4().hex)
            self.db_session.add(self.old);self.db_session.commit()
        self.conn.exec_driver_sql('ALTER TABLE debtor_ledger DROP COLUMN payment_method CASCADE')
        self.original_columns=[column['name'] for column in sa.inspect(self.conn).get_columns('debtor_ledger')]
        self.before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        migrate(self.conn)
        selected=','.join(self.conn.dialect.identifier_preparer.quote(name) for name in self.original_columns)
        self.assertEqual(self.conn.exec_driver_sql('SELECT '+selected+' FROM debtor_ledger ORDER BY id').all(),self.before)

    def payment(self,method='EFT',key=None,ref='',**changes):
        data=dict(amount='10.00',date='2026-11-14',payment_method=method,payment_key=key or uuid4().hex,ref=ref)
        data.update(changes);return self.client.post(self.ledger_url()+'/add_payment',data=data)

    def test_all_methods_optional_reference_and_duplicate_protection(self):
        self.fixtures()
        tree=html.fromstring(self.client.get(self.ledger_url()).data)
        self.assertEqual(tree.xpath('//select[@name="payment_method"]/option/@value'),['Cash','EFT','eWallet','Card','Other'])
        self.assertNotIn('required',tree.xpath('//input[@name="ref"]')[0].attrib)
        for method in ('Cash','EFT','eWallet','Card','Other'):
            key=uuid4().hex
            self.assertEqual(self.payment(method,key).status_code,302)
            self.assertEqual(self.payment(method,key).status_code,302)
            self.assertEqual(self.payment(method).status_code,302)
            row=base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,payment_method=method).one()
            self.assertIsNone(row.ref);self.assertEqual(row.amount,1000)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),5)
        text=html.fromstring(self.client.get(f'/debtor/{self.tomas.id}/soa').data).text_content()
        for method in ('Cash','EFT','eWallet','Card','Other'):self.assertIn('Payment received',text);self.assertIn(method,text)
        self.assertEqual(self.payment('Cheque').status_code,400)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),5)

    def test_legacy_records_and_provided_references_are_preserved(self):
        self.fixtures(legacy=True)
        self.db_session.expire_all();self.assertIsNone(self.old.payment_method)
        key=uuid4().hex
        self.assertEqual(self.payment('EFT',key,ref='BANK-42').status_code,302)
        self.assertEqual(self.payment('Cash',ref='BANK-42').status_code,302)
        self.assertEqual(self.payment('Cash',key,ref='BANK-42').status_code,409)
        row=base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,ref='BANK-42').one()
        self.assertEqual(row.payment_method,'EFT')
        self.assertEqual(self.old.ref,'Original eWallet / receipt 007');self.assertEqual(self.old.description,'Original receipt description')
        for url in (self.ledger_url(),f'/debtor/{self.tomas.id}/soa'):
            text=html.fromstring(self.client.get(url).data).text_content()
            for value in ('Original eWallet / receipt 007','Original receipt description','BANK-42','EFT'):self.assertIn(value,text)
        before=self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all()
        with self.client.session_transaction() as session:session['_user_id']='2'
        self.assertEqual(self.payment('eWallet').status_code,404)
        self.assertEqual(self.client.get(self.ledger_url()).status_code,404)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM debtor_ledger ORDER BY id').all(),before)

    def test_old_reference_methods_and_new_dropdown_do_not_double_credit(self):
        self.fixtures()
        old=base.DebtorLedger(debtor_id=self.tomas.id,txn_date=date(2026,11,14),kind='credit',amount=112500,ref='eWallet',description='Original receipt',payment_key=uuid4().hex)
        self.db_session.add(old);self.db_session.commit()
        self.assertEqual(self.payment('eWallet',amount='1125.00').status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),1)
        self.assertEqual(old.ref,'eWallet');self.assertIsNone(old.payment_method)
        self.assertEqual(self.payment('Cash',amount='25.00',description='Deposit received').status_code,302)
        response=self.client.post(self.ledger_url()+'/add_payment',data=dict(amount='25.00',date='2026-11-14',ref='Cash',payment_key=uuid4().hex))
        self.assertEqual(response.status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),2)
        row=base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,payment_method='Cash').one()
        self.assertEqual(row.description,'Deposit received')

    def test_ewallet_2148_32_pdf_is_readable_and_a4_aligned(self):
        import fitz
        self.fixtures()
        key=uuid4().hex
        self.assertEqual(self.payment('eWallet',key,amount='1125.00',ref='EW-1125').status_code,302)
        self.assertEqual(self.payment('eWallet',key,amount='1125.00',ref='EW-1125').status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),1)
        self.assertIn('2148.32',self.client.get(self.ledger_url()).get_data(as_text=True))
        response=self.client.get(f'/debtor/{self.tomas.id}/soa?pdf=1')
        self.assertEqual(response.mimetype,'application/pdf')
        (base.ROOT/'tmp/protrade-method-soa.pdf').write_bytes(response.data)
        with fitz.open(stream=response.data,filetype='pdf') as pdf:
            self.assertEqual(len(pdf),1);page=pdf[0];text=page.get_text()
            for value in ('Carpentry quotation Q-0005','Carpentry quotation Q-0006','Payment received \u2014 eWallet','2,148.32','EW-1125','12/11/2026','14/11/2026'):self.assertIn(value,text)
            words=page.get_text('words')
            right=lambda value:max(word[2] for word in words if word[4]==value)
            self.assertAlmostEqual(right('1,800.00'),right('3,273.32'),delta=1)
            self.assertAlmostEqual(right('3,273.32'),right('2,148.32'),delta=1)
            debit=next(word for word in words if word[4]=='1,473.32')
            self.assertTrue(any(word[4]=='1,800.00' and abs(word[2]-debit[2])<1 for word in words))
            self.assertLess(right('2,148.32'),page.rect.width-28)
            page.get_pixmap(matrix=fitz.Matrix(1.5,1.5)).save(str(base.ROOT/'tmp/protrade-method-soa.png'))

    def test_browser_method_selection_and_reference_reach_ledger_and_soa(self):
        from threading import Thread
        from unittest.mock import patch
        from werkzeug.serving import make_server
        from playwright.sync_api import sync_playwright
        self.fixtures()
        self.db_session.add(base.models.AitTokenWallet(user_id=1,balance=50));self.db_session.commit()
        self.app.static_folder=str(base.ROOT/'app/static')
        self.app.jinja_loader.loaders[0].mapping['layout.html']='<style>.hidden{display:none!important}</style>{% block head %}{% endblock %}{% block content %}{% endblock %}'
        server=make_server('127.0.0.1',0,self.app)
        def serve():
            try:server.serve_forever(poll_interval=0.1)
            finally:self.db_session.remove()
        thread=Thread(target=serve,daemon=True);thread.start();origin=f'http://127.0.0.1:{server.server_port}'
        count=base.ProTradeBoqQuote.query.count()
        try:
            with patch('flask_mail.Mail.send',side_effect=AssertionError('No automatic mail')),sync_playwright() as playwright:
                browser=playwright.chromium.launch(headless=True)
                try:
                    context=browser.new_context();context.add_cookies([{'name':'session','value':self.client.get_cookie('session').value,'url':origin}])
                    page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                    page.goto(origin+self.ledger_url())
                    page.get_by_role('button',name='Record Payment',exact=True).click()
                    page.get_by_label('Payment Method',exact=True).select_option('eWallet')
                    page.locator('input[name=amount]').fill('1125.00');page.locator('input[name=date]').fill('2026-11-14');page.locator('input[name=ref]').fill('EW-BROWSER')
                    key=page.locator('input[name=payment_key]').input_value()
                    page.get_by_role('button',name='Save Payment',exact=True).click()
                    self.assertIn('2148.32',page.locator('tfoot').text_content())
                    self.assertIn('eWallet',page.locator('table').text_content());self.assertIn('EW-BROWSER',page.locator('table').text_content())
                    response=context.request.post(origin+self.ledger_url()+'/add_payment',form=dict(amount='1125.00',date='2026-11-14',ref='EW-BROWSER',payment_key=key,payment_method='eWallet'))
                    self.assertEqual(response.status,200)
                    page.reload();self.assertEqual(page.locator('table tbody tr').count(),3)
                    page.locator('a[title="Print / Download SOA"]').click()
                    for value in ('2,148.32','eWallet','EW-BROWSER'):self.assertIn(value,page.locator('body').text_content())
                    self.assertEqual(errors,[])
                finally:browser.close()
        finally:server.shutdown();thread.join(timeout=5);server.server_close()
        self.db_session.expire_all()
        self.assertEqual(base.models.AitTokenWallet.query.filter_by(user_id=1).one().balance,50)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),count)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id,kind='credit').count(),1)

    def test_nav_removes_only_debtors_without_disabling_routes(self):
        source=(base.ROOT/'templates/partials/navbar.html').read_text(encoding='utf-8')
        for endpoint in set(re.findall(r"url_for\(['\"]([^'\"]+)",source)):
            if endpoint not in self.app.view_functions:self.app.add_url_rule('/nav-stub/'+endpoint,endpoint,lambda **kw:'stub')
        data=dict(nav_live_programs=[SimpleNamespace(slug='mechanic',name='ProTrade'),SimpleNamespace(slug='debtors',name='Debtors & Statements'),SimpleNamespace(slug='reading',name='Reading')],nav_coming_soon=[SimpleNamespace(slug='debtors',name='Debtors & Statements'),SimpleNamespace(slug='other',name='Other Program')],current_user=SimpleNamespace(is_authenticated=False))
        with self.app.test_request_context('/'):
            result=render_template('partials/navbar.html',**data)
        tree=html.fromstring(result)
        names=[text.strip() for text in tree.xpath('//li/text()') if text.strip()]
        self.assertEqual(names,['ProTrade','Reading','Other Program'])
        self.assertNotIn('Debtors & Statements',tree.text_content())
        for name in ('mechanic_bp.shop_debtor_register','mechanic_bp.client_ledger','mechanic_bp.client_ledger_add_payment','debtors_bp.generate_soa','debtors_bp.email_soa'):self.assertIn(name,self.app.view_functions)

if __name__=='__main__':unittest.main()
