"""PostgreSQL and browser regressions for debtor identity and first quotations."""
import ast
from copy import deepcopy
from datetime import date
from decimal import Decimal
import importlib.util
import sys
from types import ModuleType
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit
from uuid import uuid4
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_quote_ledger as ledger
import test_protrade_payment_methods as payments


class DebtorReuseTests(unittest.TestCase):
    setUp=payments.PaymentMethodTests.setUp
    shop=payments.PaymentMethodTests.shop
    save_debtor=payments.PaymentMethodTests.save_debtor
    debtor=payments.PaymentMethodTests.debtor
    save_quote=payments.PaymentMethodTests.save_quote
    quote_url=payments.PaymentMethodTests.quote_url
    ledger_url=payments.PaymentMethodTests.ledger_url

    def fixtures(self):
        payments.PaymentMethodTests.fixtures(self)
        self.conn.exec_driver_sql('ALTER TABLE debtor DROP COLUMN identity_debtor_id CASCADE')
        tables=('debtor','debtor_ledger','protrade_boq_quotes','mech_catalog_parts','protrade_boq_items')
        before={name:self.conn.exec_driver_sql('SELECT * FROM '+name+' ORDER BY id').all() for name in tables}
        columns=[column['name'] for column in sa.inspect(self.conn).get_columns('debtor')]
        spec=importlib.util.spec_from_file_location('identity_migration',base.ROOT/'migrations/versions/protrade_debtor_identity_011.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        module.op=Operations(MigrationContext.configure(self.conn));module.upgrade()
        for name,rows in before.items():
            query='SELECT '+(','.join(columns) if name=='debtor' else '*')+' FROM '+name+' ORDER BY id'
            self.assertEqual(self.conn.exec_driver_sql(query).all(),rows)
        self.db_session.expire_all()
        self.wallet=base.models.test_wallet(self.db_session,balance=50)
        self.db_session.add(self.wallet);self.db_session.commit()

    def link(self,shop,source,**extras):
        return self.client.post(f'/mechanic/shop/{shop.id}/debtors',data=dict(existing_debtor_id=source.id,**extras))

    def test_reuse_identity_separate_accounts_repeat_link_and_ownership(self):
        self.fixtures()
        third=self.shop(name='Another Carpentry',trade='carpentry',status='active')
        old=(self.tomas.id,self.tomas.reference_id,deepcopy(self.quotes[0].items),self.quotes[0].grand_total)
        count=base.models.Debtor.query.count()
        for _ in range(3):self.assertEqual(self.link(third,self.tomas,name='Forged',opening_balance='9999').status_code,302)
        account=base.models.Debtor.query.filter_by(shop_id=third.id).one()
        self.assertEqual(base.models.Debtor.query.count(),count+1)
        self.assertEqual((account.identity_debtor_id,account.name,account.phone,account.email,account.opening_balance),(self.tomas.id,self.tomas.name,self.tomas.phone,self.tomas.email,0))
        self.assertNotEqual(account.id,self.tomas.id)
        self.assertNotEqual(account.reference_id,self.tomas.reference_id)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=account.id).count(),0)
        self.assertIn('0.00',self.client.get(self.ledger_url(account)).get_data(as_text=True))
        self.assertNotIn('3273.32',self.client.get(self.ledger_url(account)).get_data(as_text=True))
        url=self.quote_url(account)
        payload={'area':'new','quote_key':uuid4().hex,'item_id[]':['template:CAR-003'],'quantity_template:CAR-003':'2','material_rate_template:CAR-003':'259','labour_rate_template:CAR-003':'65'}
        self.assertEqual(self.client.post(url,data=payload).status_code,302)
        self.assertEqual(self.client.post(url,data=payload).status_code,302)
        self.assertEqual(base.ProTradeBoqQuote.query.filter_by(debtor_id=account.id).count(),1)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=account.id).one().amount,64800)
        self.assertEqual(sum(row.amount for row in base.DebtorLedger.query.filter_by(debtor_id=self.tomas.id)),327332)
        self.assertEqual((self.tomas.id,self.tomas.reference_id,self.quotes[0].items,self.quotes[0].grand_total),old)
        self.assertEqual(self.wallet.balance,40)
        self.assertEqual(self.client.get(self.ledger_url(account)+f'?shop_id={self.carpentry.id}').status_code,404)
        self.assertEqual(self.client.get(f'/mechanic/shop/{third.id}/debtor/{self.tomas.id}/quote').status_code,404)
        foreign=base.models.Debtor(user_id=2,shop_id=self.foreign.id,name='Private',opening_balance=0)
        self.db_session.add(foreign);self.db_session.commit()
        self.assertEqual(self.link(third,foreign).status_code,404)
        self.assertNotIn('Private',self.client.get(f'/mechanic/shop/{third.id}/debtors').get_data(as_text=True))
        with self.conn.begin_nested() as tx:
            with self.assertRaises(sa.exc.IntegrityError):
                self.conn.execute(sa.text("INSERT INTO debtor (user_id,shop_id,name,identity_debtor_id) VALUES(1,:shop,'Duplicate',:identity)"),dict(shop=third.id,identity=self.tomas.id))
            tx.rollback()

    def test_dick_generic_first_quote_ledger_and_item_editing(self):
        self.fixtures()
        self.save_debtor(self.second,name='Dick',phone='000')
        dick=base.models.Debtor.query.filter_by(shop_id=self.second.id,name='Dick').one()
        page=html.fromstring(self.client.get(f'/mechanic/shop/{self.second.id}/debtors').data)
        row=page.xpath('//table[@id="shop-debtors"]//tr[td/a[text()="Dick"]]')[0]
        self.assertEqual(row.xpath('.//a[text()="Quote"]/@href'),[self.quote_url(dick)])
        self.assertTrue(row.xpath('.//a[text()="Ledger"]'));self.assertFalse(row.xpath('.//*[@disabled]'))
        self.assertEqual(self.client.get(self.quote_url(dick)).status_code,200)
        self.assertIn('0.00',self.client.get(self.ledger_url(dick)).get_data(as_text=True))
        data={'area':'new','quote_key':uuid4().hex,'item_id[]':['template:WORK-001'],'quantity_template:WORK-001':'2','material_rate_template:WORK-001':'350','labour_rate_template:WORK-001':'250','description_template:WORK-001':'Agreed general services'}
        saved=self.client.post(self.quote_url(dick),data=data);self.assertEqual(saved.status_code,302)
        self.assertEqual(self.client.get(saved.location).status_code,200)
        self.assertEqual(self.client.post(self.quote_url(dick),data=data).status_code,302)
        quote=base.ProTradeBoqQuote.query.filter_by(debtor_id=dick.id).one()
        self.assertEqual((quote.trade,quote.grand_total,quote.items[0]['description']),('generic',Decimal('1200.00'),'Agreed general services'))
        debit=base.DebtorLedger.query.filter_by(debtor_id=dick.id).one();debit_id=debit.id
        self.assertEqual(debit.amount,120000)
        self.assertIn('1200.00',self.client.get(self.ledger_url(dick)).get_data(as_text=True))
        self.assertEqual(self.client.get(f'/debtor/{dick.id}/soa').status_code,200)
        item_id=self.ns['carpentry_quote_item_id'](quote,quote.items[0])
        item_url=f'/mechanic/shop/{self.second.id}/debtor/{dick.id}/quotes/{quote.id}/items'
        tree=html.fromstring(self.client.get(item_url+'?item_id='+item_id).data)
        edit={node.name:node.value for node in tree.xpath('//form//input[@name]')};edit.update(unit='Each',quantity='3',description='Agreed general services')
        identity=(quote.id,quote.quote_number,quote.quotation_seq)
        self.assertEqual(self.client.post(item_url,data=edit).status_code,302)
        self.assertEqual(quote.grand_total,Decimal('1800.00'))
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=dick.id).one().id,debit_id)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=dick.id).one().amount,180000)
        self.assertEqual((quote.id,quote.quote_number,quote.quotation_seq),identity)
        payment=dict(amount='500.00',date='2026-11-14',payment_method='Cash',ref='DICK-500',payment_key=uuid4().hex)
        for _ in range(2):self.assertEqual(self.client.post(self.ledger_url(dick)+'/add_payment',data=payment).status_code,302)
        self.assertEqual(base.DebtorLedger.query.filter_by(debtor_id=dick.id,kind='credit').count(),1)
        self.assertIn('1300.00',self.client.get(self.ledger_url(dick)).get_data(as_text=True))
        statement=self.client.get(f'/debtor/{dick.id}/soa').get_data(as_text=True)
        self.assertIn('1,300.00',statement);self.assertNotIn('Q-0005',statement)
        self.assertEqual(self.wallet.balance,40)

    def test_real_new_quote_imports_do_not_shadow_module_jobcard(self):
        self.fixtures();self.save_debtor(self.mechanic,name='Mechanic customer')
        debtor=self.debtor(self.mechanic)
        node=next(node for node in ast.parse((base.ROOT/'app/program_mechanic/routes.py').read_text(encoding='utf-8')).body if isinstance(node,ast.FunctionDef) and node.name=='new_quote')
        node.decorator_list=[]
        # Keep actual local imports, unlike the older route-isolation harness.
        namespace=dict(self.ns);exec(compile(ast.Module(body=[node],type_ignores=[]),'actual_new_quote','exec'),namespace)
        fake_debtors=ModuleType('app.models.debtors');fake_debtors.Debtor=base.models.Debtor
        fake_mechanic=ModuleType('app.models.mechanic');fake_mechanic.MechClient=base.models.MechClient
        with patch.dict(sys.modules,{'app.models.debtors':fake_debtors,'app.models.mechanic':fake_mechanic}):
            with self.app.test_request_context(f'/mechanic/quote/new?shop_id={self.mechanic.id}&debtor_id={debtor.id}'):
                from flask_login import login_user
                login_user(self.db_session.get(base.models.User,1))
                result=namespace['new_quote']()
                self.assertIn('Mechanic customer',result)
        self.assertEqual(self.client.get(f'/mechanic/quote/new?shop_id={self.mechanic.id}&debtor_id={debtor.id}').status_code,200)
        self.assertEqual(base.models.MechJobCard.query.count(),0);self.assertEqual(self.wallet.balance,50)

    def browser_route(self,route):
        request=route.request;url=urlsplit(request.url)
        if url.path.startswith('/static/js/'):
            return route.fulfill(path=str(base.ROOT/'app'/url.path.lstrip('/')),content_type='application/javascript')
        response=self.client.open(url.path+('?' +url.query if url.query else ''),method=request.method,data=request.post_data,content_type=request.headers.get('content-type'),follow_redirects=True)
        route.fulfill(status=response.status_code,headers={k:v for k,v in response.headers if k.lower() not in ('content-length','set-cookie')},body=response.data)

    def test_browser_reuse_search_and_dick_first_quote(self):
        self.fixtures()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.goto(f'http://localhost/mechanic/shop/{self.second.id}/debtors')
                page.get_by_role('button',name='Add Debtor',exact=True).click()
                option=page.locator(f'#existing-debtor-options option[data-id="{self.tomas.id}"]')
                page.locator('#existing-debtor-search').fill(option.get_attribute('value'))
                self.assertEqual(page.locator('input[name=name]').input_value(),'Tomas')
                page.get_by_role('button',name='Save',exact=True).click()
                self.assertEqual(base.models.Debtor.query.filter_by(shop_id=self.second.id,identity_debtor_id=self.tomas.id).count(),1)
                page.get_by_role('button',name='Add Debtor',exact=True).click()
                page.locator('input[name=name]').fill('Dick');page.get_by_role('button',name='Save',exact=True).click()
                row=page.locator('#shop-debtors tr').filter(has=page.get_by_role('link',name='Dick',exact=True))
                row.get_by_role('link',name='Ledger',exact=True).click()
                self.assertIn('0.00',page.locator('body').inner_text())
                page.goto(f'http://localhost/mechanic/shop/{self.second.id}/debtors')
                row=page.locator('#shop-debtors tr').filter(has=page.get_by_role('link',name='Dick',exact=True))
                row.get_by_role('link',name='Quote',exact=True).click()
                self.assertTrue(page.locator('#quote-area-register').is_visible())
                self.assertEqual(page.locator('input[name=vehicle_reg]').count(),0)
                page.get_by_role('button',name='New Quote',exact=True).click()
                page.locator('#quote-work-select').fill('WORK-001')
                page.locator('#quote-work-material').fill('350');page.locator('#quote-work-labour').fill('250');page.locator('#quote-work-quantity').fill('2')
                page.get_by_role('button',name='Add Item',exact=True).click()
                page.get_by_label('Description WORK-001',exact=True).fill('General business services')
                page.get_by_role('button',name='Save Quote',exact=True).click()
                dick=base.models.Debtor.query.filter_by(shop_id=self.second.id,name='Dick').one()
                quote=base.ProTradeBoqQuote.query.filter_by(debtor_id=dick.id).one()
                self.assertEqual(quote.grand_total,Decimal('1200.00'))
                self.assertEqual(quote.items[0]['description'],'General business services')
                self.assertEqual(self.wallet.balance,40)
            finally:browser.close()

if __name__=='__main__':unittest.main()
