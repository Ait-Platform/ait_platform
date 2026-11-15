"""VAT profiles, statutory settings, inclusive arithmetic and frozen invoices on PostgreSQL."""
from datetime import date,timedelta
from decimal import Decimal
from copy import deepcopy
import importlib.util,json
import unittest
from unittest.mock import patch
from uuid import uuid4
from lxml import html
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_boq_trade as boq
import test_protrade_shop_register as base
import app.protrade_vat as vat


class ProTradeVatTests(unittest.TestCase):
    setUp=boq.BoqTradeTests.setUp
    shop=boq.BoqTradeTests.shop
    save_debtor=boq.BoqTradeTests.save_debtor
    debtor=boq.BoqTradeTests.debtor
    save_quote=boq.BoqTradeTests.save_quote
    quote_url=boq.BoqTradeTests.quote_url
    ledger_url=boq.BoqTradeTests.ledger_url

    def prepare(self):
        self.boq,self.account=boq.BoqTradeTests.fixtures(self)
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES(1),(2)')
        self.conn.exec_driver_sql('DROP TABLE protrade_vat_rate')
        self.conn.exec_driver_sql('ALTER TABLE mech_shops DROP COLUMN vat_registered')
        for table in ('protrade_boq_quotes','mech_job_cards'):
            self.conn.exec_driver_sql(f'ALTER TABLE {table} DROP COLUMN tax_snapshot')
        for name in ('boq_quote_id','invoice_number','source_data'):
            self.conn.exec_driver_sql(f'ALTER TABLE mech_invoices DROP COLUMN {name} CASCADE')
        before={table:self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all() for table in ('debtor_ledger','protrade_boq_quotes','mech_job_cards','mech_invoices')}
        columns={table:[row['name'] for row in sa.inspect(self.conn).get_columns(table)] for table in before}
        spec=importlib.util.spec_from_file_location('vat_migration',base.ROOT/'migrations/versions/protrade_vat_invoices_018.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        migration.op=Operations(MigrationContext.configure(self.conn));migration.upgrade()
        for table,rows in before.items():
            names=','.join(self.conn.dialect.identifier_preparer.quote(name) for name in columns[table])
            self.assertEqual(self.conn.exec_driver_sql(f'SELECT {names} FROM {table} ORDER BY id').all(),rows)
        self.db_session.expire_all();self.db_session.commit()
        self.assertIsNone(self.boq.vat_registered)
        self.boq.vat_registered=True;self.boq.tax_number='4123456789';self.account.address='Customer physical address'
        self.db_session.commit()
        page=self.client.get(self.quote_url(self.account),query_string={'area':'new'})
        self.assertEqual(page.status_code,200)
        choices=json.loads(html.fromstring(page.data).get_element_by_id('quote-builder-data').text)['choices']
        self.work=str(choices[0]['source_id'])

    def create(self,amount='115',**changes):
        payload={'area':'new','quote_key':uuid4().hex,'item_id[]':[self.work],f'quantity_{self.work}':'1',
            f'material_rate_{self.work}':amount,f'labour_rate_{self.work}':'0','vat_treatment':'standard',
            'vat_inclusive':'yes','supply_date':date.today().isoformat()}
        payload.update(changes)
        response=self.client.post(self.quote_url(self.account),data=payload)
        self.assertEqual(response.status_code,302,response.get_data(as_text=True)[:200])
        return base.ProTradeBoqQuote.query.filter_by(shop_id=self.boq.id).order_by(base.ProTradeBoqQuote.id.desc()).first()

    def pay_sections(self,quote):
        url=f'/mechanic/shop/{self.boq.id}/boq/{quote.id}/sections'
        self.assertEqual(self.client.post(url,data=dict(action='save',name='Work',item_id=[self.work])).status_code,302)
        with patch('app.protrade_output.pdf_bytes',return_value=b'%PDF-test'):
            self.assertEqual(self.client.post(url,data=dict(action='generate',section_id=quote.boq_sections[0]['id'])).status_code,302)

    def test_inclusive_conversion_immutable_history_one_charge_and_isolation(self):
        self.prepare();quote=self.create();self.pay_sections(quote)
        self.assertEqual(quote.tax_snapshot['vat'],'15.00')
        self.assertEqual(quote.tax_snapshot['subtotal'],'100.00')
        self.assertEqual(base.DebtorLedger.query.filter_by(boq_quote_id=quote.id).one().amount,11500)
        ledger=[(row.id,row.amount,row.debtor_id) for row in base.DebtorLedger.query.all()]
        wallet=base.models.AitTokenWallet.query.filter_by(user_id=1).one();balance=wallet.balance
        url=f'/mechanic/shop/{self.boq.id}/boq/{quote.id}/invoice'
        payload=dict(confirm_invoice='yes',supply_date=date.today().isoformat(),vat_treatment='standard',recipient_vendor='yes',recipient_vat='4987654321')
        for _ in range(2):self.assertEqual(self.client.post(url,data=payload).status_code,302)
        invoice=base.catalog_globals['MechInvoice'].query.filter_by(boq_quote_id=quote.id).one()
        snapshot=deepcopy(invoice.source_data)
        self.assertEqual(snapshot['document']['title'],'Tax Invoice')
        self.assertEqual(snapshot['document']['tax']['vat'],'15.00')
        self.assertEqual(snapshot['document']['recipient']['vat_number'],'4987654321')
        self.assertEqual(wallet.balance,balance-10)
        self.assertEqual([(row.id,row.amount,row.debtor_id) for row in base.DebtorLedger.query.all()],ledger)
        for sql in [f"UPDATE mech_invoices SET total=999 WHERE id={invoice.id}",
            f"DELETE FROM mech_invoices WHERE id={invoice.id}",
            f"UPDATE protrade_boq_quotes SET grand_total=999 WHERE id={quote.id}",
            'UPDATE protrade_vat_rate SET rate=99']:
            with self.subTest(sql=sql),self.assertRaises(sa.exc.DBAPIError):
                with self.conn.begin_nested():self.conn.exec_driver_sql(sql)
        with self.client.session_transaction() as session:session['is_admin']=True
        self.assertEqual(self.client.post('/mechanic/admin/vat',data=dict(rate='16',effective_date=date.today().isoformat(),reason='Statutory change simulation in isolated test')).status_code,302)
        self.account.name='Updated directory name';self.db_session.commit()
        response=self.client.get('/mechanic/output',query_string=dict(shop_id=self.boq.id,selection=f'invoice:converted-{invoice.id}',preview=1))
        self.assertEqual(response.status_code,200)
        self.assertIn('15.00',response.get_data(as_text=True))
        self.assertEqual(invoice.source_data,snapshot)
        with self.client.session_transaction() as session:session['_user_id']='2';session.pop('is_admin',None)
        self.assertEqual(self.client.get(url).status_code,404)
        self.assertEqual(self.client.get('/mechanic/output',query_string=dict(shop_id=self.boq.id,selection=f'invoice:converted-{invoice.id}',preview=1)).status_code,404)

    def test_nonvendor_zero_exempt_and_controlled_standard_rate(self):
        self.prepare()
        with self.app.test_request_context():
            self.assertEqual(vat.calculate('100',self.boq)['total'],'115.00')
            self.assertEqual(vat.calculate('115',self.boq,inclusive=True)['vat'],'15.00')
            self.assertEqual(vat.calculate('100',self.boq,'zero',basis='Qualifying export under applicable VAT provision')['vat'],'0.00')
            self.assertEqual(vat.calculate('100',self.boq,'exempt',basis='Qualifying exempt supply under applicable VAT provision')['vat'],'0.00')
            with self.assertRaisesRegex(ValueError,'legal basis'):vat.calculate('100',self.boq,'zero')
        self.boq.vat_registered=False;self.db_session.commit()
        quote=self.create('100',vat_inclusive='',vat_rate='99')
        self.assertEqual(quote.tax_snapshot['vat'],'0.00')
        self.assertEqual(quote.grand_total,Decimal('100.00'))
        self.pay_sections(quote)
        response=self.client.post(f'/mechanic/shop/{self.boq.id}/boq/{quote.id}/invoice',data=dict(confirm_invoice='yes',supply_date=date.today().isoformat()))
        self.assertEqual(response.status_code,302)
        invoice=base.catalog_globals['MechInvoice'].query.filter_by(boq_quote_id=quote.id).one()
        self.assertEqual(invoice.source_data['document']['title'],'Invoice')
        self.assertEqual(invoice.source_data['document']['tax']['vat'],'0.00')

    def test_effective_dates_admin_only_and_unknown_registration(self):
        self.prepare()
        url='/mechanic/admin/vat'
        self.assertEqual(self.client.get(url).status_code,403)
        with self.client.session_transaction() as session:session['is_admin']=True
        tomorrow=date.today()+timedelta(days=1)
        self.assertEqual(self.client.post(url,data=dict(rate='17',effective_date=tomorrow.isoformat(),reason='Future statutory rate test')).status_code,302)
        with self.app.test_request_context():
            self.assertEqual(vat.rate_for(date.today()).rate,15)
            self.assertEqual(vat.rate_for(tomorrow).rate,17)
        self.assertEqual(self.client.post(url,data=dict(rate='99',effective_date=(date.today()-timedelta(days=1)).isoformat(),reason='Invalid retroactive change')).status_code,400)
        self.boq.vat_registered=None
        with self.app.test_request_context(),self.assertRaisesRegex(ValueError,'Confirm VAT registration'):
            vat.calculate(100,self.boq)

    def test_browser_inclusive_preview_uses_approved_rate(self):
        self.prepare()
        response=self.client.get(self.quote_url(self.account),query_string={'area':'new'})
        choices=json.loads(html.fromstring(response.data).get_element_by_id('quote-builder-data').text)['choices']
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(response.get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                page.locator('#quote-work-select').fill(choices[0]['label'])
                page.locator('#quote-work-material').fill('115');page.locator('#quote-work-labour').fill('0')
                page.locator('#quote-work-quantity').fill('1');page.locator('[name=vat_inclusive]').check()
                page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('#quote-vat').text_content(),'15.00')
                self.assertEqual(page.locator('#quote-subtotal').text_content(),'100.00')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'115.00')
                page.locator('[name=vat_inclusive]').uncheck()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'132.25')
            finally:browser.close()
