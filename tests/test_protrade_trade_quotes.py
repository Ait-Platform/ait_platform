"""Stage 3E actual routes/migration on isolated PostgreSQL, plus browser totals."""
import ast
from decimal import Decimal
import importlib.util
import unittest
from uuid import uuid4
from lxml import html
from alembic.migration import MigrationContext
from alembic.operations import Operations
from playwright.sync_api import sync_playwright
import sqlalchemy as sa
import test_protrade_carpentry_boq as base
from test_protrade_shop_register import ProTradeBoqQuote, models, catalog_db, catalog_globals

class TokenTransaction(models.Base):
    __tablename__ = 'quote_test_token_transactions'
    id = sa.Column(sa.Integer, primary_key=True)
    wallet_id = sa.Column(sa.Integer)
    amount = sa.Column(sa.Integer)
    description = sa.Column(sa.Text)

class InviteLog(models.Base):
    __tablename__ = 'quote_test_invites'
    id = sa.Column(sa.Integer, primary_key=True)
    sender_id = sa.Column(sa.Integer)
    recipient_phone = sa.Column(sa.String)
    program_slug = sa.Column(sa.String)
    invite_type = sa.Column(sa.String)
    status = sa.Column(sa.String)


class TradeQuoteTests(unittest.TestCase):
    shop = base.CarpentryBoqTests.shop
    snapshot = base.CarpentryBoqTests.snapshot

    def setUp(self):
        base.CarpentryBoqTests.setUp(self)
        self.ns.update(text=sa.text, AitTokenTransaction=TokenTransaction, InviteLog=InviteLog,
                       MechLaborLine=catalog_globals['MechLaborLine'], MechPartLine=catalog_globals['MechPartLine'])
        self.conn.exec_driver_sql('CREATE TEMP TABLE system_settings (key text PRIMARY KEY, value text)')
        self.db_session.add(models.AitTokenWallet(user_id=1, balance=50))
        self.customer = models.MechClient(user_id=1, name='Customer Jane', phone='0123456789', email='jane@example.test', address='Customer address')
        self.foreign_customer = models.MechClient(user_id=2, name='Foreign Customer')
        self.db_session.add_all([self.customer, self.foreign_customer])
        self.db_session.commit()
        self.item = self.boq(self.carpentry, 'CAR-004', 'Install skirting', 'm', '85', '45', '99')
        self.each_item = self.boq(self.carpentry, 'CAR-001', 'Wooden door', 'Each', '1200', '650')
        self.foreign_item = self.boq(self.other, 'CAR-004', 'Other owner work', 'm', '1', '1')
        self.second_item = self.boq(self.second, 'CAR-004', 'Other shop work', 'm', '1', '1')
        # Real migration applied on an existing quote/job-card schema.
        self.conn.exec_driver_sql('DROP TABLE protrade_boq_quotes')
        self.conn.exec_driver_sql('ALTER TABLE mech_job_cards DROP COLUMN shop_id')
        spec = importlib.util.spec_from_file_location('quote_migration', base.ROOT / 'migrations/versions/protrade_quote_005.py')
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)
        self.migration.op = Operations(MigrationContext.configure(self.conn))
        self.migration.upgrade()
        spec=importlib.util.spec_from_file_location('foundation_migration',base.ROOT/'migrations/versions/protrade_debtor_006.py')
        foundation=importlib.util.module_from_spec(spec);spec.loader.exec_module(foundation)
        foundation.op=Operations(MigrationContext.configure(self.conn));foundation.upgrade()
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes ADD COLUMN quotation_seq integer')
        self.customers_by_shop={}
        self.debtors_by_shop={}
        for shop in (self.carpentry,self.second,self.mechanic,self.other):
            if shop.id==self.carpentry.id:
                customer=self.customer
            elif shop.id==self.other.id:
                customer=self.foreign_customer
            else:
                customer=models.MechClient(user_id=shop.user_id,name='Customer Jane',phone='0123456789',email='jane@example.test',address='Customer address')
                self.db_session.add(customer)
            customer.shop_id=shop.id
            self.db_session.flush()
            debtor=models.Debtor(user_id=shop.user_id,shop_id=shop.id,reference_id=customer.id,slug_reference='protrade',name=customer.name,phone=customer.phone,email=customer.email,address=customer.address)
            self.db_session.add(debtor);self.db_session.flush()
            self.customers_by_shop[shop.id]=customer
            self.debtors_by_shop[shop.id]=debtor
        self.db_session.commit()
        self.boq_before = self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()

    def boq(self, shop, code, description, unit, material, labour, quantity='1'):
        item = base.ProTradeBoqItem(user_id=shop.user_id, shop_id=shop.id, trade=shop.trade, code=code, description=description,
                                    unit=unit, material_rate=Decimal(material), labour_rate=Decimal(labour), quantity=Decimal(quantity))
        self.db_session.add(item)
        self.db_session.commit()
        return item

    def quote_url(self, shop=None, customer=True):
        shop = shop or self.carpentry
        selected=self.customers_by_shop.get(shop.id,self.customer)
        debtor=self.debtors_by_shop.get(shop.id)
        return f'/mechanic/shop/{shop.id}/quote' + (f'?client_id={selected.id}&debtor_id={debtor.id}' if customer and debtor else '')

    def post(self, expected=302, shop=None, customer=True, **changes):
        data = dict(quote_key=uuid4().hex, **{'item_id[]': [str(self.item.id)]}, **{f'quantity_{self.item.id}': '2.5'})
        data.update(changes)
        response = self.client.post(self.quote_url(shop, customer), data=data)
        self.assertEqual(response.status_code, expected, response.get_data(as_text=True)[:300])
        return response

    def test_customer_details_scoped_boq_and_no_vehicle_fields(self):
        response = self.client.get(self.quote_url())
        self.assertEqual(response.status_code, 200)
        tree = html.fromstring(response.data)
        self.assertEqual(tree.get_element_by_id('quote-client-name').text_content(), 'Customer Jane')
        text = tree.text_content()
        self.assertFalse(tree.xpath('//button[@data-quote-area="debtor"]'))
        self.assertIn('Install skirting', text)
        self.assertNotIn('Other owner work', text)
        self.assertNotIn('Other shop work', text)
        self.assertEqual(tree.xpath('//tr[contains(@class,"quote-line")]'), [])
        self.assertFalse(tree.xpath('//input[@name="vehicle_reg" or @name="vin_number" or @name="mileage" or @name="make"]'))
        for word in ('Vehicle Registration', 'VIN', 'Mileage', 'Vehicle Make'):
            self.assertNotIn(word, text)
        self.assertEqual(models.MechVehicle.query.count(), 0)

    def test_save_decimal_line_totals_grand_total_and_snapshots(self):
        response = self.post(**{'item_id[]': [str(self.item.id), str(self.each_item.id)], f'quantity_{self.each_item.id}': '2', 'material_rate': '0', 'grand_total': '0'})
        quote = ProTradeBoqQuote.query.one()
        self.assertEqual((quote.user_id, quote.shop_id, quote.client_id, quote.trade), (1, self.carpentry.id, self.customer.id, 'carpentry'))
        self.assertEqual(quote.grand_total, Decimal('4025.00'))
        self.assertEqual([line['total'] for line in quote.items], ['325.00', '3700.00'])
        self.assertEqual(quote.items[0]['quantity'], '2.5')
        self.assertEqual(quote.client_name, 'Customer Jane')
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(), self.boq_before)
        self.assertEqual(self.snapshot(), self.original)
        self.assertEqual(models.MechVehicle.query.count(), 0)
        self.item.material_rate = Decimal('999')
        self.customer.name = 'Changed later'
        self.db_session.commit()
        saved = self.client.get(response.location)
        self.assertEqual(saved.status_code, 200)
        self.assertIn('4025.00', saved.get_data(as_text=True))
        self.assertIn('Customer Jane', saved.get_data(as_text=True))
        self.assertEqual(quote.items[0]['material_rate'], '85.00')

    def test_isolation_forged_lines_and_unsupported_trade(self):
        for item in (self.foreign_item, self.second_item):
            self.post(expected=400, **{'item_id[]': [str(item.id)], f'quantity_{item.id}': '1'})
        self.assertEqual(self.client.get(self.quote_url(self.other)).status_code, 404)
        self.assertEqual(self.client.get(f'/mechanic/shop/{self.carpentry.id}/quote?client_id={self.foreign_customer.id}&debtor_id={self.debtors_by_shop[self.carpentry.id].id}').status_code, 404)
        self.assertEqual(ProTradeBoqQuote.query.count(), 0)
        response = self.post()
        quote = ProTradeBoqQuote.query.one()
        self.assertEqual(self.client.get(f'/mechanic/shop/{self.second.id}/quotes/{quote.id}').status_code, 404)
        with self.client.session_transaction() as sess:
            sess['_user_id'] = '2'
        self.assertEqual(self.client.get(response.location).status_code, 404)

    def test_registered_shop_requires_debtor_before_quotation(self):
        response=self.client.get(self.quote_url(customer=False))
        self.assertEqual(response.status_code,302)
        self.assertIn('/debtors',response.location)
        self.assertEqual(ProTradeBoqQuote.query.count(),0)
        self.assertEqual(self.client.post(self.quote_url(self.other),data={}).status_code,404)

    def test_migration_preserves_preexisting_mechanic_quotation_fields(self):
        self.conn.exec_driver_sql('DROP TABLE protrade_boq_quotes')
        self.conn.exec_driver_sql('ALTER TABLE mech_job_cards DROP COLUMN shop_id')
        self.conn.execute(sa.text("INSERT INTO mech_vehicles (id,client_id,make,license_plate) VALUES (-10,:client,'Legacy Make','OLD123')"), dict(client=self.customer.id))
        self.conn.exec_driver_sql("INSERT INTO mech_job_cards (id,job_number,vehicle_id,status,description,vat_rate,payment_method) VALUES (-10,'LEGACY-QUOTE',-10,'Quote','Existing quote',15,'EFT')")
        old_columns = ','.join(column['name'] for column in sa.inspect(self.conn).get_columns('mech_job_cards'))
        before = self.conn.exec_driver_sql(f'SELECT {old_columns} FROM mech_job_cards ORDER BY id').all()
        self.migration.upgrade()
        self.assertEqual(self.conn.exec_driver_sql(f'SELECT {old_columns} FROM mech_job_cards ORDER BY id').all(), before)
        self.assertIsNone(self.conn.exec_driver_sql("SELECT shop_id FROM mech_job_cards WHERE job_number='LEGACY-QUOTE'").scalar())
        self.assertEqual(self.snapshot(), self.original)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(), self.boq_before)

    def test_missing_shop_requires_selection_and_stale_session_cannot_override(self):
        response = self.client.get(f'/mechanic/client/{self.customer.id}/quote')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/mechanic/dashboard', response.location)
        with self.client.session_transaction() as sess:
            sess['active_shop_id'] = self.mechanic.id
            sess['active_trade'] = 'mechanic'
        response = self.client.get(f'/mechanic/client/{self.customer.id}/quote?shop_id={self.carpentry.id}&debtor_id={self.debtors_by_shop[self.carpentry.id].id}')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Carpentry Quotation', response.get_data(as_text=True))
        self.assertEqual(ProTradeBoqQuote.query.count(), 0)

    def test_same_customer_can_quote_multiple_shops_without_new_customer_records(self):
        count = models.MechClient.query.count()
        self.post()
        self.post(shop=self.second, **{'item_id[]': [str(self.second_item.id)], f'quantity_{self.second_item.id}': '3'})
        quotes = ProTradeBoqQuote.query.order_by(ProTradeBoqQuote.id).all()
        self.assertEqual([quote.client_id for quote in quotes], [self.customer.id, self.customers_by_shop[self.second.id].id])
        self.assertEqual([quote.shop_id for quote in quotes], [self.carpentry.id, self.second.id])
        self.assertEqual([quote.grand_total for quote in quotes], [Decimal('325.00'), Decimal('6.00')])
        self.assertEqual(models.MechClient.query.count(), count)
        mechanic = self.client.get(self.quote_url(self.mechanic))
        self.assertEqual(mechanic.status_code, 302)
        self.assertIn(f'client_id={self.customers_by_shop[self.mechanic.id].id}', mechanic.location)

    def test_invalid_quantities_empty_selection_and_repeat_submission(self):
        for quantity in ('', '0', '-1', 'NaN', 'Infinity', '1.0001', '100000000000'):
            self.post(expected=400, **{f'quantity_{self.item.id}': quantity})
        self.post(expected=400, **{'item_id[]': []})
        self.post(expected=400, **{'item_id[]': [str(self.item.id), str(self.item.id)]})
        key = uuid4().hex
        first = self.post(quote_key=key)
        second = self.post(quote_key=key)
        self.assertIn('/quotes/', first.location)
        self.assertIn('area=register', second.location)
        self.assertEqual(self.client.get(second.location).status_code,200)
        self.assertEqual(ProTradeBoqQuote.query.count(), 1)

    def test_mechanic_uses_existing_form_and_save_links_exact_customer_and_shop(self):
        response = self.client.get(self.quote_url(self.mechanic))
        self.assertEqual(response.status_code, 302)
        self.assertIn('/mechanic/quote/new', response.location)
        form = self.client.get(response.location)
        self.assertEqual(form.status_code, 200)
        tree = html.fromstring(form.data)
        self.assertEqual(tree.xpath('//input[@name="customer_name"]/@value'), ['Customer Jane'])
        self.assertTrue(tree.xpath('//input[@name="vehicle_reg"]'))
        self.assertTrue(tree.xpath('//input[@name="vin_number"]'))
        self.assertEqual(tree.xpath('//input[@name="shop_id"]/@value'), [str(self.mechanic.id)])
        saved = self.client.post('/mechanic/quote/new', data=dict(shop_id=self.mechanic.id, client_id=self.customers_by_shop[self.mechanic.id].id, debtor_id=self.debtors_by_shop[self.mechanic.id].id,
                                  customer_name='Edited display name', vehicle_reg='ABC123'))
        self.assertEqual(saved.status_code, 302)
        card = models.MechJobCard.query.one()
        self.assertEqual(card.shop_id, self.mechanic.id)
        self.assertEqual(card.vehicle.client_id, self.customers_by_shop[self.mechanic.id].id)
        self.assertEqual(models.AitTokenWallet.query.one().balance, 45)
        self.assertEqual(ProTradeBoqQuote.query.count(), 0)
        self.assertEqual(self.snapshot(), self.original)

    def test_register_quote_only_and_browser_grand_total(self):
        response = self.client.get(f'/mechanic/dashboard?shop_id={self.carpentry.id}')
        tree = html.fromstring(response.data)
        row = tree.xpath('//tr[td/a[text()="Happy Carpentry"]]')[0]
        self.assertEqual(row.xpath('./td[last()]/a/text()'),['Debtor Register'])
        self.assertNotIn('Customer Jane',tree.get_element_by_id('client-register-table').text_content())
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_content(self.client.get(self.quote_url()).get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                page.evaluate("document.getElementById('quote-area-form').hidden=false")
                page.locator('#quote-work-select').fill('CAR-004 \u2014 Install skirting')
                page.locator('#quote-work-quantity').fill('2.5')
                page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(), '325.00')
                page.locator('#quote-work-select').fill('CAR-001 \u2014 Supply and install wooden door')
                page.locator('#quote-work-quantity').fill('2')
                page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(), '4025.00')
            finally:
                browser.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
