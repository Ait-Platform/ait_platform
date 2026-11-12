"""Stage 3G: read-only shop-specific document register on PostgreSQL."""
from decimal import Decimal
import unittest
from lxml import html
import test_protrade_shop_register as base


class QuoteInvoiceRegisterTests(unittest.TestCase):
    shop = base.ShopRegisterTests.shop

    def setUp(self):
        base.ShopRegisterTests.setUp(self)
        self.mechanic = self.shop(name='Mechanic Shop', status='active')
        self.second = self.shop(name='Second Shop', status='active')
        self.carpentry = self.shop(name='Carpentry Shop', trade='carpentry', status='active')
        self.foreign = self.shop(owner=2, name='Other Owner', status='active')
        self.customer = base.models.MechClient(user_id=1, name='Our Customer')
        self.foreign_customer = base.models.MechClient(user_id=2, name='Foreign Customer')
        self.db_session.add_all([self.customer, self.foreign_customer])
        self.db_session.commit()
        self.quote = self.job('OUR-QUOTE', self.mechanic)
        self.approved = self.job('OUR-APPROVED', self.mechanic, status='Approved')
        self.billed = self.job('OUR-BILLED', self.mechanic, status='Billed')
        self.legacy_bill = self.job('OLD-BILL', self.mechanic, status='Billed')
        self.unlinked = self.job('UNLINKED', None)
        self.job('SECOND-QUOTE', self.second)
        self.job('FOREIGN-SHOP', self.foreign)
        self.job('FOREIGN-CUSTOMER', self.mechanic, customer=self.foreign_customer)
        self.db_session.add(base.catalog_globals['MechInvoice'](job_card_id=self.billed.id, subtotal=100, tax=15, total=115, status='Unpaid'))
        self.db_session.add(base.catalog_globals['MechPartLine'](job_card_id=self.quote.id, description='Part', quantity=2, markup_price=100))
        self.db_session.add(base.catalog_globals['MechLaborLine'](job_card_id=self.quote.id, hours=1.5, rate_per_hour=200))
        self.db_session.add(base.ProTradeBoqQuote(user_id=1, shop_id=self.carpentry.id, client_id=self.customer.id, trade='carpentry',
                                               quote_number='CAR-Q-SAVED', client_name='Our Customer', items=[], grand_total=Decimal('325.00')))
        self.db_session.commit()

    def job(self, number, shop, status='Quote', customer=None):
        customer = customer or self.customer
        vehicle = base.models.MechVehicle(client_id=customer.id, make='Car', license_plate=number)
        self.db_session.add(vehicle)
        self.db_session.flush()
        job = base.models.MechJobCard(job_number=number, shop_id=shop.id if shop else None, vehicle_id=vehicle.id, status=status, vat_rate=15)
        self.db_session.add(job)
        self.db_session.commit()
        return job

    def page(self, shop):
        return self.client.get(f'/mechanic/quote-invoice?shop_id={shop.id}')

    def test_selected_mechanic_quotes_invoices_and_existing_actions(self):
        response = self.page(self.mechanic)
        self.assertEqual(response.status_code, 200)
        tree = html.fromstring(response.data)
        table = tree.get_element_by_id('quote-invoice-register')
        text = table.text_content()
        for number in ('OUR-QUOTE','OUR-APPROVED','OUR-BILLED','OLD-BILL'):
            self.assertIn(number, text)
        for number in ('SECOND-QUOTE','FOREIGN-SHOP','FOREIGN-CUSTOMER','UNLINKED','CAR-Q-SAVED'):
            self.assertNotIn(number, text)
        quote_row = table.xpath('.//tr[td[text()="OUR-QUOTE"]]')[0]
        self.assertIn('575.00', quote_row.text_content())
        invoice = table.xpath('.//tr[td[text()="OUR-BILLED"]]')[0]
        self.assertEqual(invoice.xpath('./td[1]/text()'), ['Invoice'])
        self.assertIn('115.00', invoice.text_content())
        from flask import url_for
        with self.app.test_request_context('/'):
            expected = url_for('mechanic_bp.job_card_detail', id=self.quote.id, shop_id=self.mechanic.id)
        self.assertEqual(quote_row.xpath('./td[last()]/a/@href'), [expected])
        self.assertFalse(tree.xpath('//form[@method="POST"]'))

    def test_carpentry_only_shows_its_saved_quotes(self):
        response = self.page(self.carpentry)
        tree = html.fromstring(response.data)
        table = tree.get_element_by_id('quote-invoice-register')
        self.assertIn('CAR-Q-SAVED', table.text_content())
        self.assertIn('325.00', table.text_content())
        self.assertNotIn('OUR-QUOTE', table.text_content())
        quote = base.ProTradeBoqQuote.query.one()
        self.assertEqual(table.xpath('.//a/@href'), [f'/mechanic/shop/{self.carpentry.id}/quotes/{quote.id}'])
        self.assertNotIn('Accepted', table.text_content())

    def test_viewing_register_preserves_records_and_does_not_bill_accepted_work(self):
        before = {table:self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all()
                  for table in ('mech_shops','mech_job_cards','mech_invoices','protrade_boq_quotes','mech_clients')}
        for shop in (self.mechanic, self.carpentry, self.second):
            self.assertEqual(self.page(shop).status_code, 200)
        for table, records in before.items():
            self.assertEqual(self.conn.exec_driver_sql(f'SELECT * FROM {table} ORDER BY id').all(), records)
        self.assertEqual(self.approved.status, 'Approved')
        self.assertEqual(base.catalog_globals['MechInvoice'].query.count(), 1)

    def test_foreign_missing_and_no_selected_shop(self):
        self.assertEqual(self.page(self.foreign).status_code, 404)
        self.assertEqual(self.client.get('/mechanic/quote-invoice?shop_id=bad').status_code, 404)
        response = self.client.get('/mechanic/quote-invoice')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Choose a shop above', response.get_data(as_text=True))
        self.assertNotIn('OUR-QUOTE', response.get_data(as_text=True))
        self.assertNotIn('Other Owner', response.get_data(as_text=True))

    def test_document_opening_validates_explicit_shop_and_uses_its_profile(self):
        import ast
        from types import SimpleNamespace
        from unittest.mock import Mock
        from flask_login import login_user
        from werkzeug.exceptions import NotFound
        node = next(node for node in ast.parse((base.ROOT / 'app/program_mechanic/routes.py').read_text(encoding='utf-8')).body
                    if isinstance(node, ast.FunctionDef) and node.name == 'job_card_detail')
        node.decorator_list = []
        code = ast.fix_missing_locations(base.ProductionImports().visit(ast.Module(body=[node], type_ignores=[])))
        communication = Mock()
        communication.query.filter_by.return_value.order_by.return_value.all.return_value = []
        bank = Mock()
        bank.query.filter_by.return_value.order_by.return_value.first.return_value = None
        self.ns.update(MechJobCard=SimpleNamespace(query=SimpleNamespace(get_or_404=lambda identifier: self.db_session.get(base.models.MechJobCard, identifier))),
                       MechCommunication=communication, BusinessBankAccount=bank,
                       render_template=lambda template, **values: values['shop'].id)
        exec(compile(code, 'production_document_open', 'exec'), self.ns)
        with self.app.test_request_context(f'/?shop_id={self.mechanic.id}'):
            login_user(self.db_session.get(base.models.User, 1))
            from flask import session
            session['active_trade'] = 'carpentry'
            self.assertEqual(self.ns['job_card_detail'](self.quote.id), self.mechanic.id)
        for shop in (self.second, self.foreign):
            with self.app.test_request_context(f'/?shop_id={shop.id}'):
                login_user(self.db_session.get(base.models.User, 1))
                with self.assertRaises(NotFound):
                    self.ns['job_card_detail'](self.quote.id)

    def test_legacy_records_remain_accessible_without_guessing_shop(self):
        response = self.page(self.mechanic)
        tree = html.fromstring(response.data)
        link = tree.xpath('//a[text()="existing mechanic register"]')
        self.assertEqual(len(link), 1)
        self.assertIsNone(self.unlinked.shop_id)
        self.assertNotIn('UNLINKED', tree.get_element_by_id('quote-invoice-register').text_content())


if __name__ == '__main__':
    unittest.main(verbosity=2)
