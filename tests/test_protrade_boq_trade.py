"""Dedicated BOQ entry points reuse existing shop-scoped quotation structures."""
import importlib.util
import unittest
from uuid import uuid4
from alembic.migration import MigrationContext
from alembic.operations import Operations
import test_protrade_payment_methods as payments
import test_protrade_shop_register as base


class BoqTradeTests(unittest.TestCase):
    setUp = payments.PaymentMethodTests.setUp
    shop = payments.PaymentMethodTests.shop
    save_debtor = payments.PaymentMethodTests.save_debtor
    debtor = payments.PaymentMethodTests.debtor
    save_quote = payments.PaymentMethodTests.save_quote
    quote_url = payments.PaymentMethodTests.quote_url
    ledger_url = payments.PaymentMethodTests.ledger_url

    def test_boq_price_quote_ledger_and_isolation(self):
        payments.PaymentMethodTests.fixtures(self)
        spec = importlib.util.spec_from_file_location('boq_trade_migration', base.ROOT / 'migrations/versions/protrade_boq_trade_014.py')
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        migration.op = Operations(MigrationContext.configure(self.conn))
        migration.upgrade()
        self.second.trade = 'boq'
        self.db_session.commit()
        shop = self.shop(name='BOQ Business', trade='boq')
        self.save_debtor(shop, name='BOQ Client')
        debtor = self.debtor(shop)
        self.assertIn(('boq', 'Bill of Quantities'), self.ns['AVAILABLE_TRADES'])
        response = self.client.post('/mechanic/catalog', data=dict(shop_id=shop.id, action='save_boq', description='Excavation', unit='m', material_rate='20', labour_rate='30', quantity='1'))
        self.assertEqual(response.status_code, 302)
        item = base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).one()
        self.assertTrue(item.code.startswith('BOQ-C-'))
        self.assertEqual(item.trade, 'boq')
        response = self.client.post(self.quote_url(debtor), data={'area': 'new', 'quote_key': uuid4().hex, 'item_id[]': [str(item.id)], f'quantity_{item.id}': '2'})
        self.assertEqual(response.status_code, 302)
        quote = base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(quote.trade, 'boq')
        charge = base.DebtorLedger.query.filter_by(boq_quote_id=quote.id).one()
        self.assertEqual(charge.amount, int(quote.grand_total * 100))
        self.assertIn('BOQ quotation', charge.description)
        self.assertIn('BOQ Quotation', self.client.get(response.location).get_data(as_text=True))
        self.assertEqual(self.client.get('/mechanic/catalog', query_string=dict(shop_id=self.second.id, edit_id=item.id)).status_code, 404)
        with self.client.session_transaction() as session:
            session['_user_id'] = '2'
        self.assertEqual(self.client.get(response.location).status_code, 404)


if __name__ == '__main__':
    unittest.main()
