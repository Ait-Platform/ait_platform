"""Dedicated BOQ entry points reuse existing shop-scoped quotation structures."""
import importlib.util
from unittest.mock import patch
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

    def fixtures(self):
        payments.PaymentMethodTests.fixtures(self)
        # Production revision 012 includes migration 011's all-trade functions.
        self.conn.exec_driver_sql('ALTER TABLE debtor DROP COLUMN identity_debtor_id CASCADE')
        identity_spec = importlib.util.spec_from_file_location('identity_migration', base.ROOT / 'migrations/versions/protrade_debtor_identity_011.py')
        identity = importlib.util.module_from_spec(identity_spec)
        identity_spec.loader.exec_module(identity)
        identity.op = Operations(MigrationContext.configure(self.conn))
        identity.upgrade()
        self.db_session.expire_all()
        spec = importlib.util.spec_from_file_location('boq_trade_migration', base.ROOT / 'migrations/versions/protrade_boq_trade_014.py')
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        migration.op = Operations(MigrationContext.configure(self.conn))
        migration.upgrade()
        self.second.trade = 'boq'
        self.db_session.commit()
        import app.protrade_sections as sections
        import app.protrade_output as output
        for module in (sections, output):
            for name, value in dict(self.ns, ProTradeDocument=base.ProTradeDocument).items():
                if name == 'db' or name.startswith(('Mech','ProTrade','Debtor')):
                    setattr(module, name, value)
        shop = self.shop(name='BOQ Business', trade='boq')
        self.save_debtor(shop, name='BOQ Client')
        debtor = self.debtor(shop)
        return shop, debtor

    def test_boq_price_quote_ledger_and_isolation(self):
        shop, debtor = self.fixtures()
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
        wallet = base.models.AitTokenWallet.query.filter_by(user_id=1).one()
        balance = wallet.balance
        self.assertEqual(base.ProTradeDocument.query.filter_by(shop_id=shop.id).count(), 0)
        section_url = f'/mechanic/shop/{shop.id}/boq/{quote.id}/sections'
        self.assertEqual(self.client.post(section_url, data={'action':'save','name':'Building Works','item_id':[str(item.id)]}).status_code, 302)
        self.assertEqual(wallet.balance, balance)
        section = quote.boq_sections[0]
        with patch('app.protrade_output.pdf_bytes', return_value=b'%PDF-test'):
            for _ in range(3):
                self.assertEqual(self.client.post(section_url, data={'action':'generate','section_id':section['id']}).status_code, 302)
            self.assertEqual(wallet.balance, balance - 25)
            self.assertEqual(self.client.get(section_url, query_string={'section_id':section['id'],'download':'1'}).status_code, 200)
        receipt = base.ProTradeDocument.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(receipt.tokens, 25)
        corrected = {'action':'save','section_id':section['id'],'name':'Building and Repairs','item_id':[str(item.id)]}
        self.assertEqual(self.client.post(section_url,data=corrected).status_code,302)
        self.assertEqual(wallet.balance,balance-25)
        self.assertEqual(quote.boq_sections[0]['id'],section['id'])
        self.assertEqual(quote.boq_sections[0]['name'],'Building and Repairs')
        self.assertEqual(receipt.snapshot['section']['name'],'Building Works')
        self.assertEqual(receipt.snapshot['commercial_terms'], dict(currency='ZAR', contractor_fee_cents=5000, operator_retains_cents=2500, ait_wallet_value_cents=2500, tokens=25, contractor_payment_recipient='operator', ait_collection='prepaid_wallet_deduction'))
        self.assertIn('contractor pays you R50.00 directly', self.client.get(section_url).get_data(as_text=True))
        with self.app.test_request_context():
            from flask_login import login_user
            login_user(self.db_session.get(base.models.User, 1))
            from app.protrade_sections import require_paid_sections
            require_paid_sections(quote)

        charge = base.DebtorLedger.query.filter_by(boq_quote_id=quote.id).one()
        self.assertEqual(charge.amount, int(quote.grand_total * 100))
        self.assertIn('Boq quotation', charge.description)
        self.assertIn('BOQ Quotation', self.client.get(response.location).get_data(as_text=True))
        self.assertEqual(self.client.get('/mechanic/catalog', query_string=dict(shop_id=self.second.id, edit_id=item.id)).status_code, 404)
        with self.client.session_transaction() as session:
            session['_user_id'] = '2'
        self.assertEqual(self.client.get(response.location).status_code, 404)



    def test_four_sections_cost_100_and_failures_preserve_funds(self):
        shop, debtor = self.fixtures()
        wallet = base.models.AitTokenWallet.query.filter_by(user_id=1).one()
        wallet.balance = 100
        items = []
        for index in range(4):
            item = base.ProTradeBoqItem(user_id=1, shop_id=shop.id, trade='boq',
                code=f'BOQ-{index}', description=f'Work {index}', unit='Each',
                material_rate=20, labour_rate=30)
            self.db_session.add(item)
            items.append(item)
        self.db_session.commit()
        data = {'area':'new','quote_key':uuid4().hex,'item_id[]':[str(item.id) for item in items]}
        data.update({f'quantity_{item.id}':'1' for item in items})
        self.assertEqual(self.client.post(self.quote_url(debtor), data=data).status_code, 302)
        quote = base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(wallet.balance, 100)
        url = f'/mechanic/shop/{shop.id}/boq/{quote.id}/sections'
        for name, item in zip(('Plumbing','Electrical','Building Works','Painting'), items):
            self.assertEqual(self.client.post(url, data={'action':'save','name':name,'item_id':[str(item.id)]}).status_code, 302)
        sections = quote.boq_sections
        first = {'action':'generate','section_id':sections[0]['id']}
        with patch('app.protrade_output.pdf_bytes', side_effect=RuntimeError('renderer unavailable')):
            self.assertEqual(self.client.post(url, data=first).status_code, 503)
        self.assertEqual(wallet.balance, 100)
        self.assertEqual(base.ProTradeDocument.query.filter_by(shop_id=shop.id).count(), 0)
        wallet.balance = 24
        self.db_session.commit()
        with patch('app.protrade_output.pdf_bytes', return_value=b'%PDF-test'):
            self.assertEqual(self.client.post(url, data=first).status_code, 402)
        self.assertEqual(wallet.balance, 24)
        wallet.balance = 100
        self.db_session.commit()
        with patch('app.protrade_output.pdf_bytes', return_value=b'%PDF-test'):
            for section in sections:
                for _ in range(2):
                    self.assertEqual(self.client.post(url, data={'action':'generate','section_id':section['id']}).status_code, 302)
        self.assertEqual(wallet.balance, 0)
        self.assertEqual(base.ProTradeDocument.query.filter_by(shop_id=shop.id).count(), 4)
        self.assertEqual(sum(row.amount for row in base.TokenTransaction.query.all()), -100)
        for section in sections:
            with patch('app.protrade_output.pdf_bytes', return_value=b'%PDF-test'):
                self.assertEqual(self.client.get(url, query_string={'section_id':section['id'],'download':'1'}).status_code, 200)
        with self.app.test_request_context():
            from flask_login import login_user
            import app.protrade_sections as section_helpers
            login_user(self.db_session.get(base.models.User, 1))
            section_helpers.require_paid_sections(quote)
        self.assertEqual(wallet.balance, 0)

if __name__ == '__main__':
    unittest.main()
