"""Real PostgreSQL preparation leases, real BOQ routes, no app startup hooks."""
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from unittest.mock import patch
import importlib.util
import unittest

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
import test_protrade_shop_register as base
import test_protrade_boq_trade as boq_tests
import app.protrade_boq_time as timing


class BoqPreparationTests(unittest.TestCase):
    setUp = boq_tests.BoqTradeTests.setUp
    shop = boq_tests.BoqTradeTests.shop
    save_debtor = boq_tests.BoqTradeTests.save_debtor
    debtor = boq_tests.BoqTradeTests.debtor
    save_quote = boq_tests.BoqTradeTests.save_quote
    quote_url = boq_tests.BoqTradeTests.quote_url
    ledger_url = boq_tests.BoqTradeTests.ledger_url
    fixtures = boq_tests.BoqTradeTests.fixtures

    def prepare(self):
        self.boq, self.account = self.fixtures()
        self.url = f'/mechanic/shop/{self.boq.id}/debtor/{self.account.id}/boq-preparation'
        self.key, self.tab = uuid4().hex, uuid4().hex
        self.now = datetime(2026, 11, 15, 9)
        import json
        from lxml import html
        page = self.client.get(self.quote_url(self.account), query_string={'area':'new'})
        choices = json.loads(html.fromstring(page.data).get_element_by_id('quote-builder-data').text)['choices']
        self.work_id = str(choices[0]['source_id'])

    def preparation_quote(self):
        return self.client.post(self.quote_url(self.account), data={
            'area':'new', 'quote_key':self.key, 'item_id[]':[self.work_id],
            f'quantity_{self.work_id}':'1', f'material_rate_{self.work_id}':'20',
            f'labour_rate_{self.work_id}':'30'})

    def beat(self, second, tick, action='active', tab=None, key=None, url=None):
        with patch.object(timing, 'datetime') as clock:
            clock.utcnow.return_value = self.now + timedelta(seconds=second)
            return self.client.post(url or self.url, data=dict(quote_key=key or self.key,
                tab_key=tab or self.tab, tick=tick, action=action))

    def row(self):
        return base.ProTradeBoqPreparation.query.filter_by(quote_key=self.key).one()

    def test_idle_resume_refresh_duplicates_and_interruption(self):
        self.prepare()
        self.assertEqual(self.beat(0, 1).status_code, 204)
        self.assertEqual(self.beat(15, 2).status_code, 204)
        self.beat(16, 2)  # replay
        self.beat(17, 1, tab=uuid4().hex)  # concurrent tab
        self.assertEqual(self.row().total_seconds, Decimal(15))
        self.beat(20, 3, 'pause')
        self.beat(30, 2)  # out-of-order request cannot resume paused lease
        self.assertIsNone(self.row().last_seen_at)
        self.beat(600, 4)
        self.beat(615, 5)
        self.assertEqual(self.row().total_seconds, Decimal(35))
        # A lost browser/network connection cannot count unattended hours.
        self.beat(3600, 6)
        self.assertEqual(self.row().total_seconds, Decimal(35))
        self.beat(3605, 7, 'pause')
        self.tab = uuid4().hex  # refresh/reopened session
        self.beat(3610, 1)
        self.beat(3625, 2)
        self.assertEqual(self.row().total_seconds, Decimal(55))
        self.assertEqual(base.ProTradeBoqPreparation.query.count(), 1)
        self.assertEqual(base.ProTradeDocument.query.count(), 0)

    def test_saved_attachment_completion_and_admin_report(self):
        self.prepare()
        self.beat(0, 1); self.beat(15, 2)
        wallet = base.models.AitTokenWallet.query.filter_by(user_id=1).one()
        balance = wallet.balance
        response = self.preparation_quote()
        self.assertEqual(response.status_code, 302)
        quote = base.ProTradeBoqQuote.query.filter_by(shop_id=self.boq.id).one()
        self.assertEqual(self.row().quote_id, quote.id)
        self.assertEqual(wallet.balance, balance)
        self.assertEqual(self.client.get(response.location).status_code, 200)
        # Existing New Quote retires the previous draft and freezes timing.
        with patch.object(timing, 'datetime') as clock:
            clock.utcnow.return_value = self.now + timedelta(seconds=20)
            response = self.client.post(f'/mechanic/shop/{self.boq.id}/debtor/{self.account.id}/quotes/new')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.row().total_seconds, Decimal(20))
        self.assertIsNotNone(self.row().completed_at)
        self.beat(25, 3); self.beat(30, 4)
        self.assertEqual(self.row().total_seconds, Decimal(20))
        report_url = '/mechanic/admin/boq-preparation'
        self.assertEqual(self.client.get(report_url).status_code, 403)
        with self.client.session_transaction() as session:
            session['is_admin'] = True
        response = self.client.get(report_url)
        self.assertEqual(response.status_code, 200)
        for value in [quote.display_quote_number, self.boq.business_name, quote.client_name, 'Owner', '00:00:20']:
            self.assertIn(value, response.get_data(as_text=True))
        self.assertEqual(wallet.balance, balance)

    def test_ownership_trade_scope_and_validation(self):
        self.prepare()
        self.beat(0, 1)
        wrong_shop = f'/mechanic/shop/{self.carpentry.id}/debtor/{self.account.id}/boq-preparation'
        self.assertEqual(self.beat(15, 2, url=wrong_shop).status_code, 404)
        with self.client.session_transaction() as session:
            session['_user_id'] = '2'
        self.assertEqual(self.beat(15, 2).status_code, 404)
        self.assertEqual(self.client.get('/mechanic/admin/boq-preparation').status_code, 403)
        with self.client.session_transaction() as session:
            session['_user_id'] = '1'
        self.assertEqual(self.beat(15, -1).status_code, 400)
        self.assertEqual(self.beat(15, 2, action='complete').status_code, 400)
        self.assertEqual(self.row().total_seconds, 0)
        # Even another owned BOQ business cannot use this account's preparation key.
        other = self.shop(name='Other BOQ', trade='boq')
        self.save_debtor(other, name='Separate Client')
        debtor = self.debtor(other)
        other_url = f'/mechanic/shop/{other.id}/debtor/{debtor.id}/boq-preparation'
        self.assertEqual(self.beat(15, 2, url=other_url).status_code, 404)
        self.assertEqual(base.ProTradeBoqPreparation.query.count(), 1)

    def test_migration_additive_and_unique_constraints(self):
        self.prepare()
        self.conn.exec_driver_sql('DROP TABLE protrade_boq_preparation')
        spec = importlib.util.spec_from_file_location('time_migration', base.ROOT/'migrations/versions/protrade_boq_time_016.py')
        migration = importlib.util.module_from_spec(spec); spec.loader.exec_module(migration)
        self.assertEqual(migration.down_revision, 'protrade_work_activity_015')
        migration.op = Operations(MigrationContext.configure(self.conn))
        # Fixture's auth table is named users; production is user.
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user" (id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES (1),(2)')
        migration.upgrade()
        self.beat(0, 1); self.beat(15, 2)
        self.assertEqual(self.row().total_seconds, 15)
        with self.assertRaises(sa.exc.IntegrityError):
            with self.conn.begin_nested():
                self.conn.execute(sa.text('INSERT INTO protrade_boq_preparation(user_id,shop_id,debtor_id,quote_key,started_at) VALUES(1,:shop,:debtor,:key,now())'),
                    dict(shop=self.boq.id, debtor=self.account.id, key=self.key))
        with self.assertRaisesRegex(RuntimeError, 'Preserve'):
            migration.downgrade()

    def test_csrf_protection_and_owned_token_refresh(self):
        from flask_wtf import CSRFProtect
        self.app.config['WTF_CSRF_ENABLED'] = False
        CSRFProtect(self.app)
        self.prepare()
        self.app.config['WTF_CSRF_ENABLED'] = True
        self.assertEqual(self.beat(0, 1).status_code, 400)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(base.ProTradeBoqPreparation.query.count(), 0)
        token = response.json['csrf']
        response = self.client.post(self.url, data=dict(quote_key=self.key, tab_key=self.tab,
            tick=1, action='active', csrf_token=token))
        self.assertEqual(response.status_code, 204)
        self.assertEqual(base.ProTradeBoqPreparation.query.count(), 1)
        with self.client.session_transaction() as session:
            session['_user_id'] = '2'
        self.assertEqual(self.client.get(self.url).status_code, 404)

    def test_full_boq_output_stops_time_only_after_successful_generation(self):
        from lxml import html
        import app.protrade_output as output
        self.prepare()
        self.beat(0, 1); self.beat(15, 2)
        response = self.preparation_quote()
        self.assertEqual(response.status_code, 302)
        quote = base.ProTradeBoqQuote.query.filter_by(shop_id=self.boq.id).one()
        section_url = f'/mechanic/shop/{self.boq.id}/boq/{quote.id}/sections'
        self.assertEqual(self.client.get(section_url).status_code, 200)
        preview = self.client.get('/mechanic/output', query_string=dict(shop_id=self.boq.id, selection=f'quote:boq-{quote.id}', preview=1))
        self.assertEqual(preview.status_code, 200)
        fields = {node.get('name'):node.get('value','') for node in html.fromstring(preview.data).xpath('//form[@id="output-email-form"]/input')}
        with patch.object(output, 'pdf_bytes', return_value=b'%PDF-test'):
            # Unpaid sections cannot generate a final BOQ or complete its timer.
            self.assertEqual(self.client.get('/mechanic/output', query_string=dict(fields, format='pdf')).status_code, 409)
        self.assertIsNone(self.row().completed_at)
        self.assertEqual(self.client.post(section_url, data={'action':'save','name':'Work','item_id':[self.work_id]}).status_code, 302)
        with patch.object(output, 'pdf_bytes', return_value=b'%PDF-test'):
            self.assertEqual(self.client.post(section_url, data={'action':'generate','section_id':quote.boq_sections[0]['id']}).status_code, 302)
        self.assertIsNone(self.row().completed_at)  # One paid section isn't full finalisation.
        with patch.object(output, 'pdf_bytes', side_effect=RuntimeError('offline')):
            self.assertEqual(self.client.get('/mechanic/output', query_string=dict(fields, format='pdf')).status_code, 503)
        self.assertIsNone(self.row().completed_at)
        ledger_before = [(r.id, r.amount) for r in base.DebtorLedger.query.all()]
        wallet = base.models.AitTokenWallet.query.filter_by(user_id=1).one()
        balance = wallet.balance
        with patch.object(timing, 'datetime') as clock, patch.object(output, 'pdf_bytes', return_value=b'%PDF-test'):
            clock.utcnow.return_value = self.now + timedelta(seconds=20)
            self.assertEqual(self.client.get('/mechanic/output', query_string=dict(fields, format='pdf')).status_code, 200)
        self.assertEqual(self.row().total_seconds, 20)
        completed = self.row().completed_at
        self.beat(30, 3)
        with patch.object(output, 'pdf_bytes', return_value=b'%PDF-test'):
            self.assertEqual(self.client.get('/mechanic/output', query_string=dict(fields, format='pdf')).status_code, 200)
        self.assertEqual(self.row().completed_at, completed)
        self.assertEqual(self.row().total_seconds, 20)
        self.assertEqual(wallet.balance, balance)
        self.assertEqual([(r.id, r.amount) for r in base.DebtorLedger.query.all()], ledger_before)


if __name__ == '__main__':
    unittest.main()
