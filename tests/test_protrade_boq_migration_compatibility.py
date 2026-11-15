"""Migration 014 must accept the actual ledger functions installed by 011/012."""
from decimal import Decimal
import importlib.util
import unittest
from unittest.mock import patch
from uuid import uuid4
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
import test_protrade_shop_register as base
import test_protrade_payment_methods as payments
import test_protrade_mechanic_quotes as mechanic


def migration(connection, filename):
    spec = importlib.util.spec_from_file_location(filename, base.ROOT/'migrations/versions'/filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


class BoqMigrationCompatibilityTests(unittest.TestCase):
    setUp = mechanic.MechanicQuotationTests.setUp
    shop = mechanic.MechanicQuotationTests.shop
    save_debtor = mechanic.MechanicQuotationTests.save_debtor
    debtor = mechanic.MechanicQuotationTests.debtor
    save_quote = mechanic.MechanicQuotationTests.save_quote
    quote_url = mechanic.MechanicQuotationTests.quote_url
    ledger_url = mechanic.MechanicQuotationTests.ledger_url

    def fixtures(self):
        original = payments.PaymentMethodTests.fixtures

        def include_identity(case, *args, **kwargs):
            original(case, *args, **kwargs)
            case.conn.exec_driver_sql('ALTER TABLE debtor DROP COLUMN identity_debtor_id CASCADE')
            migration(case.conn, 'protrade_debtor_identity_011.py').upgrade()
            case.db_session.expire_all()

        with patch.object(payments.PaymentMethodTests, 'fixtures', include_identity):
            mechanic.MechanicQuotationTests.fixtures(self, legacy=False)
        self.upgrade = migration(self.conn, 'protrade_boq_trade_014.py')

    def definitions(self):
        return dict(self.conn.execute(sa.text(
            "SELECT p.proname,pg_get_functiondef(p.oid) FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() "
            "AND p.proname IN ('protrade_sync_quote_debit','protrade_guard_quote_debit') AND p.pronargs=0"
        )).all())

    def test_full_011_012_chain_accepts_existing_functions_without_financial_rewrite(self):
        self.fixtures()
        functions = self.definitions()
        self.assertIn("NEW.trade='mechanic'", functions['protrade_sync_quote_debit'])
        self.assertIn("initcap(replace(NEW.trade,'_',' '))||' quotation '||reference", functions['protrade_sync_quote_debit'])
        self.assertIn("q.trade<>'mechanic'", functions['protrade_guard_quote_debit'])
        tables = ('debtor','debtor_ledger','mech_shops','mech_clients','mech_vehicles','mech_job_cards',
            'mech_part_lines','mech_labor_lines','mech_invoices','protrade_boq_items','protrade_boq_quotes',
            'ait_token_wallets','ait_token_transaction')
        before = {table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in tables}
        self.upgrade.upgrade()
        self.assertEqual(self.definitions(),functions)
        for table, records in before.items():
            self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all(),records,table)

    def test_boq_and_other_trades_keep_one_owned_debit_and_immutable_identity(self):
        self.fixtures()
        self.upgrade.upgrade()
        wallet_before = self.wallet.balance
        for trade in ('boq','plumbing'):
            shop = self.shop(name=trade+' business',trade=trade,status='active')
            self.save_debtor(shop,name=trade+' customer')
            debtor = self.debtor(shop)
            quote = base.ProTradeBoqQuote(user_id=1,shop_id=shop.id,debtor_id=debtor.id,
                client_id=debtor.reference_id,trade=trade,quotation_seq=1,quote_number=uuid4().hex,
                client_name=debtor.name,items=[],grand_total=Decimal('250'))
            self.db_session.add(quote)
            self.db_session.commit()
            debit = base.DebtorLedger.query.filter_by(boq_quote_id=quote.id).one()
            original = (debit.id,debit.txn_date,quote.quotation_seq,quote.quote_number)
            quote.grand_total = Decimal('300')
            self.db_session.commit()
            self.db_session.refresh(debit)
            self.assertEqual((debit.id,debit.txn_date,quote.quotation_seq,quote.quote_number),original)
            self.assertEqual(debit.amount,30000)
            self.assertEqual(base.DebtorLedger.query.filter_by(boq_quote_id=quote.id).count(),1)
            for sql in (
                'UPDATE debtor_ledger SET amount=1 WHERE id='+str(debit.id),
                'DELETE FROM debtor_ledger WHERE id='+str(debit.id),
                'UPDATE protrade_boq_quotes SET debtor_id='+str(self.other.id)+' WHERE id='+str(quote.id),
                'UPDATE protrade_boq_quotes SET user_id=2 WHERE id='+str(quote.id),
            ):
                with self.conn.begin_nested() as transaction:
                    with self.assertRaises(sa.exc.DBAPIError):
                        self.conn.exec_driver_sql(sql)
                    transaction.rollback()
        self.assertEqual(self.wallet.balance,wallet_before)


    def test_missing_or_weakened_financial_guards_are_rejected(self):
        self.fixtures()
        functions = self.definitions()
        changes = (
            ('protrade_sync_quote_debit', 'd.shop_id=NEW.shop_id', 'TRUE'),
            ('protrade_sync_quote_debit', 'NEW.user_id IS DISTINCT FROM OLD.user_id', 'FALSE'),
            ('protrade_sync_quote_debit', 'amount=excluded.amount', 'amount=0'),
            ('protrade_sync_quote_debit', "NEW.trade='mechanic'", "NEW.trade<>'carpentry'"),
            ('protrade_guard_quote_debit', "RAISE EXCEPTION 'Quotation ledger history cannot be deleted'", 'NULL'),
            ('protrade_guard_quote_debit', 'NEW.boq_quote_id IS DISTINCT FROM OLD.boq_quote_id', 'FALSE'),
            ('protrade_guard_quote_debit', 'd.shop_id=q.shop_id', 'TRUE'),
            ('protrade_guard_quote_debit', 'NEW.amount=round(q.grand_total*100)', 'NEW.amount>=0'),
            ('protrade_guard_quote_debit', "q.trade<>'mechanic'", "q.trade='carpentry'"),
        )
        for name, old, new in changes:
            with self.subTest(function=name, altered=old):
                with self.conn.begin_nested() as transaction:
                    changed = functions[name].replace(old,new)
                    self.assertNotEqual(changed,functions[name])
                    self.conn.execute(sa.text(changed))
                    with self.assertRaisesRegex(RuntimeError,'Unexpected quotation ledger function definition for '+name):
                        self.upgrade.upgrade()
                    self.assertEqual(self.definitions()[name],changed)
                    transaction.rollback()
        self.assertEqual(self.definitions(),functions)

    def test_combined_alembic_012_to_017_upgrade_preserves_real_011_012_data(self):
        self.fixtures()
        functions = self.definitions()
        self.conn.exec_driver_sql('DROP TABLE IF EXISTS protrade_work_activity')
        self.conn.exec_driver_sql('DROP TABLE protrade_boq_preparation')
        self.conn.exec_driver_sql('DROP TABLE protrade_pricing_revision')
        self.conn.exec_driver_sql('DROP TABLE protrade_document')
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_quotes DROP COLUMN boq_sections')
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES(1),(2)')
        self.conn.exec_driver_sql('CREATE TEMP TABLE alembic_version(version_num varchar(32) PRIMARY KEY)')
        self.conn.exec_driver_sql("INSERT INTO alembic_version VALUES('protrade_mechanic_quotes_012')")
        tables = ('debtor','debtor_ledger','mech_shops','mech_clients','mech_vehicles','mech_job_cards',
            'mech_part_lines','mech_labor_lines','mech_invoices','protrade_boq_items','protrade_boq_quotes',
            'ait_token_wallets','ait_token_transaction')
        columns = {table:[column['name'] for column in sa.inspect(self.conn).get_columns(table)] for table in tables}
        before = {table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in tables}
        config = Config(str(base.ROOT/'migrations/alembic.ini'))
        config.set_main_option('script_location',str(base.ROOT/'migrations'))
        config.attributes['connection'] = self.conn
        command.upgrade(config,'protrade_pricing_017')
        self.assertEqual(self.conn.exec_driver_sql('SELECT version_num FROM alembic_version').scalar_one(),'protrade_pricing_017')
        self.assertEqual(self.definitions(),functions)
        for table, records in before.items():
            selected = ','.join(self.conn.dialect.identifier_preparer.quote(name) for name in columns[table])
            self.assertEqual(self.conn.exec_driver_sql('SELECT '+selected+' FROM '+table+' ORDER BY id').all(),records,table)
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM protrade_document').scalar_one(),0)
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM protrade_work_activity').scalar_one(),0)
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM protrade_boq_preparation').scalar_one(),0)
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM protrade_pricing_revision').scalar_one(),1)
        # A second targeted upgrade is a no-op after the successful revision update.
        command.upgrade(config,'protrade_pricing_017')
        self.assertEqual(self.definitions(),functions)


if __name__ == '__main__':
    unittest.main()
