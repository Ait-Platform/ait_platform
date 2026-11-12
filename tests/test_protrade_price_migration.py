"""Focused PostgreSQL migration tests, no app startup or persistent writes."""
import importlib.util
from pathlib import Path
import unittest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from dotenv import dotenv_values
import sqlalchemy as sa

ROOT = Path(__file__).resolve().parents[1]


class PriceScopeMigrationTests(unittest.TestCase):
    def setUp(self):
        url = dotenv_values(ROOT / '.env')['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)
        parsed = sa.engine.make_url(url)
        self.assertEqual(parsed.database, 'ait_local_db')
        self.assertIn(parsed.host, ('localhost', '127.0.0.1', '::1'))
        self.engine = sa.create_engine(url)
        self.addCleanup(self.engine.dispose)
        self.conn = self.engine.connect()
        self.addCleanup(self.conn.close)
        tx = self.conn.begin()
        self.addCleanup(tx.rollback)
        self.assertEqual(self.conn.exec_driver_sql('SELECT current_database()').scalar(), 'ait_local_db')
        self.conn.exec_driver_sql('CREATE TEMP TABLE mech_shops (id integer PRIMARY KEY, user_id integer, trade varchar(50))')
        self.conn.exec_driver_sql('SET LOCAL search_path TO pg_temp')
        self.conn.exec_driver_sql('CREATE TABLE mech_catalog_parts (id integer PRIMARY KEY, user_id integer, part_name varchar(150), category varchar(50), default_price double precision, created_at timestamp)')
        self.conn.exec_driver_sql("INSERT INTO mech_shops VALUES (10,1,'mechanic'),(20,2,'mechanic')")
        self.conn.exec_driver_sql("INSERT INTO mech_catalog_parts VALUES (1,NULL,'Brake Pads','Brakes',450,'2026-01-01'),(2,1,'Brake Pads','Brakes',475,'2026-02-01'),(3,2,'Oil Filter','Engine',120,'2026-03-01')")
        spec = importlib.util.spec_from_file_location('protrade_price_migration', ROOT / 'migrations/versions/protrade_price_001.py')
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)
        self.migration.op = Operations(MigrationContext.configure(self.conn))
        self.original = self.snapshot()

    def snapshot(self):
        return self.conn.exec_driver_sql('SELECT id,user_id,part_name,category,default_price,created_at FROM mech_catalog_parts ORDER BY id').all()

    def columns(self):
        return {col['name']: col for col in sa.inspect(self.conn).get_columns('mech_catalog_parts')}

    def test_add_missing_columns_without_changing_records_or_legacy_scope(self):
        self.migration.upgrade()
        self.assertEqual(self.snapshot(), self.original)
        self.assertTrue(self.columns()['shop_id']['nullable'])
        self.assertTrue(self.columns()['trade']['nullable'])
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM mech_catalog_parts WHERE shop_id IS NULL AND trade IS NULL').scalar(), 3)
        self.assertEqual(self.conn.exec_driver_sql("SELECT default_price FROM mech_catalog_parts WHERE user_id=1 AND shop_id IS NULL AND trade IS NULL").scalar(), 475)
        fks = sa.inspect(self.conn).get_foreign_keys('mech_catalog_parts')
        self.assertTrue(any(fk['constrained_columns'] == ['shop_id'] and fk['referred_table'] == 'mech_shops' for fk in fks))

    def test_only_trade_missing_keeps_existing_shop_scope(self):
        self.conn.exec_driver_sql('ALTER TABLE mech_catalog_parts ADD COLUMN shop_id integer REFERENCES mech_shops(id)')
        self.conn.exec_driver_sql('UPDATE mech_catalog_parts SET shop_id=10 WHERE id=2')
        self.migration.upgrade()
        self.assertEqual(self.snapshot(), self.original)
        self.assertEqual(self.conn.exec_driver_sql('SELECT shop_id FROM mech_catalog_parts WHERE id=2').scalar(), 10)

    def test_only_shop_id_missing_preserves_existing_trade(self):
        self.conn.exec_driver_sql('ALTER TABLE mech_catalog_parts ADD COLUMN trade varchar(50)')
        self.conn.exec_driver_sql("UPDATE mech_catalog_parts SET trade='mechanic' WHERE id=2")
        self.migration.upgrade()
        self.assertEqual(self.snapshot(), self.original)
        self.assertEqual(self.conn.exec_driver_sql('SELECT trade FROM mech_catalog_parts WHERE id=2').scalar(), 'mechanic')

    def test_manual_sql_or_startup_upgrade_and_rerun_are_safe(self):
        self.migration.upgrade()
        self.conn.exec_driver_sql("UPDATE mech_catalog_parts SET shop_id=10, trade='mechanic' WHERE id=2")
        before = self.conn.exec_driver_sql('SELECT * FROM mech_catalog_parts ORDER BY id').all()
        self.migration.upgrade()
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM mech_catalog_parts ORDER BY id').all(), before)

    def test_missing_required_table_fails_before_any_ddl(self):
        self.conn.exec_driver_sql('DROP TABLE mech_shops')
        with self.assertRaisesRegex(RuntimeError, 'Required existing table missing'):
            self.migration.upgrade()
        self.assertNotIn('shop_id', self.columns())
        self.assertNotIn('trade', self.columns())

    def test_downgrade_does_not_remove_data(self):
        self.migration.upgrade()
        with self.assertRaisesRegex(RuntimeError, 'Destructive downgrade disabled'):
            self.migration.downgrade()
        self.assertEqual(self.snapshot(), self.original)
        self.assertIn('shop_id', self.columns())


if __name__ == '__main__':
    unittest.main(verbosity=2)
