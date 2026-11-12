"""Carpentry BOQ routes, real model and Alembic migration on temporary PostgreSQL."""
from decimal import Decimal
import importlib.util
import unittest
from lxml import html
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
from test_protrade_shop_register import ShopRegisterTests, ProTradeBoqItem, MechCatalogPart, ROOT


class CarpentryBoqTests(unittest.TestCase):
    shop = ShopRegisterTests.shop

    def setUp(self):
        ShopRegisterTests.setUp(self)
        self.carpentry = self.shop(name='Happy Carpentry', trade='carpentry', status='active')
        self.second = self.shop(name='Second Carpentry', trade='carpentry', status='active')
        self.other = self.shop(owner=2, name='Other Owner Carpentry', trade='carpentry', status='active')
        self.mechanic = self.shop(name='Mechanic', trade='mechanic', status='active')
        self.db_session.add_all([MechCatalogPart(part_name='Brake Pads', default_price=450),
                                 MechCatalogPart(user_id=1, part_name='Brake Pads', default_price=475)])
        self.db_session.commit()
        self.original = self.snapshot()
        # Use the actual production revision, not just ORM create_all.
        self.conn.exec_driver_sql('DROP TABLE protrade_boq_items')
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user" (id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES (1),(2)')
        spec = importlib.util.spec_from_file_location('boq_migration', ROOT / 'migrations/versions/protrade_boq_002.py')
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)
        self.migration.op = Operations(MigrationContext.configure(self.conn))
        self.migration.upgrade()
        spec = importlib.util.spec_from_file_location('boq_unit_migration', ROOT / 'migrations/versions/protrade_boq_003.py')
        self.unit_migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.unit_migration)
        self.unit_migration.op = Operations(MigrationContext.configure(self.conn))
        self.unit_migration.upgrade()
        spec = importlib.util.spec_from_file_location('boq_quantity_migration', ROOT / 'migrations/versions/protrade_boq_004.py')
        self.quantity_migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.quantity_migration)
        self.quantity_migration.op = Operations(MigrationContext.configure(self.conn))
        self.quantity_migration.upgrade()

    def snapshot(self):
        return self.conn.exec_driver_sql('SELECT * FROM mech_catalog_parts ORDER BY id').all()

    def save(self, shop=None, expected=302, **changes):
        shop = shop or self.carpentry
        data = dict(shop_id=shop.id, action='save_boq', code='CAR-001', description='Install wooden door',
                    unit='Each', material_rate='1200.00', labour_rate='650.00')
        data.update(changes)
        result = self.client.post('/mechanic/catalog', data=data)
        self.assertEqual(result.status_code, expected, result.get_data(as_text=True)[:300])
        return result

    def page(self, shop=None, **query):
        return self.client.get('/mechanic/catalog', query_string=dict(shop_id=(shop or self.carpentry).id, **query))

    def test_add_view_edit_and_database_calculated_total(self):
        self.save(total_rate='0', trade='mechanic', user_id=2)
        item = ProTradeBoqItem.query.one()
        self.assertEqual((item.user_id, item.shop_id, item.trade), (1, self.carpentry.id, 'carpentry'))
        self.assertEqual(item.total_rate, Decimal('1850.00'))
        page = self.page().get_data(as_text=True)
        self.assertIn('Carpentry BOQ', page)
        self.assertIn('Install wooden door', page)
        self.assertIn('1850.00', page)
        self.assertNotIn('Brake Pads', page)
        edit = self.page(edit_id=item.id)
        tree = html.fromstring(edit.data)
        self.assertEqual(tree.xpath('//input[@name="item_id"]/@value'), [str(item.id)])
        self.assertEqual(tree.xpath('//input[@name="material_rate"]/@value'), ['1200.00'])
        self.save(item_id=item.id, code='CAR-002', unit='m\u00b2', material_rate='0.10', labour_rate='0.20')
        self.db_session.refresh(item)
        self.assertEqual(item.total_rate, Decimal('0.30'))
        self.assertTrue(item.code.startswith('CAR-C-'))
        self.assertEqual(ProTradeBoqItem.query.count(), 1)
        self.assertEqual(self.snapshot(), self.original)

    def test_duplicate_codes_do_not_overwrite_and_are_unique_per_shop(self):
        self.save(description='Supply and install wooden door')
        self.save(description='supply and install wooden door', expected=409)
        self.assertEqual(ProTradeBoqItem.query.count(), 1)
        self.assertEqual(ProTradeBoqItem.query.one().description, 'Supply and install wooden door')
        self.save(self.second)
        self.assertEqual(ProTradeBoqItem.query.count(), 2)
        self.save(description='Install door frame')
        item = ProTradeBoqItem.query.filter_by(shop_id=self.carpentry.id, code='CAR-002').one()
        self.save(item_id=item.id, preset_code='CAR-001', expected=409)
        self.assertEqual(item.code, 'CAR-002')

    def test_shop_owner_trade_isolation_for_view_and_edit(self):
        self.save()
        item = ProTradeBoqItem.query.one()
        self.assertNotIn('Install wooden door', self.page(self.second).get_data(as_text=True))
        self.assertEqual(self.page(self.second, edit_id=item.id).status_code, 404)
        self.save(self.second, item_id=item.id, expected=404)
        self.assertEqual(self.page(self.other).status_code, 404)
        self.save(self.other, expected=404)
        # Even a BOQ row for the same shop with a different trade is excluded.
        self.db_session.add(ProTradeBoqItem(user_id=1, shop_id=self.carpentry.id, trade='plumbing', code='HIDDEN',
                                           description='Hidden trade', unit='m', material_rate=1, labour_rate=2))
        self.db_session.commit()
        self.assertNotIn('Hidden trade', self.page().get_data(as_text=True))
        self.assertEqual(item.description, 'Install wooden door')
        self.assertEqual(self.page(edit_id='bad').status_code, 404)

    def test_validation_rejects_invalid_rates_units_and_empty_fields(self):
        for changes in [dict(material_rate='-1'), dict(labour_rate='NaN'), dict(material_rate='Infinity'),
                        dict(labour_rate='0.001'), dict(material_rate='not-a-number'), dict(material_rate=''),
                        dict(material_rate='1000000000000'), dict(unit='kg'), dict(preset_code='UNKNOWN'),
                        dict(description=' '), dict(description='x' * 4001)]:
            with self.subTest(changes=changes):
                response = self.save(expected=400, **changes)
                self.assertIn('role="alert"', response.get_data(as_text=True))
        self.assertEqual(ProTradeBoqItem.query.count(), 0)
        for index, unit in enumerate(('Each', 'm', 'm\u00b2', 'm\u00b3')):
            self.save(code=f'CAR-{index}', unit=unit, material_rate='0', labour_rate='0')
        self.assertEqual(ProTradeBoqItem.query.count(), 4)

    def test_mechanic_pricing_unchanged_and_boq_actions_unavailable(self):
        self.save()
        page = self.page(self.mechanic).get_data(as_text=True)
        self.assertIn('Manage Price List', page)
        self.assertIn('Brake Pads', page)
        self.assertIn('475.00', page)
        self.assertNotIn('Carpentry BOQ', page)
        self.assertNotIn('Install wooden door', page)
        self.assertEqual(self.snapshot(), self.original)
        self.save(self.mechanic)
        self.assertEqual(ProTradeBoqItem.query.count(), 1)
        self.assertEqual(self.snapshot(), self.original)

    def test_migration_preserves_mechanic_rows_and_enforces_constraints(self):
        self.assertEqual(self.snapshot(), self.original)
        self.save()
        before = self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        statements = [
            "UPDATE protrade_boq_items SET material_rate=-1",
            "UPDATE protrade_boq_items SET material_rate='NaN'::numeric",
            "UPDATE protrade_boq_items SET unit='kg'",
            "UPDATE protrade_boq_items SET code='lowercase'",
            "UPDATE protrade_boq_items SET description=' '",
            "UPDATE protrade_boq_items SET shop_id=99999",
            "UPDATE protrade_boq_items SET user_id=99999",
            "UPDATE protrade_boq_items SET total_rate=1",
            "INSERT INTO protrade_boq_items (user_id,shop_id,trade,code,description,unit,material_rate,labour_rate,created_at) SELECT user_id,shop_id,trade,code,description,unit,material_rate,labour_rate,created_at FROM protrade_boq_items",
        ]
        for statement in statements:
            with self.subTest(statement=statement):
                with self.conn.begin_nested() as tx:
                    with self.assertRaises(sa.exc.DBAPIError):
                        self.conn.exec_driver_sql(statement)
                    tx.rollback()
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(), before)
        with self.assertRaisesRegex(RuntimeError, 'Destructive downgrade disabled'):
            self.migration.downgrade()


if __name__ == '__main__':
    unittest.main(verbosity=2)
