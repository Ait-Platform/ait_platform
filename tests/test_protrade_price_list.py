"""Stage 3A: actual price-list routes/template against temporary PostgreSQL data."""
import unittest
from lxml import html
from test_protrade_shop_register import ShopRegisterTests, MechCatalogPart, models, ROOT


class PriceListTests(unittest.TestCase):
    shop = ShopRegisterTests.shop
    def setUp(self):
        ShopRegisterTests.setUp(self)
        self.mech = self.shop(status='active')
        self.second = self.shop(name='Second Mechanic', status='active')
        self.carpentry = self.shop(name='Happy Carpentry', trade='carpentry', status='active')
        self.other = self.shop(owner=2, name='Other Owner', status='active')
        self.global_part = self.part('Brake Pads', 450)
        self.legacy = self.part('Brake Pads', 475, user_id=1)
        self.part('Other Private', 999, user_id=2)

    def part(self, name, price, **kwargs):
        part = MechCatalogPart(part_name=name, default_price=price, **kwargs)
        self.db_session.add(part)
        self.db_session.commit()
        return part

    def page(self, shop):
        response = self.client.get(f'/mechanic/catalog?shop_id={shop.id}')
        self.assertEqual(response.status_code, 200)
        return response.get_data(as_text=True)

    def save(self, shop, name='Brake Pads', price='123.45', **kwargs):
        data = dict(shop_id=shop.id, action='add', part_name=name, price=price)
        data.update(kwargs)
        response = self.client.post('/mechanic/catalog', data=data)
        self.assertEqual(response.status_code, 302)
        self.assertIn(f'shop_id={shop.id}', response.location)

    def test_legacy_mechanic_price_and_global_override_preserved(self):
        before = (self.legacy.id, self.legacy.default_price, self.global_part.default_price)
        page = self.page(self.mech)
        self.assertIn('Manage Price List', page)
        self.assertIn('Trade: General Mechanic', page)
        self.assertIn('475.00', page)
        self.assertNotIn('450.00', page)
        self.assertNotIn('Other Private', page)
        self.assertEqual(before, (self.legacy.id, self.legacy.default_price, self.global_part.default_price))
        self.assertEqual(self.ns['selected_price_list'](self.mech)[0].id, self.legacy.id)

    def test_trade_and_same_owner_shop_isolation(self):
        self.part('Wooden door', 1200, user_id=1, shop_id=self.carpentry.id, trade='carpentry')
        self.part('Wrong trade', 10, user_id=1, shop_id=self.carpentry.id, trade='plumbing')
        page = self.page(self.carpentry)
        self.assertIn('Trade: Carpentry', page)
        self.assertIn('Wooden door', page)
        self.assertNotIn('Brake Pads', page)
        self.assertNotIn('Wrong trade', page)
        self.assertNotIn('Wooden door', self.page(self.mech))
        self.assertIn('450.00', self.page(self.second))
        self.assertNotIn('475.00', self.page(self.second))
        self.assertEqual(self.legacy.default_price, 475)

    def test_same_named_prices_repeat_save_update_only_selected_shop(self):
        for _ in range(2):
            self.save(self.carpentry)
        self.save(self.second, price='600')
        scoped = MechCatalogPart.query.filter_by(shop_id=self.carpentry.id).all()
        self.assertEqual(len(scoped), 1)
        self.assertEqual(scoped[0].trade, 'carpentry')
        self.assertEqual(self.legacy.default_price, 475)
        self.assertEqual(self.global_part.default_price, 450)
        self.save(self.mech, price='500')
        self.assertEqual(self.legacy.default_price, 500)
        self.assertEqual(self.global_part.default_price, 450)
        self.assertEqual(scoped[0].default_price, 123.45)

    def test_delete_and_explicit_context_cannot_cross_shop_or_owner(self):
        item = self.part('Wood', 10, user_id=1, shop_id=self.carpentry.id, trade='carpentry')
        for shop, identifier in [(self.mech, item.id), (self.second, self.legacy.id), (self.mech, self.global_part.id)]:
            self.client.post('/mechanic/catalog', data=dict(shop_id=shop.id, action='delete', part_id=identifier))
            self.assertIsNotNone(self.db_session.get(MechCatalogPart, identifier))
        self.assertEqual(self.client.get(f'/mechanic/catalog?shop_id={self.other.id}').status_code, 404)
        self.assertEqual(self.client.post('/mechanic/catalog', data=dict(shop_id=self.other.id, action='add', part_name='stolen')).status_code, 404)
        self.assertEqual(self.client.get('/mechanic/catalog?shop_id=bad').status_code, 404)
        # Explicit shop context wins over stale session trade, and a forged trade is ignored.
        with self.client.session_transaction() as sess:
            sess['active_trade'] = 'mechanic'
            sess['active_shop_id'] = self.mech.id
        self.save(self.carpentry, name='Timber', trade='mechanic')
        self.assertEqual(MechCatalogPart.query.filter_by(part_name='Timber').one().trade, 'carpentry')
        self.client.post('/mechanic/catalog', data=dict(shop_id=self.carpentry.id, action='delete', part_id=item.id))
        self.assertIsNone(self.db_session.get(MechCatalogPart, item.id))

    def test_schema_upgrade_is_repeatable_and_preserves_legacy_rows(self):
        import importlib.util
        from contextlib import contextmanager
        from types import SimpleNamespace
        spec = importlib.util.spec_from_file_location('price_schema', ROOT / 'app/protrade_price_schema.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        before = self.conn.exec_driver_sql('SELECT id,user_id,part_name,category,default_price,created_at FROM mech_catalog_parts ORDER BY id').all()
        self.conn.exec_driver_sql('ALTER TABLE mech_catalog_parts DROP COLUMN shop_id, DROP COLUMN trade')

        @contextmanager
        def begin():
            with self.conn.begin_nested():
                yield self.conn

        module.ensure_price_list_schema(SimpleNamespace(begin=begin))
        module.ensure_price_list_schema(SimpleNamespace(begin=begin))
        after = self.conn.exec_driver_sql('SELECT id,user_id,part_name,category,default_price,created_at FROM mech_catalog_parts ORDER BY id').all()
        self.assertEqual(before, after)
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM mech_catalog_parts WHERE shop_id IS NOT NULL OR trade IS NOT NULL').scalar(), 0)

    def test_menu_keeps_shop_context_and_trade_label(self):
        from flask import render_template
        with self.app.test_request_context('/'):
            menu = render_template('program_mechanic/menu.html', active_shop=self.carpentry)
            # The integration harness stubs menu rendering for unrelated dashboard links.
            source = (ROOT / 'templates/program_mechanic/menu.html').read_text()
            menu = self.app.jinja_env.from_string(source).render(active_shop=self.carpentry)
        tree = html.fromstring(menu)
        link = tree.xpath('//a[text()="Price List"]')[0]
        self.assertIn(f'shop_id={self.carpentry.id}', link.get('href'))
        self.assertNotIn('Part List', menu)
        response = self.client.get(f'/mechanic/dashboard?shop_id={self.carpentry.id}')
        dashboard = html.fromstring(response.data)
        self.assertIn('Carpentry', dashboard.xpath('//p[contains(@class,"text-xs") and contains(.,"Active Profile")]/text()')[0])
        self.assertEqual(self.ns['registered_trade_name'](self.carpentry), 'Carpentry')


if __name__ == '__main__':
    unittest.main(verbosity=2)
