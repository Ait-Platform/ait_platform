"""Stage 3D: quantity calculations, unit controls, preservation and migration."""
from decimal import Decimal
import unittest
from lxml import html
from playwright.sync_api import sync_playwright
import sqlalchemy as sa
import test_protrade_carpentry_boq as base


class BoqQuantityTests(unittest.TestCase):
    setUp = base.CarpentryBoqTests.setUp
    shop = base.CarpentryBoqTests.shop
    save = base.CarpentryBoqTests.save
    page = base.CarpentryBoqTests.page
    snapshot = base.CarpentryBoqTests.snapshot

    def test_quantity_defaults_to_one_and_register_displays_amount(self):
        tree = html.fromstring(self.page().data)
        self.assertEqual(tree.xpath('//input[@name="quantity"]/@value'), ['1'])
        self.save()
        item = base.ProTradeBoqItem.query.one()
        self.assertEqual(item.quantity, Decimal('1.000'))
        self.assertEqual(item.total_rate, Decimal('1850.00'))
        self.assertEqual(item.total_amount, Decimal('1850.00'))
        tree = html.fromstring(self.page().data)
        self.assertIn('Quantity', tree.get_element_by_id('boq-items').text_content())
        self.assertIn('1.000', tree.get_element_by_id('boq-items').text_content())

    def test_measured_units_accept_decimals_and_edit_recalculates_amount(self):
        for index, unit in enumerate(('m', 'm\u00b2', 'm\u00b3')):
            self.save(description=f'Measured work {index}', unit=unit, quantity='2.5', material_rate='85', labour_rate='45', total_amount='0')
            item = base.ProTradeBoqItem.query.filter_by(description=f'Measured work {index}').one()
            self.assertEqual(item.total_rate, Decimal('130.00'))
            self.assertEqual(item.total_amount, Decimal('325.00'))
            register = html.fromstring(self.page().data).get_element_by_id('boq-items').text_content()
            self.assertIn('2.500', register)
            self.assertIn('325.00', register)
            self.save(item_id=item.id, description=item.description, unit=unit, quantity='3.125', material_rate='85', labour_rate='45')
            self.db_session.refresh(item)
            self.assertEqual(item.total_amount, Decimal('406.25'))
        self.assertEqual(self.snapshot(), self.original)

    def test_quantity_validation_and_whole_each(self):
        for quantity in ('', '0', '-1', 'NaN', 'Infinity', 'bad', '1.0001', '100000000000'):
            with self.subTest(quantity=quantity):
                self.save(quantity=quantity, unit='m', expected=400)
        self.save(quantity='1.5', unit='Each', expected=400)
        self.assertEqual(base.ProTradeBoqItem.query.count(), 0)
        self.save(quantity='2', unit='Each')
        self.assertEqual(base.ProTradeBoqItem.query.one().total_amount, Decimal('3700.00'))

    def test_unit_override_is_saved_and_owner_shop_checks_still_apply(self):
        self.save(preset_code='CAR-004', unit='m\u00b2', quantity='2.25')
        item = base.ProTradeBoqItem.query.one()
        self.assertEqual(item.unit, 'm\u00b2')
        self.assertEqual(item.total_amount, Decimal('4162.50'))
        self.save(self.second, item_id=item.id, quantity='7', expected=404)
        self.save(self.other, item_id=item.id, quantity='7', expected=404)
        self.assertEqual(item.quantity, Decimal('2.250'))

    def test_rounding_and_generated_amount_protected_by_postgresql(self):
        self.save(unit='m', quantity='0.005', material_rate='1', labour_rate='0')
        item = base.ProTradeBoqItem.query.one()
        self.assertEqual(item.total_amount, Decimal('0.01'))
        for statement in ["UPDATE protrade_boq_items SET quantity=0", "UPDATE protrade_boq_items SET quantity='NaN'::numeric", "UPDATE protrade_boq_items SET unit='Each'", "UPDATE protrade_boq_items SET total_amount=1"]:
            with self.conn.begin_nested() as tx:
                with self.assertRaises(sa.exc.DBAPIError):
                    self.conn.exec_driver_sql(statement)
                tx.rollback()

    def test_migration_gives_legacy_rows_one_without_changing_old_fields(self):
        self.conn.exec_driver_sql('ALTER TABLE protrade_boq_items DROP COLUMN total_amount, DROP COLUMN quantity CASCADE')
        self.conn.exec_driver_sql("INSERT INTO protrade_boq_items (user_id,shop_id,trade,code,description,unit,material_rate,labour_rate,created_at) VALUES (1,1,'carpentry','LEGACY','Legacy work','Each',12.34,56.78,'2026-01-01')")
        original_fields = 'id,user_id,shop_id,trade,code,description,unit,material_rate,labour_rate,total_rate,created_at'
        before = self.conn.exec_driver_sql(f'SELECT {original_fields} FROM protrade_boq_items ORDER BY id').all()
        self.quantity_migration.upgrade()
        self.assertEqual(self.conn.exec_driver_sql(f'SELECT {original_fields} FROM protrade_boq_items ORDER BY id').all(), before)
        self.assertEqual(self.conn.exec_driver_sql('SELECT quantity,total_amount FROM protrade_boq_items').one(), (Decimal('1.000'), Decimal('69.12')))
        self.assertEqual(self.snapshot(), self.original)

    def test_browser_units_work_on_add_edit_and_total_tracks_quantity(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_content(self.page().get_data(as_text=True))
                self.assertTrue(page.locator('#boq-unit').is_enabled())
                page.locator('#boq-unit').select_option('m')
                page.locator('#boq-description').fill('Install skirting')
                self.assertEqual(page.locator('#boq-unit').input_value(), 'm')
                self.assertTrue(page.locator('#boq-unit').is_enabled())
                page.locator('#boq-unit').select_option('m\u00b2')
                page.locator('#boq-quantity').fill('2.5')
                page.locator('#material-rate').fill('85')
                page.locator('#labour-rate').fill('45')
                self.assertEqual(page.locator('#total-rate').text_content(), '130.00')
                self.assertEqual(page.locator('#total-amount').text_content(), '325.00')
                page.locator('#boq-description').fill('Supply and install wooden door')
                self.assertEqual(page.locator('#boq-unit').input_value(), 'Each')
                self.assertEqual(page.locator('#boq-quantity').get_attribute('step'), '1')
                self.save(unit='m', quantity='2.5')
                item = base.ProTradeBoqItem.query.one()
                page.set_content(self.page(edit_id=item.id).get_data(as_text=True))
                self.assertTrue(page.locator('#boq-unit').is_enabled())
                self.assertEqual(page.locator('#boq-quantity').input_value(), '2.500')
                self.assertEqual(page.locator('#total-amount').text_content(), '4625.00')
            finally:
                browser.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
