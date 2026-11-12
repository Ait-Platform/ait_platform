"""Stage 3C catalog selection, custom codes and Hour migration on PostgreSQL."""
import unittest
from decimal import Decimal
from lxml import html
import test_protrade_carpentry_boq as base


class CarpentryCatalogTests(unittest.TestCase):
    setUp = base.CarpentryBoqTests.setUp
    shop = base.CarpentryBoqTests.shop
    save = base.CarpentryBoqTests.save
    page = base.CarpentryBoqTests.page
    snapshot = base.CarpentryBoqTests.snapshot

    def test_all_predefined_items_map_to_exact_codes_and_units_server_side(self):
        # Empty units use the predefined default; manual codes cannot override it.
        from app.protrade_boq_catalog import work_items_for
        entries = work_items_for('carpentry')
        self.assertEqual(len(entries), 15)
        self.assertEqual(entries[3], dict(code='CAR-004', description='Install skirting', unit='m'))
        self.assertEqual(entries[-1], dict(code='CAR-015', description='General carpentry work', unit='Hour'))
        for entry in entries:
            self.save(preset_code=entry['code'], description='forged', code='FORGED', unit='')
            item = base.ProTradeBoqItem.query.filter_by(code=entry['code']).one()
            self.assertEqual((item.description, item.unit), (entry['description'], entry['unit']))
            self.assertEqual(item.total_rate, Decimal('1850.00'))
        self.assertEqual(self.snapshot(), self.original)

    def test_searchable_description_dropdown_and_automatic_fields(self):
        response = self.page()
        tree = html.fromstring(response.data)
        self.assertEqual(tree.xpath('//input[@name="description"]/@list'), ['boq-work-items'])
        options = tree.xpath('//datalist[@id="boq-work-items"]/option')
        self.assertEqual(len(options), 15)
        self.assertTrue(tree.xpath('//input[@name="code" and @readonly]'))
        self.assertTrue(tree.xpath('//select[@name="unit"]/option[@value="Hour"]'))
        self.assertEqual(base.ProTradeBoqItem.query.count(), 0)
        # Works without JavaScript too: description match resolves the preset.
        self.save(description=' install skirting ', unit='', code='MANUAL')
        item = base.ProTradeBoqItem.query.one()
        self.assertEqual((item.code, item.description, item.unit), ('CAR-004', 'Install skirting', 'm'))

    def test_custom_codes_are_generated_unique_and_existing_edits_keep_codes(self):
        for _ in range(5):
            self.save(description='Custom bespoke joinery', code='CAR-004', unit='m\u00b3')
        items = base.ProTradeBoqItem.query.all()
        self.assertEqual(len({item.code for item in items}), 5)
        self.assertTrue(all(item.code.startswith('CAR-C-') for item in items))
        item = items[0]
        code = item.code
        self.save(item_id=item.id, description='Updated bespoke joinery', code='FORGED', material_rate='19.99', labour_rate='0.01')
        self.db_session.refresh(item)
        self.assertEqual(item.code, code)
        self.assertEqual(item.total_rate, Decimal('20.00'))
        self.assertEqual(base.ProTradeBoqItem.query.count(), 5)

    def test_unchanged_legacy_descriptions_codes_and_prices_are_preserved(self):
        legacy = base.ProTradeBoqItem(user_id=1, shop_id=self.carpentry.id, trade='carpentry',
                                      code='LEGACY-77', description='Install skirting', unit='Each',
                                      material_rate=Decimal('12.34'), labour_rate=Decimal('56.78'))
        self.db_session.add(legacy)
        self.db_session.commit()
        before = self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        page = self.page(edit_id=legacy.id)
        self.assertIn('LEGACY-77', page.get_data(as_text=True))
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(), before)
        self.save(item_id=legacy.id, description='Install skirting', unit='Each', material_rate='12.34', labour_rate='56.78')
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(), before)
        self.assertEqual(self.snapshot(), self.original)

    def test_predefined_duplicate_and_foreign_edit_do_not_overwrite(self):
        self.save(preset_code='CAR-004')
        item = base.ProTradeBoqItem.query.one()
        self.save(preset_code='CAR-004', material_rate='1', expected=409)
        self.assertEqual(item.material_rate, Decimal('1200.00'))
        self.save(self.second, preset_code='CAR-004')
        self.save(self.second, item_id=item.id, preset_code='CAR-004', expected=404)
        self.save(self.other, preset_code='CAR-004', expected=404)
        self.assertEqual(base.ProTradeBoqItem.query.count(), 2)

    def test_browser_selection_populates_code_unit_and_custom_mode(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_content(self.page().get_data(as_text=True))
                page.locator('#boq-description').fill('Install skirting')
                self.assertEqual(page.locator('#boq-code').input_value(), 'CAR-004')
                self.assertEqual(page.locator('#boq-unit').input_value(), 'm')
                self.assertFalse(page.locator('#boq-unit').is_disabled())
                self.assertEqual(page.locator('#preset-code').input_value(), 'CAR-004')
                page.locator('#material-rate').fill('0.10')
                page.locator('#labour-rate').fill('0.20')
                self.assertEqual(page.locator('#total-rate').text_content(), '0.30')
                page.locator('#boq-description').fill('General carpentry work')
                self.assertEqual(page.locator('#boq-unit').input_value(), 'Hour')
                page.locator('#boq-description').fill('Bespoke custom work')
                self.assertEqual(page.locator('#preset-code').input_value(), '')
                self.assertEqual(page.locator('#boq-code').input_value(), '')
                self.assertFalse(page.locator('#boq-unit').is_disabled())
            finally:
                browser.close()

    def test_hour_migration_preserves_existing_rows_and_is_repeatable(self):
        self.save(description='Legacy bespoke work')
        before = self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        # Re-running constraint-only migration must not rewrite any existing fields.
        self.unit_migration.upgrade()
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(), before)
        self.save(preset_code='CAR-015', unit='')
        self.assertEqual(base.ProTradeBoqItem.query.filter_by(code='CAR-015').one().unit, 'Hour')
        self.assertEqual(self.snapshot(), self.original)


if __name__ == '__main__':
    unittest.main(verbosity=2)
