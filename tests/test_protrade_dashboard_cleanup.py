"""Stage 3F: dashboard-only UI, exact record links and real browser profile modal."""
import unittest
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base


class DashboardCleanupTests(unittest.TestCase):
    setUp = base.ShopRegisterTests.setUp
    shop = base.ShopRegisterTests.shop

    def fixtures(self):
        carpentry = self.shop(name='Happy Shop', trade='carpentry', status='active')
        generic = self.shop(name='Happy Shop', trade='generic', status='active')
        self.shop(owner=2, name='Private Foreign Shop', status='active')
        client = base.models.MechClient(user_id=1, name='Jane & Co', phone='0123456789', email='jane@example.test', address='Client address')
        self.db_session.add(client)
        self.db_session.commit()
        return carpentry, generic, client

    def test_four_columns_single_quote_action_and_distinct_same_name_shops(self):
        carpentry, generic, client = self.fixtures()
        before = self.conn.exec_driver_sql('SELECT * FROM mech_shops ORDER BY id').all()
        clients_before = self.conn.exec_driver_sql('SELECT * FROM mech_clients ORDER BY id').all()
        response = self.client.get(f'/mechanic/dashboard?shop_id={carpentry.id}')
        self.assertEqual(response.status_code, 200)
        tree = html.fromstring(response.data)
        table = tree.get_element_by_id('client-register-table')
        self.assertEqual(table.xpath('./thead/tr/th/text()'), ['Client', 'Contact', 'Service', 'Action'])
        rows = table.xpath('./tbody/tr')
        self.assertEqual(len(rows), 3)
        for row in rows:
            self.assertEqual(len(row.xpath('./td')), 4)
            self.assertEqual([text.strip() for text in row.xpath('./td[last()]/*/text()') if text.strip()], ['Quote'])
            self.assertFalse(row.xpath('./td[last()]//form | ./td[last()]//details | ./td[last()]//summary'))
        happy = table.xpath('.//td[1]/a[text()="Happy Shop"]')
        self.assertEqual(len(happy), 2)
        urls = [link.get('href') for link in happy]
        self.assertTrue(any(f'shop_id={carpentry.id}' in url and 'action=setup' in url for url in urls))
        self.assertTrue(any(f'shop_id={generic.id}' in url and 'action=setup' in url for url in urls))
        self.assertIn('Carpentry', table.text_content())
        self.assertIn('Generic Business', table.text_content())
        self.assertNotIn('Private Foreign Shop', table.text_content())
        generic_row = table.xpath('.//tr[td[3][contains(.,"Generic Business")]]')[0]
        self.assertTrue(generic_row.xpath('./td[last()]/button[@disabled and text()="Quote"]'))
        self.assertTrue(table.xpath(f'.//a[@href="/mechanic/shop/{carpentry.id}/quote"]'))
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM mech_shops ORDER BY id').all(), before)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM mech_clients ORDER BY id').all(), clients_before)

    def test_dashboard_menu_hides_quote_only_on_dashboard(self):
        carpentry, _, _ = self.fixtures()
        source = (base.ROOT / 'templates/program_mechanic/menu.html').read_text(encoding='utf-8')
        with self.app.test_request_context('/'):
            dashboard = self.app.jinja_env.from_string(source).render(protrade_menu_active='dashboard', active_shop=carpentry)
            existing = self.app.jinja_env.from_string(source).render(protrade_menu_active='quote', active_shop=carpentry)
        self.assertFalse(html.fromstring(dashboard).xpath('//a[text()="Quote"]'))
        self.assertTrue(html.fromstring(existing).xpath('//a[text()="Quote"]'))
        self.assertIn('Price List', dashboard)
        self.assertIn('Invoice', dashboard)

    def test_name_opens_editable_customer_profile_and_exact_shop_profile(self):
        carpentry, generic, client = self.fixtures()
        response = self.client.get(f'/mechanic/dashboard?shop_id={carpentry.id}')
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.set_content('<base href="http://stage3f.test/">' + response.get_data(as_text=True))
                page.get_by_role('link', name='Jane & Co', exact=True).click()
                self.assertTrue(page.locator('#client-profile-dialog').is_visible())
                form = page.locator('#client-profile-form')
                self.assertEqual(form.locator('[name=name]').input_value(), 'Jane & Co')
                self.assertEqual(form.locator('[name=phone]').input_value(), '0123456789')
                self.assertEqual(form.locator('[name=email]').input_value(), 'jane@example.test')
                self.assertIn(str(client.id), form.get_attribute('action'))
                self.assertEqual(page.locator('#client-profile-address').text_content(), 'Client address')
                form.locator('[name=name]').fill('Jane Updated')
                data = form.evaluate('(form) => Object.fromEntries(new FormData(form))')
                self.assertEqual(data['name'], 'Jane Updated')
                self.assertEqual(data['phone'], '0123456789')
                page.locator('#client-profile-dialog').get_by_role('button', name='Back').click()
                self.assertFalse(page.locator('#client-profile-dialog').is_visible())
                # Server resolves each same-name shop by ID; clicking its name opens existing setup.
                generic_page = self.client.get(f'/mechanic/dashboard?shop_id={generic.id}&action=setup')
                page.route('http://stage3f.test/**', lambda route: route.fulfill(status=200, content_type='text/html', body=generic_page.get_data(as_text=True)))
                page.goto(f'http://stage3f.test/mechanic/dashboard?shop_id={generic.id}&action=setup')
                self.assertNotIn('hidden', page.locator('#manual-setup-modal').get_attribute('class').split())
                self.assertEqual(page.locator('#manual-setup-modal [name=shop_id]').input_value(), str(generic.id))
                self.assertEqual(page.locator('#manual-setup-modal [name=business_name]').input_value(), 'Happy Shop')
                self.assertEqual(page.locator('#profile-trade-select').input_value(), 'generic')
            finally:
                browser.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
