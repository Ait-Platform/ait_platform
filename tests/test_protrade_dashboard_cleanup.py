"""Dashboard regressions updated for the shop-first Debtor Register workflow."""
import unittest
from lxml import html
from playwright.sync_api import sync_playwright
from urllib.parse import urlsplit
import test_protrade_shop_register as base


class DashboardCleanupTests(unittest.TestCase):
    setUp=base.ShopRegisterTests.setUp
    shop=base.ShopRegisterTests.shop

    def fixtures(self):
        carpentry=self.shop(name='Happy Shop',trade='carpentry',status='active')
        generic=self.shop(name='Happy Shop',trade='generic',status='active')
        self.shop(owner=2,name='Private Foreign Shop',status='active')
        client=base.models.MechClient(user_id=1,name='Legacy Customer')
        self.db_session.add(client);self.db_session.commit()
        return carpentry,generic,client

    def test_four_columns_distinct_shops_and_no_debtors_at_consultancy_level(self):
        carpentry,generic,_=self.fixtures()
        before=self.conn.exec_driver_sql('SELECT * FROM mech_shops ORDER BY id').all()
        tree=html.fromstring(self.client.get(f'/mechanic/dashboard?shop_id={carpentry.id}').data)
        table=tree.get_element_by_id('client-register-table')
        self.assertEqual(table.xpath('./thead/tr/th/text()'),['Client','Contact','Service','Action'])
        self.assertEqual(len(table.xpath('./tbody/tr')),2)
        for row in table.xpath('./tbody/tr'):
            self.assertEqual(row.xpath('./td[last()]/a/text()'),['Price List','Debtor Register'])
        happy=table.xpath('.//td[1]/a[text()="Happy Shop"]')
        self.assertEqual({link.get('href') for link in happy},{f'/mechanic/dashboard?shop_id={carpentry.id}&action=setup',f'/mechanic/dashboard?shop_id={generic.id}&action=setup'})
        self.assertNotIn('Legacy Customer',table.text_content())
        self.assertNotIn('Private Foreign Shop',table.text_content())
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM mech_shops ORDER BY id').all(),before)

    def test_dashboard_menu_has_foundation_actions(self):
        carpentry,_,_=self.fixtures()
        source=(base.ROOT/'templates/program_mechanic/menu.html').read_text(encoding='utf-8')
        with self.app.test_request_context('/'):
            menu=self.app.jinja_env.from_string(source).render(protrade_menu_active='dashboard',active_shop=carpentry)
        self.assertEqual(html.fromstring(menu).xpath('//a/text()'),['Debtor','Tokens','Output','Folders','Help'])

    def test_row_actions_stay_together_without_horizontal_scrolling(self):
        carpentry,_,_=self.fixtures()
        tree=html.fromstring(self.client.get(f'/mechanic/dashboard?shop_id={carpentry.id}').data)
        table=html.tostring(tree.get_element_by_id('client-register-table'),encoding='unicode')
        styles=''.join(tree.xpath('//style/text()'))
        # Isolate the actual register and its responsive CSS from the legacy dashboard.
        utilities="*{box-sizing:border-box}body{margin:0;font:14px Arial}table{width:100%;table-layout:fixed;border-collapse:collapse}td,th{padding:12px}.break-words{overflow-wrap:break-word}a{padding:8px 12px;font-weight:600}.hidden{display:none}"
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page()
                for width in (320,375,640,768,1280):
                    page.set_viewport_size(dict(width=width,height=900))
                    page.set_content(f'<style>{utilities}{styles}</style>{table}')
                    self.assertTrue(page.evaluate('document.documentElement.scrollWidth <= innerWidth'), (width, page.locator('table').bounding_box(), page.evaluate('[...document.querySelectorAll("*")].filter(e=>e.getBoundingClientRect().right>innerWidth).map(e=>[e.tagName,e.className,e.getBoundingClientRect().right])')))
                    for cell in page.locator('.shop-actions').all():
                        first,second=cell.locator('a').all()
                        left,right=first.bounding_box(),second.bounding_box()
                        self.assertAlmostEqual(left['y'],right['y'],delta=1)
                        self.assertLessEqual(left['x']+left['width'],right['x'])
                        self.assertLessEqual(right['x']+right['width'],width)
            finally:
                browser.close()

    def test_shop_name_opens_exact_profile_without_creating_records(self):
        carpentry,generic,_=self.fixtures()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page()
                def route_request(route):
                    url=urlsplit(route.request.url)
                    response=self.client.get(url.path+('?' + url.query if url.query else ''))
                    route.fulfill(status=response.status_code,content_type='text/html',body=response.get_data(as_text=True))
                page.route('http://stage3h.test/**',route_request)
                page.goto(f'http://stage3h.test/mechanic/dashboard?shop_id={carpentry.id}')
                page.locator(f'#client-register-table td:first-child a[href="/mechanic/dashboard?shop_id={generic.id}&action=setup"]').click()
                self.assertIn(f'shop_id={generic.id}',page.url)
                self.assertTrue(page.locator('#manual-setup-modal').is_visible())
                self.assertEqual(page.locator('#manual-setup-modal input[name=shop_id]').input_value(),str(generic.id))
                self.assertEqual(page.locator('#manual-setup-modal input[name=business_name]').input_value(),'Happy Shop')
                self.assertEqual(page.locator('#manual-setup-modal [name=address]').input_value(),'Existing address')
            finally:
                browser.close()
        self.assertEqual(base.models.Debtor.query.count(),0)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),0)
        self.assertEqual(base.models.MechShop.query.filter_by(user_id=1).count(),2)

    def test_foreign_or_invalid_profile_id_cannot_fall_back_to_another_shop(self):
        self.fixtures()
        foreign=base.models.MechShop.query.filter_by(user_id=2).one()
        for identifier in [foreign.id,999999,'invalid']:
            self.assertEqual(self.client.get('/mechanic/dashboard',query_string=dict(shop_id=identifier,action='setup')).status_code,404)


if __name__=='__main__':
    unittest.main(verbosity=2)
