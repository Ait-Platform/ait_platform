"""Debtor working-area UI on actual shop routes, PostgreSQL and Chromium."""
import unittest
from urllib.parse import urlsplit
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
from test_protrade_debtor_foundation import DebtorFoundationTests


class DebtorWorkingAreaTests(unittest.TestCase):
    setUp=DebtorFoundationTests.setUp
    shop=DebtorFoundationTests.shop
    save_debtor=DebtorFoundationTests.save_debtor
    debtor=DebtorFoundationTests.debtor
    save_quote=DebtorFoundationTests.save_quote
    quote_url=DebtorFoundationTests.quote_url

    def register_url(self):
        return f'/mechanic/shop/{self.carpentry.id}/debtors'

    def browser_route(self,route):
        request=route.request
        url=urlsplit(request.url)
        if url.path=='/static/js/shop_debtors.js':
            return route.fulfill(path=str(base.ROOT/'app/static/js/shop_debtors.js'),content_type='application/javascript')
        response=self.client.open(url.path+('?' +url.query if url.query else ''),method=request.method,
            data=request.post_data,content_type=request.headers.get('content-type'),follow_redirects=True)
        route.fulfill(status=response.status_code,headers={k:v for k,v in response.headers if k.lower() not in ('content-length','set-cookie')},body=response.data)

    def test_register_starts_collapsed_and_legacy_notice_is_removed(self):
        self.save_debtor()
        tree=html.fromstring(self.client.get(self.register_url()).data)
        self.assertIn('hidden',tree.get_element_by_id('debtor-working-area').attrib)
        self.assertEqual(tree.get_element_by_id('add-debtor').text_content(),'Add Debtor')
        for label in ('Price List','Debtor','Tokens','Output'):
            self.assertFalse(tree.xpath('//a[normalize-space(text())="'+label+'"]'))
        register_link=tree.xpath('//a[normalize-space(text())="Debtor Register"]')[0]
        self.assertEqual(register_link.get('href'),self.register_url())
        self.assertTrue(tree.xpath('//table[@id="shop-debtors"]//a[text()="Quote"]'))
        self.assertTrue(tree.xpath('//table[@id="shop-debtors"]//a[text()="Ledger"]'))
        self.assertIn('Debtor Jane',tree.get_element_by_id('shop-debtors').text_content())
        self.assertNotIn('Legacy Quotes / Invoices',tree.text_content())
        self.assertNotIn('Legacy Debtor Accounts',tree.text_content())
        self.assertEqual(base.ProTradeBoqQuote.query.filter_by(id=self.legacy_quote.id).count(),1)
        self.assertEqual(len(tree.xpath('//form[@id="debtor-capture-form"]')),1)

    def test_validation_error_reopens_working_area_and_preserves_register(self):
        self.save_debtor()
        response=self.save_debtor(expected=400,name='',phone='Unsubmitted phone')
        tree=html.fromstring(response.data)
        self.assertNotIn('hidden',tree.get_element_by_id('debtor-working-area').attrib)
        self.assertTrue(tree.xpath('//*[@role="alert"]'))
        self.assertEqual(tree.xpath('//input[@name="phone"]/@value'),['Unsubmitted phone'])
        self.assertIn('Debtor Jane',tree.get_element_by_id('shop-debtors').text_content())
        self.assertEqual(base.models.Debtor.query.count(),1)

    def test_browser_add_close_save_edit_and_switch_back_to_add(self):
        self.save_debtor()
        original=self.debtor();original_id=original.id;original_reference=original.reference_id
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost'+self.register_url())
                area=page.locator('#debtor-working-area');register=page.locator('#shop-debtors')
                self.assertFalse(area.is_visible());self.assertTrue(register.is_visible())
                page.get_by_role('button',name='Add Debtor',exact=True).click()
                self.assertTrue(area.is_visible());self.assertTrue(register.is_visible())
                self.assertLess(area.bounding_box()['y'],register.bounding_box()['y'])
                self.assertTrue(page.locator('input[name=name]').evaluate('(node)=>node===document.activeElement'))
                page.locator('input[name=name]').fill('Discarded draft')
                current_url=page.url
                page.get_by_role('button',name='Close',exact=True).click()
                self.assertEqual(page.url,current_url);self.assertFalse(area.is_visible());self.assertTrue(register.is_visible())
                self.assertEqual(base.models.Debtor.query.count(),1)
                page.get_by_role('link',name='Debtor Jane',exact=True).click()
                self.assertTrue(area.is_visible());self.assertTrue(register.is_visible())
                self.assertEqual(page.locator('#debtor-form-title').text_content(),'Edit Debtor')
                self.assertEqual(page.locator('input[name=debtor_id]').input_value(),str(original_id))
                page.locator('input[name=address]').fill('Updated shop customer address')
                page.get_by_role('button',name='Save',exact=True).click()
                register.wait_for(state='visible')
                self.assertTrue(register.is_visible())
                self.assertIn('Updated shop customer address',register.text_content())
                self.assertEqual(base.models.Debtor.query.count(),1)
                self.assertEqual(self.debtor().reference_id,original_reference)
                page.get_by_role('button',name='Add Debtor',exact=True).click()
                self.assertEqual(page.locator('#debtor-form-title').text_content(),'Add Debtor')
                self.assertTrue(page.locator('input[name=debtor_id]').is_disabled())
                self.assertEqual(page.locator('input[name=name]').input_value(),'')
                # Repeated Add must reuse the form, not create another working area.
                page.get_by_role('button',name='Add Debtor',exact=True).click()
                self.assertEqual(page.locator('#debtor-working-area').count(),1)
                self.assertEqual(page.locator('#debtor-capture-form').count(),1)
                page.locator('input[name=name]').fill('Second Shop Customer')
                page.locator('input[name=phone]').fill('0987654321')
                page.locator('input[name=email]').fill('second@example.com')
                page.locator('input[name=address]').fill('New customer address')
                page.get_by_role('button',name='Save',exact=True).click()
                register.wait_for(state='visible')
                self.assertTrue(register.is_visible())
                self.assertIn('Debtor Jane',register.text_content());self.assertIn('Second Shop Customer',register.text_content())
                debtors=base.models.Debtor.query.order_by(base.models.Debtor.id).all()
                self.assertEqual(len(debtors),2)
                self.assertTrue(all(d.shop_id==self.carpentry.id and d.user_id==1 for d in debtors))
                self.assertNotEqual(debtors[0].reference_id,debtors[1].reference_id)
                page.get_by_role('button',name='Close',exact=True).click()
                self.assertFalse(area.is_visible());self.assertTrue(register.is_visible())
                self.assertEqual(base.models.Debtor.query.filter_by(shop_id=self.second.id).count(),0)
            finally:browser.close()

    def test_edit_preserves_quote_and_history_is_in_quotation_hub(self):
        self.save_debtor();self.save_quote()
        debtor=self.debtor()
        tree=html.fromstring(self.client.get(self.register_url()+f'?debtor_id={debtor.id}').data)
        self.assertNotIn('hidden',tree.get_element_by_id('debtor-working-area').attrib)
        self.assertEqual(tree.get_element_by_id('debtor-form-title').text_content(),'Edit Debtor')
        self.assertTrue(tree.xpath(f'//table[@id="shop-debtors"]//a[@href="{self.quote_url()}"]'))
        self.assertFalse(tree.xpath('//button[text()="New Quote"]'))
        self.assertNotIn('Quotations and Invoices for',tree.text_content())
        self.assertNotIn('CAR-Q-',tree.text_content())
        hub=html.fromstring(self.client.get(self.quote_url()+'?area=register').data)
        self.assertIn('CAR-Q-',hub.get_element_by_id('debtor-quote-register').text_content())
        self.assertEqual(self.client.get(self.quote_url()).status_code,200)
        self.assertEqual(base.ProTradeBoqQuote.query.filter_by(debtor_id=debtor.id).count(),1)


if __name__=='__main__':unittest.main()
