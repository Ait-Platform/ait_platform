"""Carpentry quote UI state on existing routes, PostgreSQL and Chromium."""
from decimal import Decimal
import unittest
from lxml import html
from playwright.sync_api import sync_playwright
from werkzeug.datastructures import MultiDict
import test_protrade_shop_register as base
import test_protrade_debtor_foundation as foundation


class CarpentryQuoteStateTests(unittest.TestCase):
    setUp=foundation.DebtorFoundationTests.setUp
    shop=foundation.DebtorFoundationTests.shop
    save_debtor=foundation.DebtorFoundationTests.save_debtor
    debtor=foundation.DebtorFoundationTests.debtor
    save_quote=foundation.DebtorFoundationTests.save_quote
    quote_url=foundation.DebtorFoundationTests.quote_url

    def fixtures(self):
        self.save_debtor()
        each=base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',
            code='CAR-002',description='Install door frame',unit='Each',quantity=9,
            material_rate=Decimal('100'),labour_rate=Decimal('50'))
        self.db_session.add(each);self.db_session.commit();return each

    def test_new_form_blank_scoped_labels_and_register_link(self):
        self.fixtures()
        response=self.client.get(self.quote_url());tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//h1/text()'),['Carpentry Quotation'])
        self.assertTrue(tree.xpath('//h2[text()="Quotation Items"]'))
        self.assertIn('Item Code',tree.get_element_by_id('quote-boq-items').text_content())
        self.assertFalse(tree.xpath('//input[@type="checkbox"][@checked]'))
        self.assertEqual(tree.xpath('//input[@type="number"]/@value'),['',''])
        self.assertFalse(tree.xpath('//input[@type="number"][@required]'))
        self.assertEqual(tree.get_element_by_id('quote-grand-total').text_content(),'0.00')
        self.assertNotIn('Saved Quotes',tree.text_content())
        self.assertNotIn('LEGACY-CAR',tree.text_content())
        self.assertTrue(tree.xpath(f'//a[@href="/mechanic/quote-invoice?shop_id={self.carpentry.id}"]'))
        self.assertEqual(base.ProTradeBoqQuote.query.count(),1)

    def test_browser_blank_defaults_required_selection_and_submission(self):
        each=self.fixtures()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(self.client.get(self.quote_url()).get_data(as_text=True))
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
                self.assertEqual(page.locator('input[type=checkbox]:checked').count(),0)
                self.assertEqual(page.locator('input[type=number]').evaluate_all('(nodes)=>nodes.map(node=>node.value)'),['',''])
                # Simulate browser-restored fields; the form must restore this page's server state.
                page.evaluate("""() => {
                    document.querySelector('input[type=checkbox]').checked=true;
                    document.querySelector('input[type=number]').value='99';
                    window.dispatchEvent(new Event('pageshow'));
                }""")
                self.assertEqual(page.locator('input[type=checkbox]:checked').count(),0)
                self.assertEqual(page.locator('input[type=number]').evaluate_all('(nodes)=>nodes.map(node=>node.value)'),['',''])
                page.locator(f'input[type=checkbox][value="{self.item.id}"]').check()
                self.assertFalse(page.locator('#carpentry-quote-form').evaluate('(form)=>form.checkValidity()'))
                page.locator(f'input[name="quantity_{self.item.id}"]').fill('2.5')
                self.assertTrue(page.locator('#carpentry-quote-form').evaluate('(form)=>form.checkValidity()'))
                self.assertFalse(page.locator(f'input[name="quantity_{each.id}"]').evaluate('(node)=>node.required'))
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'325.00')
                fields=page.locator('#carpentry-quote-form').evaluate('(form)=>Array.from(new FormData(form).entries())')
                response=self.client.post(self.quote_url(),data=MultiDict(fields))
                self.assertEqual(response.status_code,302)
                draft=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
                self.assertEqual(draft.grand_total,Decimal('325.00'))
                self.assertEqual([line['source_item_id'] for line in draft.items],[self.item.id])
            finally:browser.close()

    def test_explicit_new_quote_is_blank_and_previous_quotes_remain_in_register(self):
        self.fixtures();self.save_quote()
        first=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        snapshot=(first.id,first.quote_number,first.items,first.grand_total)
        response=self.client.post(f'/mechanic/shop/{self.carpentry.id}/debtor/{self.debtor().id}/quotes/new')
        tree=html.fromstring(self.client.get(response.location).data)
        self.assertFalse(tree.xpath('//input[@type="checkbox"][@checked]'))
        self.assertEqual(tree.xpath('//input[@type="number"]/@value'),['',''])
        self.assertNotIn('Saved Quotes',tree.text_content())
        self.assertEqual((first.id,first.quote_number,first.items,first.grand_total),snapshot)
        register=html.fromstring(self.client.get(f'/mechanic/quote-invoice?shop_id={self.carpentry.id}').data)
        self.assertIn(first.quote_number,register.text_content())
        self.assertTrue(register.xpath(f'//a[@href="/mechanic/shop/{self.carpentry.id}/quotes/{first.id}"]'))
        self.assertEqual(base.ProTradeBoqQuote.query.count(),2)

    def test_edit_uses_only_current_quote_snapshot_not_other_quotes_or_price_changes(self):
        each=self.fixtures();self.save_quote()
        first=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        self.client.post(f'/mechanic/shop/{self.carpentry.id}/debtor/{self.debtor().id}/quotes/new')
        self.save_quote(**{'item_id[]':[str(each.id)],f'quantity_{each.id}':'3'})
        current=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        identifier,number=current.id,current.quote_number
        each.material_rate=Decimal('999');self.db_session.commit()
        response=self.client.get(self.quote_url());tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//input[@type="checkbox"][@checked]/@value'),[str(each.id)])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{each.id}"]/@value'),['3'])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{self.item.id}"]/@value'),[''])
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(response.get_data(as_text=True))
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'450.00')
            finally:browser.close()
        old_view=html.fromstring(self.client.get(f'/mechanic/shop/{self.carpentry.id}/quotes/{first.id}').data)
        self.assertEqual(old_view.get_element_by_id('quote-grand-total').text_content(),'325.00')
        self.assertIn('CAR-004',old_view.text_content());self.assertNotIn('CAR-002',old_view.text_content())
        key=tree.xpath('//input[@name="quote_key"]/@value')[0]
        self.save_quote(key=key,**{'item_id[]':[str(each.id)],f'quantity_{each.id}':'4'})
        self.assertEqual((current.id,current.quote_number),(identifier,number))
        self.assertEqual(current.grand_total,Decimal('600.00'))
        self.assertEqual(current.items[0]['material_rate'],'100.00')
        self.assertEqual(first.grand_total,Decimal('325.00'))

    def test_invalid_edit_preserves_only_submitted_selection_and_does_not_change_saved_quote(self):
        each=self.fixtures();self.save_quote()
        draft=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        before=(draft.id,draft.quote_number,draft.items,draft.grand_total)
        response=self.client.post(self.quote_url(),data={'quote_key':draft.quote_number.removeprefix('CAR-Q-'), 'item_id[]':[str(each.id)],f'quantity_{each.id}':'0'})
        self.assertEqual(response.status_code,400)
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//input[@type="checkbox"][@checked]/@value'),[str(each.id)])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{self.item.id}"]/@value'),[''])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{each.id}"]/@value'),['0'])
        self.assertEqual((draft.id,draft.quote_number,draft.items,draft.grand_total),before)


if __name__=='__main__':unittest.main()
