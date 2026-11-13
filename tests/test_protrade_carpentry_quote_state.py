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
        self.assertEqual(tree.xpath('//tr[contains(@class,"quote-line")]'),[])
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
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
                self.assertEqual(page.locator('.quote-line').count(),0)
                self.assertEqual(page.locator('#quote-work-select').input_value(),'')
                self.assertTrue(page.locator('#quote-save').is_disabled())
                page.locator('#quote-work-select').fill('CAR-004 \u2014 Install skirting')
                page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('.quote-line').count(),0)
                page.locator('#quote-work-quantity').fill('2.5')
                page.locator('#quote-add-item').click()
                self.assertTrue(page.locator('#carpentry-quote-form').evaluate('(form)=>form.checkValidity()'))
                self.assertEqual(page.locator(f'input[name="quantity_{each.id}"]').count(),0)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'325.00')
                # A fresh form's history restore must not retain an unsaved previous selection.
                page.evaluate("window.dispatchEvent(new Event('pageshow'))")
                self.assertEqual(page.locator('.quote-line').count(),0)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
                page.locator('#quote-work-select').fill('CAR-004 \u2014 Install skirting')
                page.locator('#quote-work-quantity').fill('2.5');page.locator('#quote-add-item').click()
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
        self.assertEqual(tree.xpath('//tr[contains(@class,"quote-line")]'),[])
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
        self.assertEqual(tree.xpath('//input[@name="item_id[]"]/@value'),[str(each.id)])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{each.id}"]/@value'),['3'])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{self.item.id}"]/@value'),[])
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(response.get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
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
        self.assertEqual(tree.xpath('//input[@name="item_id[]"]/@value'),[str(each.id)])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{self.item.id}"]/@value'),[])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{each.id}"]/@value'),['0'])
        self.assertEqual((draft.id,draft.quote_number,draft.items,draft.grand_total),before)

    def test_templates_unpriced_and_custom_items_are_shop_scoped(self):
        self.fixtures()
        custom=base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',code='CAR-C-SHOP',description='Custom timber repair',unit='m',material_rate=0,labour_rate=0)
        foreign=base.ProTradeBoqItem(user_id=2,shop_id=self.foreign.id,trade='carpentry',code='CAR-C-PRIVATE',description='Private owner work',unit='m',material_rate=10,labour_rate=20)
        self.db_session.add_all([custom,foreign]);self.db_session.commit()
        before=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        tree=html.fromstring(self.client.get(self.quote_url()).data)
        self.assertEqual(len(tree.xpath('//datalist[@id="quote-work-options"]/option')),16)
        self.assertNotIn('Private owner work',tree.text_content())
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(self.client.get(self.quote_url()).get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                page.locator('#quote-work-select').fill('CAR-001 \u2014 Supply and install wooden door')
                self.assertEqual(page.locator('#quote-work-code').input_value(),'CAR-001')
                self.assertEqual(page.locator('#quote-work-unit').input_value(),'Each')
                self.assertEqual(page.locator('#quote-work-material').input_value(),'')
                self.assertTrue(page.locator('#quote-add-item').is_disabled())
                page.locator('#quote-work-select').fill('CAR-C-SHOP \u2014 Custom timber repair')
                self.assertEqual(page.locator('#quote-work-material').input_value(),'0.00')
                self.assertFalse(page.locator('#quote-add-item').is_disabled())
                page.locator('#quote-work-quantity').fill('2');page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('.quote-line').count(),1)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
            finally:browser.close()
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(),before)

    def test_three_item_builder_save_reopen_and_verify_all_rates_quantities_totals(self):
        each=self.fixtures()
        custom=base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',code='CAR-C-SHOP',description='Custom timber repair',unit='m',material_rate=Decimal('25'),labour_rate=Decimal('10'))
        self.db_session.add(custom);self.db_session.commit()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(self.client.get(self.quote_url()).get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                for label,quantity in [('CAR-002 \u2014 Install door frame','3'),('CAR-004 \u2014 Install skirting','2.5'),('CAR-C-SHOP \u2014 Custom timber repair','4')]:
                    page.locator('#quote-work-select').fill(label)
                    page.locator('#quote-work-quantity').fill(quantity)
                    page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('.quote-line').count(),3)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'915.00')
                page.locator('#quote-work-select').fill('CAR-004 \u2014 Install skirting')
                self.assertTrue(page.locator('#quote-add-item').is_disabled())
                page.locator(f'input[name="quantity_{custom.id}"]').fill('5')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'950.00')
                page.get_by_role('button',name='Remove CAR-C-SHOP',exact=True).click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'775.00')
                page.locator('#quote-work-select').fill('CAR-C-SHOP \u2014 Custom timber repair')
                page.locator('#quote-work-quantity').fill('4');page.locator('#quote-add-item').click()
                fields=page.locator('#carpentry-quote-form').evaluate('(form)=>Array.from(new FormData(form).entries())')
                response=self.client.post(self.quote_url(),data=MultiDict(fields))
                self.assertEqual(response.status_code,302)
                draft=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
                identity=(draft.id,draft.quote_number)
                expected={self.item.id:('2.5','85.00','45.00','325.00'),each.id:('3','100.00','50.00','450.00'),custom.id:('4','25.00','10.00','140.00')}
                self.assertEqual({line['source_item_id']:(line['quantity'],line['material_rate'],line['labour_rate'],line['total']) for line in draft.items},expected)
                self.assertEqual(draft.grand_total,Decimal('915.00'))
                custom.material_rate=Decimal('999');self.db_session.commit()
                page.set_content(self.client.get(self.quote_url()).get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                self.assertEqual(page.locator('.quote-line').count(),3)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'915.00')
                for identifier,(quantity,material,labour,total) in expected.items():
                    row=page.locator(f'.quote-line[data-source-id="{identifier}"]')
                    self.assertEqual(row.locator('input[type=number]').input_value(),quantity)
                    self.assertEqual(row.locator('td').nth(4).text_content(),material)
                    self.assertEqual(row.locator('td').nth(5).text_content(),labour)
                    self.assertEqual(row.locator('.line-total').text_content(),total)
                fields=page.locator('#carpentry-quote-form').evaluate('(form)=>Array.from(new FormData(form).entries())')
                self.assertEqual(self.client.post(self.quote_url(),data=MultiDict(fields)).status_code,302)
                self.assertEqual((draft.id,draft.quote_number),identity)
                self.assertEqual(draft.grand_total,Decimal('915.00'))
                saved=self.client.get(response.location).get_data(as_text=True)
                self.assertIn('915.00',saved)
                self.assertEqual(base.ProTradeBoqQuote.query.filter_by(debtor_id=self.debtor().id).count(),1)
            finally:browser.close()


if __name__=='__main__':unittest.main()
