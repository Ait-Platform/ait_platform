"""Tomas input regressions on local PostgreSQL and real Chromium controls."""
from decimal import Decimal
import unittest
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_debtor_foundation as foundation


class CarpentryQuoteInputTests(unittest.TestCase):
    setUp=foundation.DebtorFoundationTests.setUp
    shop=foundation.DebtorFoundationTests.shop
    save_debtor=foundation.DebtorFoundationTests.save_debtor
    debtor=foundation.DebtorFoundationTests.debtor
    save_quote=foundation.DebtorFoundationTests.save_quote
    quote_url=foundation.DebtorFoundationTests.quote_url

    def fixtures(self):
        self.save_debtor(name='Tomas')
        self.tomas=self.debtor()
        self.url=self.quote_url(self.tomas)
        self.register=f'/mechanic/shop/{self.carpentry.id}/debtors?debtor_id={self.tomas.id}'

    def browser_route(self,route):
        req=route.request;url=urlsplit(req.url)
        if url.path in ('/static/js/carpentry_quote_builder.js','/static/js/carpentry_quote_hub.js','/static/js/shop_debtors.js'):
            return route.fulfill(path=str(base.ROOT/'app'/url.path.removeprefix('/')),content_type='application/javascript')
        response=self.client.open(url.path+('?' +url.query if url.query else ''),method=req.method,data=req.post_data,
            content_type=req.headers.get('content-type'),follow_redirects=True)
        route.fulfill(status=response.status_code,headers={k:v for k,v in response.headers if k.lower() not in ('content-length','set-cookie')},body=response.data)

    def page(self,browser):
        page=browser.new_page();page.route('http://localhost/**',self.browser_route)
        page.goto('http://localhost'+self.url)
        page.get_by_role('button',name='New Quote',exact=True).click()
        return page

    def test_typed_code_and_description_resolve_saved_shop_item(self):
        self.fixtures()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=self.page(browser)
                search=page.locator('#quote-work-select')
                for typed in ('car-004','Install skirting','Skirting'):
                    search.fill('');search.press_sequentially(typed)
                    self.assertEqual(page.locator('#quote-work-code').input_value(),'CAR-004')
                    self.assertEqual(page.locator('#quote-work-material').input_value(),'85.00')
                    self.assertEqual(page.locator('#quote-work-labour').input_value(),'45.00')
                    self.assertFalse(page.get_by_role('button',name='Add Item',exact=True).is_disabled())
                page.locator('#quote-work-quantity').press_sequentially('2.5')
                self.assertEqual(page.locator('#quote-work-total').text_content(),'325.00')
                page.get_by_role('button',name='Add Item',exact=True).click()
                self.assertEqual(page.locator('.quote-line').count(),1)
            finally:browser.close()

    def test_late_normal_pageshow_does_not_erase_entered_values(self):
        self.fixtures()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=self.page(browser)
                page.locator('#quote-work-select').fill('CAR-004 \u2014 Install skirting')
                page.locator('#quote-work-quantity').fill('2.5')
                page.evaluate("window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:false}))")
                self.assertEqual(page.locator('#quote-work-quantity').input_value(),'2.5')
                page.get_by_role('button',name='Add Item',exact=True).click()
                page.locator(f'input[name="quantity_{self.item.id}"]').fill('3.75')
                page.evaluate("window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:false}))")
                self.assertEqual(page.locator('.quote-line').count(),1)
                self.assertEqual(page.locator(f'input[name="quantity_{self.item.id}"]').input_value(),'3.75')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'487.50')
            finally:browser.close()

    def test_tomas_register_quote_new_save_register_open_retains_values(self):
        self.fixtures();self.save_quote()
        original=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        original_identity=(original.id,original.quote_number,original.items,original.grand_total)
        door=base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',code='CAR-002',description='Install door frame',unit='Each',material_rate=Decimal('100'),labour_rate=Decimal('50'))
        self.db_session.add(door);self.db_session.commit()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost'+self.register)
                self.assertNotIn('Quotations and Invoices for Tomas',page.locator('body').text_content())
                self.assertEqual(page.get_by_role('button',name='New Quote',exact=True).count(),0)
                self.assertEqual(page.get_by_role('link',name=original.quote_number,exact=True).count(),0)
                page.locator('#shop-debtors tr').filter(has_text='Tomas').get_by_role('link',name='Quote',exact=True).click()
                page.get_by_role('button',name='New Quote',exact=True).click()
                self.assertEqual(page.locator('.quote-line').count(),0)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
                search=page.locator('#quote-work-select')
                search.press_sequentially('Install skirting')
                page.locator('#quote-work-quantity').press_sequentially('2.5')
                page.get_by_role('button',name='Add Item',exact=True).click()
                include=page.get_by_role('checkbox',name='Include CAR-004',exact=True)
                include.uncheck();self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
                include.check()
                self.assertTrue(page.locator(f'input[name="quantity_{self.item.id}"]').is_editable())
                page.locator(f'input[name="quantity_{self.item.id}"]').fill('3.75')
                search.fill('CAR-002');page.locator('#quote-work-quantity').fill('2')
                page.get_by_role('button',name='Add Item',exact=True).click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'787.50')
                page.get_by_role('button',name='Remove CAR-002',exact=True).click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'487.50')
                search.fill('Install door frame');page.locator('#quote-work-quantity').fill('2')
                page.get_by_role('button',name='Add Item',exact=True).click()
                page.get_by_role('button',name='Save Quote',exact=True).click()
                page.locator('#quote-area-form').wait_for(state='visible')
                saved=base.ProTradeBoqQuote.query.filter_by(debtor_id=self.tomas.id,is_draft=True).one()
                identity=(saved.id,saved.quote_number)
                expected={self.item.id:('3.75','85.00','45.00','487.50'),door.id:('2','100.00','50.00','300.00')}
                self.assertEqual({line['source_item_id']:(line['quantity'],line['material_rate'],line['labour_rate'],line['total']) for line in saved.items},expected)
                self.assertEqual(saved.grand_total,Decimal('787.50'))
                self.assertEqual((saved.user_id,saved.shop_id,saved.debtor_id),(1,self.carpentry.id,self.tomas.id))
                page.get_by_role('button',name='Register',exact=True).click()
                page.locator('#debtor-quote-register tr').filter(has_text=saved.quote_number).get_by_role('link',name='Open',exact=True).click()
                page.locator('#quote-area-form').wait_for(state='visible')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'787.50')
                for identifier,(quantity,material,labour,total) in expected.items():
                    row=page.locator(f'.quote-line[data-source-id="{identifier}"]')
                    self.assertEqual(row.locator('input[name^="quantity_"]').input_value(),quantity)
                    self.assertTrue(row.locator('input[type=checkbox]').is_checked())
                    self.assertEqual(row.locator('td').nth(5).locator('input').input_value(),material)
                    self.assertEqual(row.locator('td').nth(6).locator('input').input_value(),labour)
                    self.assertEqual(row.locator('.line-total').text_content(),total)
                self.assertEqual((saved.id,saved.quote_number),identity)
                self.assertEqual((original.id,original.quote_number,original.items,original.grand_total),original_identity)
                self.assertEqual(errors,[])
            finally:browser.close()

    def test_tomas_editable_rates_350_250_quantity_2_save_and_reopen(self):
        self.fixtures()
        prices=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost'+self.register)
                page.locator('#shop-debtors tr').filter(has_text='Tomas').get_by_role('link',name='Quote',exact=True).click()
                page.get_by_role('button',name='New Quote',exact=True).click()
                page.locator('#quote-work-select').fill('CAR-004')
                material=page.locator('#quote-work-material');labour=page.locator('#quote-work-labour');quantity=page.locator('#quote-work-quantity')
                self.assertEqual(material.input_value(),'85.00');self.assertEqual(labour.input_value(),'45.00')
                for field in (material,labour,quantity):
                    self.assertTrue(field.is_editable());self.assertEqual(field.get_attribute('type'),'number')
                material.fill('.5');labour.fill('.5');quantity.fill('.5')
                self.assertEqual(page.locator('#quote-work-total').text_content(),'0.50')
                material.fill('350');labour.fill('250');quantity.fill('2')
                self.assertEqual(page.locator('#quote-work-total').text_content(),'1200.00')
                quantity.fill('2.5');self.assertEqual(page.locator('#quote-work-total').text_content(),'1500.00')
                material.fill('375');self.assertEqual(page.locator('#quote-work-total').text_content(),'1562.50')
                material.fill('350');quantity.fill('2')
                self.assertEqual(labour.input_value(),'250')
                page.get_by_role('button',name='Add Item',exact=True).click()
                row=page.locator('.quote-line')
                material=row.locator('input[name^="material_rate_"]');labour=row.locator('input[name^="labour_rate_"]');quantity=row.locator('input[name^="quantity_"]')
                material.fill('351');self.assertEqual(page.locator('#quote-grand-total').text_content(),'1202.00')
                labour.fill('249');self.assertEqual(page.locator('#quote-grand-total').text_content(),'1200.00')
                quantity.fill('2.25');self.assertEqual(page.locator('#quote-grand-total').text_content(),'1350.00')
                material.fill('350');labour.fill('250');quantity.fill('2')
                include=page.get_by_role('checkbox',name='Include CAR-004',exact=True)
                include.uncheck()
                for field in (material,labour,quantity):self.assertTrue(field.is_editable())
                include.check();self.assertEqual(page.locator('#quote-grand-total').text_content(),'1200.00')
                page.get_by_role('button',name='Save Quote',exact=True).click()
                page.locator('#quote-area-form').wait_for(state='visible')
                saved=base.ProTradeBoqQuote.query.filter_by(debtor_id=self.tomas.id,is_draft=True).one()
                identity=(saved.id,saved.quote_number)
                self.assertEqual(saved.grand_total,Decimal('1200.00'))
                self.assertEqual((saved.items[0]['material_rate'],saved.items[0]['labour_rate'],saved.items[0]['quantity']),('350.00','250.00','2'))
                self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(),prices)
                page.get_by_role('button',name='Register',exact=True).click()
                page.locator('#debtor-quote-register tr').filter(has_text=saved.quote_number).get_by_role('link',name='Open',exact=True).click()
                page.locator('#quote-area-form').wait_for(state='visible')
                self.assertEqual(page.locator('input[name^="material_rate_"]').input_value(),'350.00')
                self.assertEqual(page.locator('input[name^="labour_rate_"]').input_value(),'250.00')
                self.assertEqual(page.locator('input[name^="quantity_"]').input_value(),'2')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'1200.00')
                page.get_by_role('button',name='Save Changes',exact=True).click()
                page.locator('#quote-area-form').wait_for(state='visible')
                self.assertEqual((saved.id,saved.quote_number),identity)
                self.assertEqual(saved.grand_total,Decimal('1200.00'))
                self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(),prices)
                self.assertEqual(errors,[])
            finally:browser.close()

    def test_rate_overrides_validate_and_never_change_price_list(self):
        self.fixtures()
        prices=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        payload={'item_id[]':[str(self.item.id)],f'quantity_{self.item.id}':'2',f'material_rate_{self.item.id}':'350',f'labour_rate_{self.item.id}':'250','quote_key':'0123456789abcdef0123456789abcdef'}
        self.assertEqual(self.client.post(self.url,data=payload).status_code,302)
        saved=base.ProTradeBoqQuote.query.filter_by(debtor_id=self.tomas.id).one()
        identity=(saved.id,saved.quote_number,saved.items,saved.grand_total)
        for kind in ('material_rate','labour_rate'):
            for value in ('','-1','NaN','Infinity','350.001','1000000000000'):
                data=dict(payload);data[f'{kind}_{self.item.id}']=value
                response=self.client.post(self.url,data=data)
                self.assertEqual(response.status_code,400)
                self.assertEqual((saved.id,saved.quote_number,saved.items,saved.grand_total),identity)
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(),prices)

    def test_saved_overrides_survive_price_list_changes_and_decimal_each_quantity(self):
        self.fixtures()
        each=base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',code='CAR-002',description='Install door frame',unit='Each',material_rate=100,labour_rate=50)
        self.db_session.add(each);self.db_session.commit()
        payload={'item_id[]':[str(each.id)],f'quantity_{each.id}':'1.5',f'material_rate_{each.id}':'350',f'labour_rate_{each.id}':'250','quote_key':'1123456789abcdef0123456789abcdef'}
        self.assertEqual(self.client.post(self.url,data=payload).status_code,302)
        saved=base.ProTradeBoqQuote.query.filter_by(debtor_id=self.tomas.id).one()
        self.assertEqual(saved.grand_total,Decimal('900.00'))
        each.material_rate=Decimal('999');each.labour_rate=Decimal('888');self.db_session.commit()
        response=self.client.get(self.url+f'?area=quote&quote_id={saved.id}')
        from lxml import html
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath(f'//input[@name="material_rate_{each.id}"]/@value'),['350.00'])
        self.assertEqual(tree.xpath(f'//input[@name="labour_rate_{each.id}"]/@value'),['250.00'])
        self.assertEqual(tree.xpath(f'//input[@name="quantity_{each.id}"]/@value'),['1.5'])
        # Legacy form submissions without rate fields keep the quote's saved overrides.
        payload.pop(f'material_rate_{each.id}');payload.pop(f'labour_rate_{each.id}')
        self.assertEqual(self.client.post(self.url,data=payload).status_code,302)
        self.assertEqual(saved.grand_total,Decimal('900.00'))
        self.assertEqual(each.material_rate,Decimal('999'));self.assertEqual(each.labour_rate,Decimal('888'))


if __name__=='__main__':unittest.main()
