"""Debtor-specific quotation hub and explicit quote editing on PostgreSQL/Chromium."""
from decimal import Decimal
from datetime import datetime
import json
import re
import unittest
from urllib.parse import urlsplit
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_debtor_foundation as foundation


def open_whole_quote(page, row):
    # Full-editor regressions use its explicit existing route. Register Edit now
    # opens saved items and is covered separately by the item-navigation tests.
    href=row.get_by_role('link',name='Edit',exact=True).get_attribute('href')
    match=re.search(r'/shop/(\d+)/debtor/\d+/quotes/(\d+)/items',href)
    assert match,href
    page.goto(f'http://localhost/mechanic/shop/{match[1]}/quotes/{match[2]}')


class CarpentryQuoteHubTests(unittest.TestCase):
    setUp=foundation.DebtorFoundationTests.setUp
    shop=foundation.DebtorFoundationTests.shop
    save_debtor=foundation.DebtorFoundationTests.save_debtor
    debtor=foundation.DebtorFoundationTests.debtor
    save_quote=foundation.DebtorFoundationTests.save_quote
    quote_url=foundation.DebtorFoundationTests.quote_url

    def fixtures(self):
        self.save_debtor();self.owner_debtor=self.debtor()
        self.url=self.quote_url(self.owner_debtor)
        self.save_quote();self.first=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        self.client.post(f'/mechanic/shop/{self.carpentry.id}/debtor/{self.owner_debtor.id}/quotes/new')
        self.save_quote(quantity='4');self.second_quote=base.ProTradeBoqQuote.query.filter_by(is_draft=True).one()
        client=base.models.MechClient(user_id=1,shop_id=self.carpentry.id,name='Different debtor')
        self.db_session.add(client);self.db_session.flush()
        self.other=base.models.Debtor(user_id=1,shop_id=self.carpentry.id,reference_id=client.id,name=client.name,slug_reference='protrade')
        self.db_session.add(self.other);self.db_session.flush()
        self.other_quote=base.ProTradeBoqQuote(user_id=1,shop_id=self.carpentry.id,debtor_id=self.other.id,client_id=client.id,trade='carpentry',quote_number='CAR-Q-OTHER-DEBTOR',client_name=client.name,items=[],grand_total=Decimal('999'))
        self.db_session.add(self.other_quote)
        self.foreign_quote=base.ProTradeBoqQuote(user_id=2,shop_id=self.foreign.id,trade='carpentry',quote_number='CAR-Q-FOREIGN',client_name='Foreign',items=[],grand_total=Decimal('777'))
        self.db_session.add(self.foreign_quote);self.db_session.commit()

    def browser_route(self,route):
        req=route.request;url=urlsplit(req.url)
        if url.path in ('/static/js/carpentry_quote_builder.js','/static/js/carpentry_quote_hub.js'):
            return route.fulfill(path=str(base.ROOT/'app'/url.path.removeprefix('/')),content_type='application/javascript')
        response=self.client.open(url.path+('?' +url.query if url.query else ''),method=req.method,data=req.post_data,
                                  content_type=req.headers.get('content-type'),follow_redirects=True)
        route.fulfill(status=response.status_code,headers={k:v for k,v in response.headers if k.lower() not in ('set-cookie','content-length')},body=response.data)

    def test_register_is_debtor_scoped_and_dates_use_south_africa(self):
        self.fixtures()
        self.first.created_at=datetime(2026,11,12,23,30);self.db_session.commit()
        before=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all()
        tree=html.fromstring(self.client.get(self.url+'?area=register').data)
        self.assertEqual(tree.xpath('//nav[@id="quote-hub-navigation"]/button/text()'),['New Quote','Register'])
        self.assertNotIn('Quote / Invoice Register',tree.text_content())
        register=tree.get_element_by_id('debtor-quote-register')
        self.assertEqual(register.xpath('./thead/tr/th/text()'),['Quote No.','Date','Amount','Status','Action'])
        self.assertEqual(set(register.xpath('./tbody/tr/td[1]/a/text()')),{self.first.quote_number,self.second_quote.quote_number})
        self.assertNotIn('CAR-Q-OTHER-DEBTOR',register.text_content());self.assertNotIn('CAR-Q-FOREIGN',register.text_content())
        self.assertNotIn('LEGACY-CAR',register.text_content());self.assertIn('2026-11-13',register.text_content())
        for link in register.xpath('.//td[last()]/a'):
            self.assertIn(f'/debtor/{self.owner_debtor.id}/quotes/',link.get('href'))
            self.assertTrue(link.get('href').endswith('/items'))
        for link in register.xpath('.//td[1]/a'):
            self.assertIn(f'/shop/{self.carpentry.id}/debtor/{self.owner_debtor.id}/quotes/',link.get('href'))
            self.assertTrue(link.get('href').endswith('/items'))
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all(),before)

    def test_explicit_open_enforces_debtor_shop_owner_and_preserves_quote_number(self):
        self.fixtures()
        for identifier in (self.other_quote.id,self.foreign_quote.id,999999):
            self.assertEqual(self.client.get(self.url+f'?area=quote&quote_id={identifier}').status_code,404)
            self.assertEqual(self.client.post(self.url,data={'area':'quote','quote_id':identifier}).status_code,404)
        self.assertEqual(self.client.get(self.url+'?quote_id=bad').status_code,404)
        redirect=self.client.get(self.url+f'?area=quote&quote_id={self.first.id}')
        self.assertEqual(redirect.status_code,302)
        self.assertTrue(redirect.location.endswith(f'/quotes/{self.first.id}/items'))
        response=self.client.get(f'/mechanic/shop/{self.carpentry.id}/quotes/{self.first.id}')
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//input[@name="quantity_'+str(self.item.id)+'"]/@value'),['2.5'])
        self.assertEqual(tree.xpath('//input[@name="quote_id"]/@value'),[str(self.first.id)])
        wrong=self.client.post(self.url,data={'area':'quote','quote_id':self.first.id,'quote_key':self.second_quote.quote_number.removeprefix('CAR-Q-'),'item_id[]':[self.item.id],f'quantity_{self.item.id}':'3'})
        self.assertEqual(wrong.status_code,400)
        self.assertEqual(self.first.grand_total,Decimal('325.00'))

    def test_browser_hub_close_scoped_open_edit_and_blank_new_with_fresh_rates(self):
        self.fixtures()
        first_identity=(self.first.id,self.first.quote_number)
        second_identity=(self.second_quote.id,self.second_quote.quote_number)
        self.item.material_rate=Decimal('999');self.db_session.commit()
        count=base.ProTradeBoqQuote.query.count()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost'+self.url)
                self.assertEqual(page.locator('.quote-working-area:visible').count(),0)
                self.assertTrue(page.get_by_role('link',name='Back',exact=True).is_visible())
                for name in ('New Quote','Register'):self.assertTrue(page.get_by_role('button',name=name,exact=True).is_visible())
                self.assertEqual(page.get_by_role('button',name='Debtor',exact=True).count(),0)
                page.get_by_role('button',name='Register',exact=True).click()
                row=page.locator('#debtor-quote-register tr').filter(has_text=self.first.quote_number)
                open_whole_quote(page,row)
                page.locator('#quote-area-form').wait_for(state='visible')
                self.assertEqual(page.locator('.quote-working-area:visible').count(),1)
                self.assertFalse(page.locator('#quote-area-register').is_visible())
                self.assertEqual(page.locator('input[name=quote_id]').input_value(),str(self.first.id))
                self.assertEqual(page.locator(f'input[name="quantity_{self.item.id}"]').input_value(),'2.5')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'325.00')
                page.locator(f'input[name="quantity_{self.item.id}"]').fill('3')
                page.get_by_role('button',name='Save Changes',exact=True).click()
                page.locator('#quote-area-register').wait_for(state='visible')
                self.assertFalse(page.locator('#quote-area-form').is_visible())
                self.assertEqual(self.first.grand_total,Decimal('390.00'))
                self.assertFalse(self.first.is_draft);self.assertTrue(self.second_quote.is_draft)
                self.assertEqual(self.second_quote.grand_total,Decimal('520.00'))
                self.assertEqual((self.first.id,self.first.quote_number),first_identity)
                self.assertEqual((self.second_quote.id,self.second_quote.quote_number),second_identity)
                page.get_by_role('button',name='New Quote',exact=True).click()
                self.assertEqual(page.locator('.quote-working-area:visible').count(),1)
                self.assertEqual(page.locator('.quote-line').count(),0)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
                self.assertEqual(page.locator('#quote-work-select').input_value(),'')
                self.assertEqual(page.locator('#quote-work-quantity').input_value(),'')
                self.assertEqual(page.locator('#quote-work-material').input_value(),'')
                self.assertTrue(page.locator('input[name=quote_id]').is_disabled())
                self.assertEqual(page.locator('#quote-boq-items th').all_text_contents(),['Include','Item Code','Description','Unit','Quantity','Material R/unit','Labour R/unit','Total R'])
                page.get_by_role('button',name='Close',exact=True).click()
                self.assertEqual(base.ProTradeBoqQuote.query.count(),count)
                self.assertTrue(self.second_quote.is_draft)
                page.get_by_role('button',name='New Quote',exact=True).click()
                page.locator('#quote-work-select').fill('CAR-004 \u2014 Install skirting')
                self.assertEqual(page.locator('#quote-work-material').input_value(),'999.00')
                page.locator('#quote-work-quantity').fill('1');page.get_by_role('button',name='Add Item',exact=True).click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'1044.00')
                page.get_by_role('checkbox',name='Include CAR-004',exact=True).uncheck()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'0.00')
                self.assertTrue(page.locator('#quote-save').is_disabled())
                page.get_by_role('checkbox',name='Include CAR-004',exact=True).check()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'1044.00')
                page.get_by_role('button',name='Save Quote',exact=True).click()
                page.locator('#quote-area-form').wait_for(state='visible')
                self.assertEqual(base.ProTradeBoqQuote.query.count(),count+1)
                self.assertFalse(self.second_quote.is_draft)
                newest=base.ProTradeBoqQuote.query.filter_by(debtor_id=self.owner_debtor.id,is_draft=True).one()
                self.assertEqual(newest.grand_total,Decimal('1044.00'))
                self.assertEqual(page.locator('#quote-hub-navigation').get_attribute('data-debtor-id'),str(self.owner_debtor.id))
                page.get_by_role('button',name='Register',exact=True).click()
                self.assertEqual(page.locator('.quote-working-area:visible').count(),1)
                self.assertEqual(page.locator('#debtor-quote-register tbody tr').count(),3)
            finally:browser.close()


    def test_tomas_edit_1149_reopen_keeps_identity_and_single_working_area(self):
        self.fixtures()
        self.owner_debtor.name='Tomas'
        self.first.items=[dict(source_item_id='template:CAR-003',code='CAR-003',description='Install door lock',unit='Each',quantity='3',material_rate='259.00',labour_rate='124.00',total='1149.00')]
        self.first.grand_total=Decimal('1149');self.db_session.commit()
        identity=(self.first.id,self.first.quote_number);count=base.ProTradeBoqQuote.query.count()
        # Model a stylesheet which overrides the browser's default hidden rule.
        self.app.jinja_loader.loaders[0].mapping['layout.html']='<style>section { display:block; }</style>{% block head %}{% endblock %}{% block content %}{% endblock %}'
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost'+self.url+'?area=register')
                row=page.locator('#debtor-quote-register tr').filter(has_text=self.first.quote_number)
                self.assertIn('1149.00',row.text_content())
                open_whole_quote(page,row)
                self.assertEqual(page.locator('.quote-working-area:visible').count(),1)
                self.assertFalse(page.locator('#quote-area-register').is_visible())
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'1149.00')
                quantity=page.locator('input[name="quantity_template:CAR-003"]')
                self.assertEqual(quantity.input_value(),'3')
                self.assertEqual(page.locator('input[name="material_rate_template:CAR-003"]').input_value(),'259.00')
                self.assertEqual(page.locator('input[name="labour_rate_template:CAR-003"]').input_value(),'124.00')
                quantity.fill('2')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'766.00')
                page.get_by_role('button',name='Save Changes',exact=True).click()
                page.locator('#quote-area-register').wait_for(state='visible')
                self.assertFalse(page.locator('#quote-area-form').is_visible())
                page.get_by_role('button',name='Register',exact=True).click()
                row=page.locator('#debtor-quote-register tr').filter(has_text=self.first.quote_number)
                self.assertIn('766.00',row.text_content())
                open_whole_quote(page,row)
                self.assertEqual(page.locator('input[name="quantity_template:CAR-003"]').input_value(),'2')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'766.00')
                page.get_by_role('button',name='Close',exact=True).click()
                self.assertEqual(page.locator('.quote-working-area:visible').count(),0)
                self.assertEqual((self.first.id,self.first.quote_number),identity)
                self.assertEqual(base.ProTradeBoqQuote.query.count(),count)
                self.assertEqual(self.second_quote.grand_total,Decimal('520'))
            finally:browser.close()

    def test_tomas_q0006_all_snapshot_lines_edit_add_remove_and_return_register(self):
        self.fixtures();self.owner_debtor.name='Tomas'
        self.first.quotation_seq=6;self.second_quote.quotation_seq=5
        self.first.items=[
            dict(source_item_id='template:CAR-003',code='CAR-003',description='Install door lock',unit='Each',quantity='1',material_rate='259.00',labour_rate='60.00',total='319.00'),
            dict(source_item_id=str(self.item.id),code='CAR-004',description='Agreed skirting description',unit='m',quantity='2',material_rate='200.00',labour_rate='100.00',total='600.00'),
            dict(source_item_id='template:CAR-OLD',code='CAR-OLD',description='Historical custom work',unit='Hour',quantity='1',material_rate='100.00',labour_rate='130.00',total='230.00')]
        self.first.grand_total=Decimal('1149');self.db_session.commit()
        identity=(self.first.id,self.first.quote_number,self.first.quotation_seq)
        other=(self.second_quote.id,self.second_quote.quote_number,self.second_quote.quotation_seq,self.second_quote.items,self.second_quote.grand_total)
        count=base.ProTradeBoqQuote.query.count()
        prices=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost'+f'/mechanic/shop/{self.carpentry.id}/debtors')
                page.locator('#shop-debtors tr').filter(has_text='Tomas').get_by_role('link',name='Quote',exact=True).click()
                page.get_by_role('button',name='Register',exact=True).click()
                row=page.locator('#debtor-quote-register tr').filter(has_text='Q-0006')
                self.assertIn('1149.00',row.text_content())
                self.assertIn('bg-orange-100',row.get_by_role('link',name='Edit',exact=True).get_attribute('class'))
                open_whole_quote(page,row)
                self.assertTrue(page.get_by_role('heading',name='Edit Quotation',exact=True).is_visible())
                self.assertEqual(page.locator('.quote-working-area:visible').count(),1)
                self.assertEqual(page.locator('.quote-line').count(),3)
                for index,line in enumerate(self.first.items):
                    saved_row=page.locator('.quote-line').nth(index)
                    self.assertIn(line['description'],saved_row.text_content())
                    self.assertIn(line['code'],saved_row.text_content());self.assertIn(line['unit'],saved_row.text_content())
                    for field in ('quantity','material_rate','labour_rate'):
                        self.assertEqual(saved_row.locator('input[name^="'+field+'_"]').input_value(),line[field])
                    self.assertEqual(saved_row.locator('.line-total').text_content(),line['total'])
                page.get_by_role('spinbutton',name='Quantity CAR-003',exact=True).fill('3')
                self.assertEqual(page.locator('.quote-line').nth(0).locator('.line-total').text_content(),'957.00')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'1787.00')
                page.get_by_role('button',name='Remove CAR-OLD',exact=True).click()
                page.locator('#quote-work-select').fill('CAR-001')
                page.locator('#quote-work-material').fill('50');page.locator('#quote-work-labour').fill('10');page.locator('#quote-work-quantity').fill('2')
                page.get_by_role('button',name='Add Item',exact=True).click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'1677.00')
                page.get_by_role('button',name='Save Changes',exact=True).click()
                page.locator('#quote-area-register').wait_for(state='visible')
                self.assertFalse(page.locator('#quote-area-form').is_visible())
                self.assertEqual(page.locator('.quote-working-area:visible').count(),1)
                row=page.locator('#debtor-quote-register tr').filter(has_text='Q-0006')
                self.assertIn('1677.00',row.text_content())
                open_whole_quote(page,row)
                self.assertEqual(page.get_by_role('spinbutton',name='Quantity CAR-003',exact=True).input_value(),'3')
                self.assertEqual(page.get_by_role('spinbutton',name='Material Rate CAR-004',exact=True).input_value(),'200.00')
                self.assertEqual(page.locator('.quote-line').count(),3)
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'1677.00')
                self.assertNotIn('CAR-OLD',page.locator('#quote-item-lines').text_content())
                self.assertEqual(errors,[])
                self.assertEqual((self.first.id,self.first.quote_number,self.first.quotation_seq),identity)
                self.assertEqual((self.second_quote.id,self.second_quote.quote_number,self.second_quote.quotation_seq,self.second_quote.items,self.second_quote.grand_total),other)
                self.assertEqual(base.ProTradeBoqQuote.query.count(),count)
                self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(),prices)
            finally:browser.close()


if __name__=='__main__':unittest.main()
