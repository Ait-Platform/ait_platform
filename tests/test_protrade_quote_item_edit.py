"""Focused saved quotation item updates on PostgreSQL and Chromium."""
from copy import deepcopy
from decimal import Decimal
import unittest
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_carpentry_quote_hub as hub

class QuoteItemEditTests(unittest.TestCase):
    setUp=hub.CarpentryQuoteHubTests.setUp
    shop=hub.CarpentryQuoteHubTests.shop
    save_debtor=hub.CarpentryQuoteHubTests.save_debtor
    debtor=hub.CarpentryQuoteHubTests.debtor
    save_quote=hub.CarpentryQuoteHubTests.save_quote
    quote_url=hub.CarpentryQuoteHubTests.quote_url
    browser_route=hub.CarpentryQuoteHubTests.browser_route

    def fixtures(self):
        hub.CarpentryQuoteHubTests.fixtures(self)
        self.owner_debtor.name='Tomas';self.first.quotation_seq=6;self.second_quote.quotation_seq=5
        self.first.items=[dict(source_item_id='template:CAR-003',code='CAR-003',description='Install door lock',unit='Each',quantity='1',material_rate='259.00',labour_rate='65.00',total='324.00'),dict(source_item_id='template:CAR-005',code='CAR-005',description='Install cornice',unit='m',quantity='3',material_rate='250.00',labour_rate='25.00',total='825.00')]
        self.first.grand_total=Decimal('1149');self.db_session.commit()
        self.items_url=f'/mechanic/shop/{self.carpentry.id}/debtor/{self.owner_debtor.id}/quotes/{self.first.id}/items'

    def payload(self,index=0,**changes):
        tree=html.fromstring(self.client.get(self.items_url+f'?item_id={self.ns["carpentry_quote_item_id"](self.first,self.first.items[index])}').data)
        data={node.name:node.value for node in tree.xpath('//form//input[@name]')}
        data['unit']=tree.xpath('//select[@name="unit"]/option[@selected]/text()')[0]
        data.update(changes);return data

    def test_tomas_exact_1473_workflow_and_cancel(self):
        self.fixtures();identity=(self.first.id,self.first.quote_number,self.first.quotation_seq)
        cornice=deepcopy(self.first.items[1]);other=deepcopy(self.second_quote.items)
        count=base.ProTradeBoqQuote.query.count()
        prices=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all()
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                page.goto('http://localhost'+self.url+'?area=register')
                page.get_by_role('link',name='Items Q-0006',exact=True).click()
                row=page.locator('.saved-quote-items:visible tr').filter(has_text='CAR-003')
                row.get_by_role('link',name='Edit',exact=True).click()
                page.locator('#quote-item-inline-row #quote-item-edit').wait_for()
                self.assertTrue(page.get_by_role('heading',name='Edit Quote Item \u2014 CAR-003',exact=True).is_visible())
                self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Item Code',exact=True).input_value(),'CAR-003')
                self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Description',exact=True).input_value(),'Install door lock')
                self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Unit',exact=True).input_value(),'Each')
                self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Material Rate',exact=True).input_value(),'259.00')
                self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Labour Rate',exact=True).input_value(),'65.00')
                page.locator('#quote-item-inline-row').get_by_label('Quantity',exact=True).fill('2')
                page.locator('#quote-item-inline-row').get_by_role('button',name='Save Changes',exact=True).click()
                page.locator('#quote-item-inline-row').wait_for(state='detached')
                page.locator('.saved-quote-items:visible').wait_for()
                row=page.locator('.saved-quote-items:visible tr').filter(has_text='CAR-003')
                self.assertEqual(row.locator('td').nth(3).text_content(),'2')
                self.assertEqual(row.locator('td').nth(6).text_content(),'R648.00')
                self.assertEqual(page.locator('.saved-quote-items:visible tr').filter(has_text='CAR-005').locator('td').nth(6).text_content(),'R825.00')
                self.assertEqual(page.locator('.quote-detail-row:visible [id^="saved-quote-grand-total-"]').text_content(),'1473.00')
                row.get_by_role('link',name='Edit',exact=True).click()
                page.locator('#quote-item-inline-row #quote-item-edit').wait_for()
                self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Quantity',exact=True).input_value(),'2')
                page.locator('#quote-item-inline-row').get_by_label('Quantity',exact=True).fill('9')
                page.locator('#quote-item-inline-row').get_by_role('link',name='Cancel',exact=True).click()
                self.assertEqual(page.locator('.quote-detail-row:visible [id^="saved-quote-grand-total-"]').text_content(),'1473.00')
                self.assertEqual(self.first.items[0]['quantity'],'2')
                self.assertEqual(self.first.items[1],cornice)
                self.assertEqual(self.second_quote.items,other)
                self.assertEqual((self.first.id,self.first.quote_number,self.first.quotation_seq),identity)
                self.assertEqual(base.ProTradeBoqQuote.query.count(),count)
                self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_items ORDER BY id').all(),prices)
            finally:browser.close()

    def test_validation_stale_submission_and_only_deliberate_metadata_changes(self):
        self.fixtures();original=deepcopy(self.first.items)
        for changes in ({'quantity':'0'},{'quantity':'NaN'},{'quantity':'1.0001'},{'material_rate':'-1'},{'labour_rate':'Infinity'},{'description':''},{'unit':'unknown'}):
            self.assertEqual(self.client.post(self.items_url,data=self.payload(**changes)).status_code,400)
            self.assertEqual(self.first.items,original)
        data=self.payload(quantity='2')
        self.assertEqual(self.client.post(self.items_url,data=data).status_code,302)
        self.assertEqual(self.client.post(self.items_url,data=data).status_code,409)
        self.assertEqual(self.first.grand_total,Decimal('1473'))
        data=self.payload(quantity='2.5',description='Agreed lock repair',unit='m\u00b2',material_rate='260.00')
        self.assertEqual(self.client.post(self.items_url,data=data).status_code,302)
        self.assertEqual(self.first.items[0]['description'],'Agreed lock repair')
        self.assertEqual(self.first.items[0]['unit'],'m\u00b2')
        self.assertEqual(self.first.items[0]['total'],'812.50')
        self.assertEqual(self.first.items[1],original[1])

    def test_item_and_quote_owner_shop_debtor_scope(self):
        self.fixtures();before=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all()
        for suffix in ('?item=-1','?item=2','?item=bad'):
            self.assertEqual(self.client.get(self.items_url+suffix).status_code,404)
        wrongs=[self.items_url.replace(f'/debtor/{self.owner_debtor.id}/',f'/debtor/{self.other.id}/'),self.items_url.replace(f'/shop/{self.carpentry.id}/',f'/shop/{self.second.id}/'),self.items_url.replace(f'/quotes/{self.first.id}/',f'/quotes/{self.foreign_quote.id}/')]
        for url in wrongs:
            self.assertEqual(self.client.get(url).status_code,404)
            self.assertEqual(self.client.post(url,data={'item':'0'}).status_code,404)
        with self.client.session_transaction() as session:session['_user_id']='2'
        self.assertEqual(self.client.get(self.items_url).status_code,404)
        with self.client.session_transaction() as session:session.clear()
        self.assertIn(self.client.get(self.items_url).status_code,(302,401))
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all(),before)

    def test_item_id_stays_with_saved_line_after_reordering(self):
        self.fixtures();data=self.payload(quantity='2')
        item_id=data['item_id'];cornice=deepcopy(self.first.items[1])
        self.first.items=list(reversed(self.first.items));self.db_session.commit()
        self.assertEqual(self.client.post(self.items_url,data=data).status_code,302)
        self.assertEqual(self.first.items[0],cornice)
        self.assertEqual(self.first.items[1]['quote_item_id'],item_id)
        self.assertEqual(self.first.items[1]['quantity'],'2')
        self.assertEqual(self.first.grand_total,Decimal('1473'))
        self.assertEqual(self.client.get(self.items_url+'?item_id=not-this-item').status_code,404)

    def test_exact_old_url_and_workflow_over_real_http_with_no_side_effects(self):
        from uuid import uuid4
        from threading import Thread
        from werkzeug.serving import make_server
        from unittest.mock import patch
        from test_protrade_trade_quotes import TokenTransaction
        self.fixtures()
        shop=base.models.MechShop(id=6,user_id=1,business_name='Happy Shop',trade='carpentry',onboarding_status='active')
        self.db_session.add(shop);self.db_session.flush()
        client=base.models.MechClient(id=15,user_id=1,shop_id=6,name='Tomas')
        self.db_session.add(client);self.db_session.flush()
        debtor=base.models.Debtor(id=15,user_id=1,shop_id=6,reference_id=15,name='Tomas',slug_reference='protrade')
        self.db_session.add(debtor);self.db_session.flush()
        starting=deepcopy(self.first.items);starting[0].update(quantity='2',total='648.00')
        quote=base.ProTradeBoqQuote(id=6,user_id=1,shop_id=6,debtor_id=15,client_id=15,trade='carpentry',quote_number='CAR-Q-'+uuid4().hex.upper(),quotation_seq=6,client_name='Tomas',items=starting,grand_total=Decimal('1473'),is_draft=True)
        wallet=base.models.AitTokenWallet(user_id=1,balance=50)
        previous=base.ProTradeBoqQuote(id=7,user_id=1,shop_id=6,debtor_id=15,client_id=15,trade='carpentry',quote_number='CAR-Q-'+uuid4().hex.upper(),quotation_seq=5,client_name='Tomas',items=[deepcopy(self.first.items[1])],grand_total=Decimal('825'),is_draft=False)
        self.db_session.add_all([quote,previous,wallet]);self.db_session.flush()
        self.db_session.add(TokenTransaction(wallet_id=wallet.id,amount=-3,description='Existing transaction'))
        self.db_session.commit()
        tables=('ait_token_wallets','quote_test_token_transactions','mech_invoices','quote_test_invites','protrade_output_email','protrade_boq_items')
        before={table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in tables}
        identity=(quote.id,quote.quote_number,quote.quotation_seq)
        cornice=deepcopy(quote.items[1]);count=base.ProTradeBoqQuote.query.count()
        item_id=self.ns['carpentry_quote_item_id'](quote,quote.items[0])
        self.app.static_folder=str(base.ROOT/'app/static')
        server=make_server('127.0.0.1',0,self.app)
        def serve():
            try:server.serve_forever(poll_interval=0.1)
            finally:self.db_session.remove()
        thread=Thread(target=serve,daemon=True);thread.start()
        origin=f'http://127.0.0.1:{server.server_port}'
        old='/mechanic/shop/6/debtor/15/quote?area=quote&quote_id=6'
        canonical='/mechanic/shop/6/debtor/15/quote?area=register&expanded_quote_id=6'
        try:
            with patch('flask_mail.Mail.send',side_effect=AssertionError('Item edit must not send mail')),patch('smtplib.SMTP',side_effect=AssertionError('Item edit must not connect to SMTP')),patch('smtplib.SMTP_SSL',side_effect=AssertionError('Item edit must not connect to SMTP')),sync_playwright() as playwright:
                browser=playwright.chromium.launch(headless=True)
                try:
                    context=browser.new_context(viewport={'width':1400,'height':1000})
                    context.add_cookies([{'name':'session','value':self.client.get_cookie('session').value,'url':origin}])
                    page=context.new_page();errors=[];requests=[]
                    page.on('pageerror',lambda error:errors.append(str(error)))
                    page.on('request',lambda request:requests.append(request.url))
                    page.goto(origin+old)
                    self.assertEqual(page.url,origin+canonical)
                    self.assertFalse(page.locator('#quote-work-select').is_visible())
                    page.goto(origin+'/mechanic/shop/6/debtors')
                    page.locator('#shop-debtors tr').filter(has_text='Tomas').get_by_role('link',name='Quote',exact=True).click()
                    page.get_by_role('button',name='Register',exact=True).click()
                    # The prominent Edit action must also open the item register.
                    page.locator('#debtor-quote-register tr').filter(has_text='Q-0006').get_by_role('link',name='Edit',exact=True).click()
                    self.assertEqual(page.url,origin+canonical)
                    self.assertEqual(page.locator('.quote-detail-row:visible').count(),1)
                    self.assertEqual(page.locator('[data-quote-summary="6"]').evaluate('(node)=>node.nextElementSibling.id'),'quote-details-6')
                    page.locator('[data-quote-summary="7"]').get_by_role('link',name='Edit',exact=True).click()
                    self.assertTrue(page.locator('#quote-details-7').is_visible())
                    self.assertFalse(page.locator('#quote-details-6').is_visible())
                    self.assertEqual(page.locator('.quote-detail-row:visible').count(),1)
                    page.locator('[data-quote-summary="6"]').get_by_role('link',name='Edit',exact=True).click()
                    page.evaluate('window.inlineDocumentProof = 42')
                    row=page.locator('.saved-quote-items:visible tr').filter(has_text='CAR-003')
                    row.get_by_role('link',name='Edit',exact=True).click()
                    page.locator('#quote-item-inline-row #quote-item-edit').wait_for()
                    self.assertEqual(page.url,origin+canonical)
                    self.assertTrue(page.get_by_role('heading',name='Edit Quote Item \u2014 CAR-003',exact=True).is_visible())
                    self.assertFalse(page.locator('#quote-work-select').is_visible())
                    self.assertFalse(page.locator('#carpentry-quote-form').is_visible())
                    self.assertNotIn('CAR-005',page.locator('#quote-item-inline-row').text_content())
                    self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Quantity',exact=True).input_value(),'2')
                    self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Material Rate',exact=True).input_value(),'259.00')
                    self.assertEqual(page.locator('#quote-item-inline-row').get_by_label('Labour Rate',exact=True).input_value(),'65.00')
                    self.assertEqual(page.locator('#quote-item-line-total').text_content(),'648.00')
                    page.screenshot(path=str(base.ROOT/'tmp/quote-item-actual-route-edit.png'),full_page=True)
                    self.assertEqual(page.locator('.quote-inline-editor').count(),1)
                    self.assertEqual(page.locator('#saved-quote-items-6 [data-quote-item-row]').first.evaluate('(node)=>node.nextElementSibling.id'),'quote-item-inline-row')
                    # Switching item discards the first form, leaving exactly one editor.
                    page.locator('#saved-quote-items-6 tr').filter(has_text='CAR-005').get_by_role('link',name='Edit',exact=True).click()
                    page.locator('#quote-item-inline-row').get_by_role('heading',name='Edit Quote Item \u2014 CAR-005',exact=True).wait_for()
                    self.assertEqual(page.locator('.quote-inline-editor').count(),1)
                    page.locator('#quote-item-inline-row').get_by_role('link',name='Cancel',exact=True).click()
                    self.assertTrue(page.locator('#quote-details-6').is_visible())
                    row.get_by_role('link',name='Edit',exact=True).click()
                    form=page.locator('#quote-item-inline-row #quote-item-edit');form.wait_for()
                    # Native validation keeps the form open without changing the saved rows.
                    form.get_by_label('Quantity',exact=True).fill('0')
                    form.get_by_role('button',name='Save Changes',exact=True).click()
                    self.assertTrue(form.is_visible())
                    form.get_by_label('Quantity',exact=True).fill('2')
                    # A server rejection also stays inline and retains the entered value.
                    form.get_by_label('Material Rate',exact=True).evaluate('(node)=>node.step="any"')
                    form.get_by_label('Material Rate',exact=True).fill('259.001')
                    form.get_by_role('button',name='Save Changes',exact=True).click()
                    form.get_by_role('alert').wait_for()
                    self.assertEqual(form.get_by_label('Material Rate',exact=True).input_value(),'259.001')
                    self.assertEqual(page.locator('#saved-quote-grand-total-6').text_content(),'1473.00')
                    self.assertEqual(page.evaluate('window.inlineDocumentProof'),42)
                    form.get_by_label('Material Rate',exact=True).fill('259')
                    form.get_by_label('Quantity',exact=True).fill('3')
                    self.assertEqual(form.locator('#quote-item-line-total').text_content(),'972.00')
                    form.get_by_role('button',name='Save Changes',exact=True).click()
                    page.locator('#quote-item-inline-row').wait_for(state='detached')
                    self.assertEqual(page.evaluate('window.inlineDocumentProof'),42)
                    self.assertEqual(page.url,origin+canonical)
                    self.assertEqual(page.locator('.quote-detail-row:visible [id^="saved-quote-grand-total-"]').text_content(),'1797.00')
                    self.assertEqual(page.locator('[data-quote-summary="6"] > td').nth(2).text_content(),'R1797.00')
                    row=page.locator('.saved-quote-items:visible tr').filter(has_text='CAR-003')
                    self.assertEqual(row.locator('td').nth(3).text_content(),'3')
                    self.assertEqual(row.locator('td').nth(6).text_content(),'R972.00')
                    self.assertEqual(page.locator('.saved-quote-items:visible tr').filter(has_text='CAR-005').locator('td').nth(6).text_content(),'R825.00')
                    page.screenshot(path=str(base.ROOT/'tmp/quote-item-actual-route-register.png'),full_page=True)
                    row.get_by_role('link',name='Edit',exact=True).click()
                    reopened=page.locator('#quote-item-inline-row #quote-item-edit');reopened.wait_for()
                    self.assertEqual(reopened.get_by_label('Quantity',exact=True).input_value(),'3')
                    self.assertEqual(reopened.locator('#quote-item-line-total').text_content(),'972.00')
                    reopened.get_by_label('Quantity',exact=True).fill('9')
                    reopened.get_by_role('link',name='Cancel',exact=True).click()
                    self.assertEqual(page.locator('.quote-inline-editor').count(),0)
                    self.assertEqual(page.evaluate('window.inlineDocumentProof'),42)
                    self.assertEqual(page.url,origin+canonical)
                    self.assertEqual(page.locator('#saved-quote-grand-total-6').text_content(),'1797.00')
                    page.locator('[data-quote-summary="7"]').get_by_role('link',name='Edit',exact=True).click()
                    self.assertFalse(page.locator('#quote-details-6').is_visible())
                    page.locator('[data-quote-summary="6"]').get_by_role('link',name='Edit',exact=True).click()
                    self.assertTrue(page.locator('#quote-details-6').is_visible())
                    # Direct old URL plus the explicit saved item ID must open only that item.
                    page.goto(origin+old+'&item_id='+item_id)
                    self.assertEqual(page.get_by_label('Quantity',exact=True).input_value(),'3')
                    self.assertEqual(page.locator('#quote-item-line-total').text_content(),'972.00')
                    page.get_by_label('Quantity',exact=True).fill('9')
                    page.get_by_role('link',name='Cancel',exact=True).click()
                    self.assertEqual(page.url,origin+canonical)
                    self.assertTrue(page.locator('#quote-details-6').is_visible())
                    self.assertEqual(page.locator('.quote-detail-row:visible').count(),1)
                    self.assertEqual(page.locator('#saved-quote-items-6 tr').filter(has_text='CAR-003').locator('td').nth(3).text_content(),'3')
                    page.reload()
                    self.assertTrue(page.locator('#quote-details-6').is_visible())
                    self.assertEqual(errors,[])
                    self.assertFalse(any('/send' in url or '/invoice' in url or '/email' in url for url in requests))
                finally:browser.close()
        finally:server.shutdown();thread.join(timeout=5);server.server_close()
        self.db_session.expire_all()
        self.assertEqual((quote.id,quote.quote_number,quote.quotation_seq),identity)
        self.assertEqual(quote.items[0]['quantity'],'3');self.assertEqual(quote.items[1],cornice)
        self.assertEqual(base.ProTradeBoqQuote.query.count(),count)
        self.assertEqual({table:self.conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY id').all() for table in tables},before)

    def test_expansion_is_scoped_read_only_and_legacy_register_redirects(self):
        self.fixtures()
        before=self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all()
        for identifier in (self.other_quote.id,self.foreign_quote.id,999999,'bad'):
            response=self.client.get(self.url+f'?area=register&expanded_quote_id={identifier}')
            self.assertEqual(response.status_code,404)
        response=self.client.get(self.items_url,follow_redirects=True)
        self.assertEqual(response.status_code,200)
        tree=html.fromstring(response.data)
        details=tree.get_element_by_id('quote-details-'+str(self.first.id))
        self.assertNotIn('hidden',details.attrib)
        self.assertEqual(len(tree.xpath('//tr[@data-quote-details and not(@hidden)]')),1)
        self.assertTrue(tree.xpath('//table[@id="debtor-quote-register"]'))
        self.assertFalse(tree.xpath('//h1[text()="Quotation Items"]'))
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM protrade_boq_quotes ORDER BY id').all(),before)

if __name__=='__main__':unittest.main()
