"""BOQ reads owned existing Carpentry prices without changing their source."""
import json
from decimal import Decimal
from uuid import uuid4
import unittest
from lxml import html
import test_protrade_boq_trade as boq
import test_protrade_shop_register as base


class BoqCatalogueTests(unittest.TestCase):
    setUp = boq.BoqTradeTests.setUp
    shop = boq.BoqTradeTests.shop
    save_debtor = boq.BoqTradeTests.save_debtor
    debtor = boq.BoqTradeTests.debtor
    save_quote = boq.BoqTradeTests.save_quote
    quote_url = boq.BoqTradeTests.quote_url
    ledger_url = boq.BoqTradeTests.ledger_url
    fixtures = boq.BoqTradeTests.fixtures

    def test_carpentry_and_plumbing_share_one_boq_without_copying_catalogues(self):
        shop,debtor=self.fixtures()
        plumbing=self.shop(name='Plumbing price list',trade='plumbing')
        part=base.MechCatalogPart(user_id=1,shop_id=plumbing.id,trade='plumbing',part_name='Repair leaking pipe',default_price=80)
        wood=base.ProTradeBoqItem(user_id=1,shop_id=self.carpentry.id,trade='carpentry',code='CAR-TEST',description='Timber work',unit='Each',material_rate=20,labour_rate=10)
        self.db_session.add_all([part,wood]);self.db_session.commit()
        before=(base.MechCatalogPart.query.count(),base.ProTradeBoqItem.query.count())
        response=self.client.get(self.quote_url(debtor),query_string={'area':'new'})
        tree=html.fromstring(response.data);data=json.loads(tree.get_element_by_id('quote-builder-data').text)
        wood_choice=next(row for row in data['choices'] if row['source_id']==wood.id)
        pipe_choice=next(row for row in data['choices'] if row['source_id']==f'part:{part.id}')
        self.assertEqual(pipe_choice['material_rate'],'80.00')
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(response.get_data(as_text=True));page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                page.locator('#quote-catalogue').select_option([f'carpentry:{self.carpentry.id}',f'plumbing:{plumbing.id}'])
                for choice,quantity in [(wood_choice,'2'),(pipe_choice,'3')]:
                    page.locator('#quote-work-select').fill(choice['label']);page.locator('#quote-work-quantity').fill(quantity);page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('[data-trade=carpentry]').inner_text(),'Carpentry subtotal: R60.00')
                self.assertEqual(page.locator('[data-trade=plumbing]').inner_text(),'Plumbing subtotal: R240.00')
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'300.00')
            finally:browser.close()
        payload={'area':'new','quote_key':uuid4().hex,'item_id[]':[str(wood.id),f'part:{part.id}'],f'quantity_{wood.id}':'2',f'quantity_part:{part.id}':'3'}
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,302)
        quote=base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(quote.grand_total,Decimal('300.00'))
        self.assertEqual({line['trade'] for line in quote.items},{'carpentry','plumbing'})
        self.assertEqual((base.MechCatalogPart.query.count(),base.ProTradeBoqItem.query.count()),before)
        self.assertEqual(part.default_price,80)

    def test_existing_carpentry_rates_snapshots_isolation_and_timer(self):
        shop, debtor = self.fixtures()
        source = self.shop(name='Existing Carpentry', trade='carpentry')
        foreign = self.shop(owner=2, name='Private Carpentry', trade='carpentry')
        first = base.ProTradeBoqItem(user_id=1, shop_id=source.id, trade='carpentry', code='CAR-001', description='Owned wooden door', unit='Each', material_rate=20, labour_rate=10)
        second = base.ProTradeBoqItem(user_id=1, shop_id=source.id, trade='carpentry', code='CAR-002', description='Owned frame', unit='Each', material_rate=30, labour_rate=10)
        private = base.ProTradeBoqItem(user_id=2, shop_id=foreign.id, trade='carpentry', code='CAR-PRIVATE', description='Private pricing', unit='Each', material_rate=99, labour_rate=99)
        self.db_session.add_all([first,second,private]);self.db_session.commit()
        response = self.client.get(self.quote_url(debtor), query_string={'area':'new'})
        self.assertEqual(response.status_code, 200)
        tree = html.fromstring(response.data)
        data = json.loads(tree.get_element_by_id('quote-builder-data').text)
        self.assertIn('Carpentry — Existing Carpentry', tree.text_content())
        self.assertNotIn('Private pricing', response.get_data(as_text=True))
        self.assertFalse(any(choice['code']=='WORK-001' for choice in data['choices']))
        choice = next(row for row in data['choices'] if row['source_id']==first.id)
        self.assertEqual((choice['unit'],choice['material_rate'],choice['labour_rate']), ('Each','20.00','10.00'))
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(response.get_data(as_text=True))
                page.add_script_tag(path=str(base.ROOT/'app/static/js/carpentry_quote_builder.js'))
                page.locator('#quote-catalogue').select_option(f'carpentry:{source.id}')
                page.locator('#quote-work-select').fill(choice['label'])
                self.assertEqual(page.locator('#quote-work-code').input_value(),'CAR-001')
                self.assertEqual(page.locator('#quote-work-unit').input_value(),'Each')
                self.assertEqual(page.locator('#quote-work-material').input_value(),'20.00')
                page.locator('#quote-work-material').fill('12');page.locator('#quote-work-labour').fill('8')
                page.locator('#quote-work-quantity').fill('2');page.locator('#quote-add-item').click()
                page.locator('#quote-work-select').fill('CAR-002 — Owned frame')
                page.locator('#quote-work-quantity').fill('3');page.locator('#quote-add-item').click()
                self.assertEqual(page.locator('#quote-grand-total').text_content(),'160.00')
                self.assertEqual(page.locator('#quote-subtotal').text_content(),'160.00')
            finally:
                browser.close()
        key = uuid4().hex
        self.assertEqual(self.client.post(f'/mechanic/shop/{shop.id}/debtor/{debtor.id}/boq-preparation', data=dict(quote_key=key,tab_key=uuid4().hex,tick=1,action='active')).status_code,204)
        balance = base.models.AitTokenWallet.query.filter_by(user_id=1).one().balance
        payload = {'area':'new','quote_key':key,'item_id[]':[str(first.id),str(second.id)],f'quantity_{first.id}':'2',f'material_rate_{first.id}':'12',f'labour_rate_{first.id}':'8',f'quantity_{second.id}':'3'}
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,302)
        quote = base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(quote.grand_total, Decimal('160.00'))
        self.assertEqual([row['total'] for row in quote.items], ['40.00','120.00'])
        self.assertEqual(first.material_rate,Decimal('20.00'))
        self.assertEqual(first.labour_rate,Decimal('10.00'))
        self.assertEqual(base.ProTradeBoqPreparation.query.one().quote_id,quote.id)
        self.assertEqual(base.models.AitTokenWallet.query.filter_by(user_id=1).one().balance,balance)
        self.assertEqual(base.ProTradeDocument.query.count(),0)
        # A forged reference to another user's catalogue cannot be saved.
        payload.update(quote_key=uuid4().hex)
        payload['item_id[]']=[str(private.id)];payload[f'quantity_{private.id}']='1'
        self.assertEqual(self.client.post(self.quote_url(debtor),data=payload).status_code,400)
