"""All registered trades use one Price List; no production data or startup factory."""
from decimal import Decimal
import json
from uuid import uuid4
import unittest
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_unified_prices as unified
import test_protrade_shop_register as base
from app.protrade_boq_catalog import TRADE_CATALOGUES,work_items_for


class AllTradePriceTests(unittest.TestCase):
    setUp=unified.UnifiedPricesTests.setUp
    shop=unified.UnifiedPricesTests.shop
    save_debtor=unified.UnifiedPricesTests.save_debtor
    debtor=unified.UnifiedPricesTests.debtor
    save_quote=unified.UnifiedPricesTests.save_quote
    quote_url=unified.UnifiedPricesTests.quote_url
    ledger_url=unified.UnifiedPricesTests.ledger_url
    fixtures=unified.UnifiedPricesTests.fixtures
    save_price=unified.UnifiedPricesTests.save_price
    choices=unified.UnifiedPricesTests.choices

    def test_every_registered_trade_defaults_to_own_catalogue_and_calculates_in_same_interface(self):
        registered=dict(self.ns['AVAILABLE_TRADES'])
        self.assertEqual(set(registered),set(TRADE_CATALOGUES))
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page()
                for trade in registered:
                    with self.subTest(trade=trade):
                        shop=self.shop(trade=trade,name='Registered '+trade)
                        response=self.client.get('/mechanic/catalog',query_string=dict(shop_id=shop.id))
                        self.assertEqual(response.status_code,200)
                        tree=html.fromstring(response.data)
                        self.assertEqual(tree.xpath('//select[@id="price-category"]/option[@selected]/@value'),[trade])
                        self.assertIn('General Works',tree.text_content())
                        self.assertIn('Client-specific item',tree.text_content())
                        self.assertTrue(work_items_for(trade))
                        self.assertEqual(base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).count(),0)
                        page.set_content(response.text)
                        selected=work_items_for(trade)[0]
                        page.locator('#price-item-select').select_option(selected['code'])
                        self.assertEqual(page.locator('#boq-code').input_value(),selected['code'])
                        self.assertEqual(page.locator('#boq-unit').input_value(),selected['unit'])
                        self.assertEqual(page.locator('#boq-quantity').input_value(),'1')
                        page.locator('#material-rate').fill('12.34');page.locator('#labour-rate').fill('6.66')
                        page.locator('#boq-quantity').fill('2')
                        self.assertEqual(page.locator('#total-rate').text_content(),'19.00')
                        self.assertEqual(page.locator('#total-amount').text_content(),'38.00')
                        self.assertEqual(self.save_price(shop,trade,description=selected['description'],unit=selected['unit'],preset_code=selected['code']).status_code,302)
                        self.assertEqual(base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).one().code,selected['code'])
            finally:browser.close()

    def test_new_auto_electrician_registration_and_independent_prices_reach_quote_engine(self):
        response=self.client.post('/mechanic/onboarding/process',data=dict(trade='auto_elec',business_name='Short Circuit',vat_registered='no'))
        self.assertEqual(response.status_code,302)
        shop=base.models.MechShop.query.filter_by(business_name='Short Circuit').one()
        page=self.client.get('/mechanic/catalog',query_string=dict(shop_id=shop.id))
        self.assertIn('Vehicle electrical diagnostic inspection',page.text)
        self.assertIn('Auto Electrician',page.text)
        self.assertEqual(self.save_price(shop,'auto_elec',description='Private diagnostic',material_rate='10',labour_rate='5').status_code,302)
        another=self.shop(trade='auto_elec',name='Other auto electrician')
        self.assertEqual(self.save_price(another,'auto_elec',description='Private diagnostic',material_rate='999',labour_rate='1').status_code,302)
        self.save_debtor(shop,name='Auto debtor');debtor=self.debtor(shop)
        response,data=self.choices(debtor)
        own=next(item for item in data['choices'] if item['description']=='Private diagnostic')
        self.assertEqual(Decimal(own['material_rate']),Decimal('10'))
        self.assertEqual(Decimal(own['labour_rate']),Decimal('5'))
        self.assertNotIn('999',json.dumps(data))
        item=base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).one()
        response=self.client.post(self.quote_url(debtor),data={'area':'new','quote_key':uuid4().hex,'item_id[]':[str(item.id)],f'quantity_{item.id}':'2'})
        self.assertEqual(response.status_code,302,response.text)
        quote=base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        self.assertEqual(quote.grand_total,Decimal('30.00'))
        self.assertEqual(quote.items[0]['trade'],'auto_elec')
        self.assertEqual(item.total_rate,Decimal('15.00'))

    def test_each_nonmechanic_trade_prepares_quotes_from_its_own_unified_prices(self):
        self.fixtures()
        for trade in TRADE_CATALOGUES:
            if trade=='mechanic':continue
            with self.subTest(trade=trade):
                shop=self.shop(trade=trade,name='Quote '+trade)
                self.save_debtor(shop,name='Debtor '+trade);debtor=self.debtor(shop)
                self.assertEqual(self.save_price(shop,trade,description='Own '+trade,material_rate='8',labour_rate='3').status_code,302)
                response,data=self.choices(debtor)
                row=next(item for item in data['choices'] if item['description']=='Own '+trade)
                self.assertEqual(Decimal(row['material_rate']),Decimal('8'))
                self.assertEqual(Decimal(row['labour_rate']),Decimal('3'))
                self.assertEqual(row['catalogue_key'],trade)

    def test_legacy_prices_are_visible_but_never_rewritten_or_inferred_as_split(self):
        for trade in ('mechanic','panel','auto_elec','generic','protrade'):
            with self.subTest(trade=trade):
                shop=self.shop(trade=trade)
                part=base.MechCatalogPart(user_id=1,shop_id=shop.id,trade=trade,part_name='Legacy '+trade,item_code='OLD-'+trade.upper(),default_price=19.995)
                self.db_session.add(part);self.db_session.commit()
                response=self.client.get('/mechanic/catalog',query_string=dict(shop_id=shop.id))
                self.assertIn('Legacy '+trade,response.text)
                response=self.client.get('/mechanic/catalog',query_string=dict(shop_id=shop.id,edit_id='legacy:'+str(part.id)))
                tree=html.fromstring(response.data)
                self.assertEqual(Decimal(tree.xpath('//input[@id="combined-rate"]/@value')[0]),Decimal('19.995'))
                self.assertEqual(self.save_price(shop,trade,item_id='legacy:'+str(part.id),description='Legacy '+trade,combined_rate='19.995').status_code,302)
                canonical=base.ProTradeBoqItem.query.filter_by(shop_id=shop.id).one()
                self.assertEqual(canonical.combined_rate,Decimal('19.995'))
                self.assertIsNone(canonical.material_rate)
                self.assertIsNone(canonical.labour_rate)
                self.assertEqual(part.default_price,19.995)
                self.assertEqual(part.item_code,'OLD-'+trade.upper())
                self.assertEqual(canonical.legacy_part_id,part.id)
                response=self.client.get('/mechanic/catalog',query_string=dict(shop_id=shop.id))
                self.assertNotIn('edit_id=legacy:',response.text)

    def test_mechanic_same_named_category_prices_have_distinct_quote_choices(self):
        from app.protrade_boq_catalog import mechanic_quote_catalogue
        shop=self.shop(trade='mechanic')
        for category,rate in [('mechanic','15'),('general','30')]:
            self.assertEqual(self.save_price(shop,category,description='Same work',material_rate=rate,labour_rate='5').status_code,302)
        rows=mechanic_quote_catalogue(1,shop,base.ProTradeBoqItem,base.models.MechShop,base.MechCatalogPart,[])
        matching=[row for row in rows if row.part_name.endswith('/ Same work')]
        self.assertEqual(len(matching),2)
        self.assertEqual(len({row.part_name for row in matching}),2)
        self.assertEqual({row.default_price for row in matching},{Decimal('15'),Decimal('30')})

    def test_mechanic_quote_uses_same_split_prices_without_rewriting_legacy_records(self):
        shop=self.shop(trade='mechanic')
        self.save_debtor(shop,name='Mechanic debtor');debtor=self.debtor(shop)
        self.assertEqual(self.save_price(shop,'mechanic',description='Canonical repair',material_rate='45.25',labour_rate='85.50').status_code,302)
        self.db_session.add(base.MechCatalogPart(user_id=None,shop_id=None,trade=None,part_name='Canonical repair',default_price=999))
        self.db_session.commit()
        response=self.client.get('/mechanic/quote/new',query_string=dict(shop_id=shop.id,debtor_id=debtor.id))
        self.assertEqual(response.status_code,200)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.set_content(response.text)
                page.locator('[name="part_desc[]"]').first.fill('Canonical repair')
                self.assertEqual(page.locator('[name="part_rate[]"]').first.input_value(),'45.25')
                page.locator('[name="labor_desc[]"]').first.fill('Canonical repair')
                self.assertEqual(page.locator('[name="labor_rate[]"]').first.input_value(),'85.50')
                page.locator('[name="labor_desc[]"]').first.fill('Vehicle diagnostic inspection')
                self.assertEqual(page.locator('[name="labor_rate[]"]').first.input_value(),'')
            finally:browser.close()


if __name__=='__main__':unittest.main()
