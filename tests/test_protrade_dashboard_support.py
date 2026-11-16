"""ProTrade Folders/Help navigation and shared persistent browser folder storage."""
import json
import unittest
from unittest.mock import patch
from urllib.parse import urlencode
from lxml import html
from playwright.sync_api import sync_playwright
import test_protrade_shop_register as base
import test_protrade_output as reports


class DashboardSupportTests(unittest.TestCase):
    def setUp(self):
        reports.ProTradeOutputTests.setUp(self)
        self.app.jinja_loader.loaders[0].mapping.pop('program_mechanic/menu.html',None)
    shop=reports.ProTradeOutputTests.shop
    contact=reports.ProTradeOutputTests.contact
    boq_quote=reports.ProTradeOutputTests.boq_quote
    browser_route=reports.ProTradeOutputTests.browser_route

    def test_release_menu_legacy_redirect_and_ownership(self):
        for shop in (self.carpentry,self.second,self.mechanic):
            response=self.client.get('/mechanic/output',query_string={'shop_id':shop.id})
            self.assertEqual(response.status_code,200)
            tree=html.fromstring(response.data)
            self.assertEqual(tree.xpath('//nav[@aria-label="ProTrade"]//a/text()'),['Price List','Debtor','Tokens','Output','Help'])
            self.assertEqual(tree.xpath('//h1/text()'),['ProTrade Output'])
            shop_ids=tree.xpath('//select[@id="output-shop"]/option/@value')
            self.assertNotIn(str(self.foreign.id),shop_ids)
            self.assertIn(str(self.carpentry.id),shop_ids)
            legacy=self.client.get('/mechanic/folders',query_string={'shop_id':shop.id})
            self.assertEqual(legacy.status_code,302)
            self.assertTrue(legacy.location.endswith('/mechanic/output?shop_id='+str(shop.id)))
        for path in ('folders','help','output'):
            self.assertEqual(self.client.get('/mechanic/'+path,query_string={'shop_id':self.foreign.id}).status_code,404)
        guide=self.client.get('/mechanic/help').get_data(as_text=True)
        self.assertIn('Download PDF',guide)
        self.assertNotIn('Select one base ProTrade folder',guide)
        with self.client.session_transaction() as session:session.clear()
        for path in ('folders','help','output'):self.assertEqual(self.client.get('/mechanic/'+path).status_code,401)

if __name__=='__main__':unittest.main()
