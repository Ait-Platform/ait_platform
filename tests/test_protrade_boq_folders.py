"""Paid BOQ sections download normally without charging or finalising again."""
from uuid import uuid4
import unittest
from unittest.mock import patch
from playwright.sync_api import sync_playwright
import test_protrade_boq_trade as boq
import test_protrade_shop_register as base
import test_protrade_output as output


class BoqFolderTests(unittest.TestCase):
    setUp=boq.BoqTradeTests.setUp
    shop=boq.BoqTradeTests.shop
    save_debtor=boq.BoqTradeTests.save_debtor
    debtor=boq.BoqTradeTests.debtor
    save_quote=boq.BoqTradeTests.save_quote
    quote_url=boq.BoqTradeTests.quote_url
    ledger_url=boq.BoqTradeTests.ledger_url
    browser_route=output.ProTradeOutputTests.browser_route

    def test_paid_section_download_without_folder_or_extra_deduction(self):
        shop,debtor=boq.BoqTradeTests.fixtures(self)
        item=base.ProTradeBoqItem(user_id=1,shop_id=shop.id,trade='boq',code='BOQ-C-TEST',description='Work',unit='Each',material_rate=20,labour_rate=30)
        self.db_session.add(item);self.db_session.commit()
        key=uuid4().hex
        self.assertEqual(self.client.post(f'/mechanic/shop/{shop.id}/debtor/{debtor.id}/boq-preparation',data=dict(quote_key=key,tab_key=uuid4().hex,tick=1,action='active')).status_code,204)
        self.assertEqual(self.client.post(self.quote_url(debtor),data={'area':'new','quote_key':key,'item_id[]':[str(item.id)],f'quantity_{item.id}':'1'}).status_code,302)
        quote=base.ProTradeBoqQuote.query.filter_by(shop_id=shop.id).one()
        url=f'/mechanic/shop/{shop.id}/boq/{quote.id}/sections'
        self.assertEqual(self.client.post(url,data=dict(action='save',name='Work',item_id=[str(item.id)])).status_code,302)
        with patch.object(output.output,'pdf_bytes',return_value=b'%PDF-BOQ-section'):
            self.assertEqual(self.client.post(url,data=dict(action='generate',section_id=quote.boq_sections[0]['id'])).status_code,302)
            balance=base.models.AitTokenWallet.query.filter_by(user_id=1).one().balance
            with sync_playwright() as runtime:
                browser=runtime.chromium.launch(headless=True)
                try:
                    page=browser.new_page();page.route('http://localhost/**',self.browser_route)
                    page.goto('http://localhost'+url)
                    self.assertEqual(page.locator('[data-section-save], #boq-folder-context').count(),0)
                    with page.expect_download() as download:page.get_by_role('link',name='Download',exact=True).click()
                    self.assertTrue(download.value.suggested_filename.endswith('.pdf'))
                finally:browser.close()
        self.assertEqual(base.models.AitTokenWallet.query.filter_by(user_id=1).one().balance,balance)
        self.assertEqual(base.ProTradeDocument.query.filter_by(kind='boq_section',shop_id=shop.id).count(),1)
        self.assertIsNone(base.ProTradeBoqPreparation.query.filter_by(quote_id=quote.id).one().completed_at)
