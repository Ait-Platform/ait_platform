"""Historical municipal-account filtering without changing capture or payment records."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_utilities_capture as fixture
class StatementFilterTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        from app.program_billing import utilities_output
        mock=patch.object(utilities_output,'db',self.db);mock.start();self.addCleanup(mock.stop)
        mock=patch.object(utilities_output,'current_user',self.user);mock.start();self.addCleanup(mock.stop)
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number) VALUES(2,26,'SECOND-26'),(3,28,'FOREIGN-28')"))
        for account,month in [(1,m) for m in range(1,8)]+[(2,7),(2,8),(3,9)]:
            self.session.execute(text("INSERT INTO bil_municipal_statement(account_id,period,statement_date,arrears,balance,due,due_date) VALUES(:a,:p,CAST(:d AS date),10.01,100.02,90.01,CAST(:d AS date))"),dict(a=account,p='2026-%02d'%month,d='2026-%02d-20'%month))
        self.session.commit();self.conn.commit()
    def test_account_controls_scope_order_and_output(self):
        before=self.snapshot();page=self.client.get(self.statements_url,headers=self.headers);self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//select[@id="statement-account-filter"]/option/@value'),['','1','2'])
        self.assertEqual(tree.xpath('//*[@id="capture-actions"]//a/text()'),['Cancel','Output'])
        dates=tree.xpath('//tr[@data-statement-row]/td[1]/text()');self.assertEqual(dates,sorted(dates,reverse=True));self.assertEqual(len(dates),9)
        out=self.client.get('/billing/utilities/26/output?source=reconciliation&preview=1&item_id=1',headers=self.headers)
        self.assertEqual(out.status_code,200,out.text);self.assertEqual(len(html.fromstring(out.data).xpath('//tbody/tr')),7)
        self.assertNotIn('SECOND-26',html.fromstring(out.data).xpath('//section[@id="utilities-report"]')[0].text_content());self.assertNotIn('FOREIGN-28',out.text)
        self.assertEqual(self.client.get('/billing/utilities/26/output?source=reconciliation&item_id=3',headers=self.headers).status_code,403)
        self.assertEqual(before,self.snapshot())
    def test_browser_account_filter_across_months_and_all_accounts(self):
        from playwright.sync_api import sync_playwright
        before=self.snapshot();page=self.client.get(self.statements_url,headers=self.headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort());tab.set_content(page.text,wait_until='domcontentloaded')
                tab.add_script_tag(path='app/static/js/pm_statement_filter.js')
                rows=tab.locator('[data-statement-row]:visible');self.assertEqual(rows.count(),9)
                tab.select_option('#statement-account-filter','1');self.assertEqual(rows.count(),7)
                self.assertEqual(tab.locator('[data-statement-row]:visible:not([data-account-id="1"])').count(),0)
                self.assertIn('item_id=1',tab.locator('#statement-output').get_attribute('href'))
                tab.select_option('#statement-account-filter','2');self.assertEqual(rows.count(),2)
                self.assertIn('item_id=2',tab.locator('#statement-output').get_attribute('href'))
                tab.select_option('#statement-account-filter','');self.assertEqual(rows.count(),9)
                self.assertTrue(tab.locator('#statement-output').get_attribute('href').endswith('item_id='))
            finally:browser.close()
        self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(StatementFilterTests(name) for name in StatementFilterTests.__dict__ if name.startswith('test_'))
