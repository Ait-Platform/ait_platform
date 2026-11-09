"""Output toolbar placement and month/custom-date selection; existing builders."""
import unittest
from lxml import html
import test_pm_central_utilities_output as fixture

class OutputMenuMonthTests(unittest.TestCase):
    def setUp(self):
        self.owner=fixture.CentralOutputTests(methodName='test_all_seven_reports_use_one_work_area_and_preserve_records')
        self.owner.setUp();self.addCleanup(self.owner.doCleanups);self.fx=self.owner.fx
    def get(self,**changes):
        values=dict(source='readings',report='history');values.update(changes)
        return self.fx.client.get(self.owner.url,query_string=values,headers=self.fx.headers)
    def test_menu_order_and_six_controls(self):
        response=self.get();self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        menu=tree.get_element_by_id('output-actions');filters=tree.get_element_by_id('output-filters')
        self.assertEqual(menu.xpath('./button/text()|./a/text()'),['Print','Email','Save Report','Folders'])
        self.assertTrue(filters.xpath('./button[@name="preview"]'))
        self.assertIn('justify-end',menu.get('class'))
        children=list(menu.getparent())
        self.assertLess(children.index(tree.get_element_by_id('output-heading')),children.index(menu))
        self.assertLess(children.index(menu),children.index(filters))
        self.assertEqual(filters.xpath('./label/*/@name'),['source','report','item_id','by_month','date_from','date_to'])
        self.assertEqual(tree.xpath('//*[@id="output-download"]/@aria-disabled'),['true'])
        self.assertFalse(tree.xpath('//*[@id="output-download"]/@href'))
    def test_by_month_sets_full_calendar_boundaries_without_javascript(self):
        for month,first,last in [('2024-02','2024-02-01','2024-02-29'),('2026-02','2026-02-01','2026-02-28'),('2026-12','2026-12-01','2026-12-31')]:
            with self.subTest(month=month),self.fx.app.test_request_context():
                data=self.owner.output.report(26,dict(source='readings',report='history',by_month=month))
                self.assertEqual((data['date_from'],data['date_to']),(first,last))
                self.assertEqual(data['by_month'],month)
    def test_custom_dates_and_blank_history_preserve_existing_filters(self):
        with self.fx.app.test_request_context():
            data=self.owner.output.report(26,dict(source='readings',report='history',by_month='',billing_month='2026-10',date_from='2026-09-01',date_to='2026-09-30'))
            self.assertTrue(data['rows']);self.assertTrue(all(r['reading_date'].month==9 for r in data['rows']))
            all_data=self.owner.output.report(26,dict(source='readings',report='history',billing_month='2026-10'))
            self.assertEqual(all_data['date_from'],'');self.assertEqual(all_data['date_to'],'');self.assertEqual(all_data['by_month'],'')
            self.assertTrue(any(r['reading_date'].month==11 for r in all_data['rows']))
        self.assertEqual(self.get(by_month='not-a-month',preview='1').status_code,400)
    def test_monthly_reports_use_by_month_and_keep_existing_calculations(self):
        for source,key in [('metsoa','metsoa_statement'),('reconciliation','utilities_reconciliation')]:
            with self.subTest(key=key),self.fx.app.test_request_context():
                data=self.owner.output.report(26,dict(source=source,report=key,preview='1',by_month='2026-10',billing_month='2026-09',date_from='2026-10-10',date_to='2026-10-12',recipient_id='11'))
                original=self.fx.metsoa.build(26,'2026-10',recipient_id=11)
                self.assertEqual(data['billing_month'],'2026-10')
                self.assertEqual((data['date_from'],data['date_to']),('2026-10-01','2026-10-31'))
                self.assertEqual(data['report_payload']['grand_total'],original['grand_total'])
                self.assertEqual(data['report_payload']['pm_comparisons'],original['pm_comparisons'])
    def test_browser_month_fill_custom_range_and_menu_invalidation(self):
        from playwright.sync_api import sync_playwright
        response=self.get(preview='1')
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('**/*',lambda r:r.abort());page.set_content(response.text);page.add_script_tag(path='app/static/js/pm_utilities_output.js')
                page.locator('#output-month').fill('2024-02');page.locator('#output-month').dispatch_event('change')
                self.assertEqual(page.locator('#output-from').input_value(),'2024-02-01')
                self.assertEqual(page.locator('#output-to').input_value(),'2024-02-29')
                page.locator('#output-from').fill('2024-02-07');page.locator('#output-from').dispatch_event('change')
                self.assertEqual(page.locator('#output-month').input_value(),'')
                self.assertEqual(page.locator('#output-from').input_value(),'2024-02-07')
                self.assertTrue(page.locator('#output-download').is_hidden())
                self.assertEqual(page.locator('#output-download').get_attribute('aria-disabled'),'true')
                self.assertTrue(page.locator('#output-save').is_disabled())
                self.assertIsNone(page.locator('#output-download').get_attribute('href'))
                page.select_option('#output-source','metsoa')
                self.assertTrue(page.locator('#output-month').is_visible())
                self.assertTrue(page.locator('#output-recipient').is_visible())
                self.assertTrue(page.locator('#output-from').evaluate('(e)=>e.readOnly'))
                page.locator('#output-month').fill('2026-10');page.locator('#output-month').dispatch_event('change')
                self.assertEqual(page.locator('#output-billing-month').input_value(),'2026-10')
                self.assertEqual(page.locator('#output-to').input_value(),'2026-10-31')
            finally:browser.close()
    def test_preview_token_retains_by_month_for_download_and_email(self):
        response=self.get(preview='1',by_month='2026-10');self.assertEqual(response.status_code,200,response.text)
        fields=self.owner.fields(response);self.assertEqual(fields['by_month'],'2026-10')
        tree=html.fromstring(response.data)
        self.assertIn('by_month=2026-10',tree.get_element_by_id('output-download').get('href'))
        self.assertTrue(tree.xpath('//*[@id="output-download" and @href and @hidden]'))

if __name__=='__main__':unittest.main()
