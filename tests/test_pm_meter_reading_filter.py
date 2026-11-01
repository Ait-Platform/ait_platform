"""Real browser filtering over property-isolated canonical PostgreSQL readings."""
import unittest
from sqlalchemy import text
from lxml import html
import test_pm_utilities_capture as fixture
class ReadingFilterTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(99,26,'ANG489','Water'),(98,26,'EMPTY','Water'),(97,28,'FOREIGN','Water')"))
        for day in range(1,9):
            self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(99,CAST(:d AS date),:v)'),dict(d='2026-09-%02d'%day,v=100+day))
        self.session.commit();self.conn.commit()
    def test_filter_options_and_rows_are_property_isolated_and_descending(self):
        before=self.snapshot();page=self.client.get(self.readings_url,headers=self.headers)
        self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        options=tree.xpath('//select[@id="reading-meter-filter"]/option/text()')
        self.assertIn('All Meters',options);self.assertIn('ANG489',options);self.assertNotIn('FOREIGN',options)
        dates=tree.xpath('//tr[@data-reading-row][@data-meter-id="99"]/td[1]/text()')
        self.assertEqual(dates,sorted(dates,reverse=True));self.assertEqual(len(dates),8)
        self.assertEqual(before,self.snapshot())
    def test_browser_filter_show_more_less_and_all_meters(self):
        from playwright.sync_api import sync_playwright
        before=self.snapshot();page=self.client.get(self.readings_url,headers=self.headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort())
                tab.set_content(page.text,wait_until='domcontentloaded')
                tab.add_script_tag(path='app/static/js/pm_reading_toggle.js')
                rows=tab.locator('[data-reading-row]:visible')
                self.assertEqual(rows.count(),5)
                tab.select_option('#reading-meter-filter','99')
                self.assertEqual(rows.count(),5)
                self.assertEqual(tab.locator('[data-reading-row]:visible:not([data-meter-id="99"])').count(),0)
                self.assertEqual(tab.locator('#reading-toggle').inner_text(),'Show More (5 of 8)')
                tab.click('#reading-toggle');self.assertEqual(rows.count(),8)
                self.assertEqual(tab.locator('#reading-toggle').inner_text(),'Show Less (8 of 8)')
                tab.click('#reading-toggle');self.assertEqual(rows.count(),5)
                tab.select_option('#reading-meter-filter','98');self.assertEqual(rows.count(),0)
                self.assertTrue(tab.locator('#reading-filter-empty').is_visible())
                self.assertFalse(tab.locator('#reading-toggle').is_visible())
                tab.select_option('#reading-meter-filter','');self.assertEqual(rows.count(),5)
                tab.click('#reading-toggle');self.assertEqual(rows.count(),9)
            finally:browser.close()
        self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReadingFilterTests(name) for name in ReadingFilterTests.__dict__ if name.startswith('test_'))
