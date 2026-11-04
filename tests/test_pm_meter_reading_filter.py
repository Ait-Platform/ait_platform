"""Real browser filtering over property-isolated canonical PostgreSQL readings."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_utilities_capture as fixture
class ReadingFilterTests(fixture.UtilitiesCaptureTests):
    def setUp(self):
        super().setUp()
        from app.program_billing import utilities_output
        mock=patch.object(utilities_output,'db',self.db);mock.start();self.addCleanup(mock.stop)
        mock=patch.object(utilities_output,'current_user',self.user);mock.start();self.addCleanup(mock.stop)
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(99,26,'ANG489','Water'),(98,26,'EMPTY','Water'),(97,28,'FOREIGN','Water'),(96,26,'ELEC-96','Electricity')"))
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
    def test_reading_sections_and_edit_workspace_preserve_form_and_context(self):
        before=self.snapshot()
        tree=html.fromstring(self.client.get(self.readings_url+'?billing_month=2026-09',headers=self.headers).data)
        self.assertFalse(tree.xpath('//nav[@aria-label="Municipal Utilities"]'))
        self.assertEqual(tree.xpath('//section/h2/text()'),['Reporting Month','Capture Meter Reading','Recorded Meter Readings','Work Area'])
        self.assertTrue(tree.xpath('//section[@id="reading-reporting"]//form[@id="municipal-reading-period"]//button[@value="municipal_pdf"]'))
        self.assertTrue(tree.xpath('//section[@id="reading-capture"]//form[@id="utilities-capture"]'))
        self.assertTrue(tree.xpath('//section[@id="reading-register"]//tr[@data-reading-row]'))
        self.assertTrue(tree.xpath('//section[@id="reading-work-area"][@hidden]'))
        self.assertEqual(len(tree.xpath('//form[@id="utilities-capture"]')),1)
        record=self.session.execute(text('SELECT id FROM bil_meter_reading WHERE meter_id=99 ORDER BY reading_date DESC LIMIT 1')).scalar()
        edit=self.readings_url+'?edit='+str(record)+'&billing_month=2026-09&view_item=99'
        tree=html.fromstring(self.client.get(edit,headers=self.headers).data)
        self.assertTrue(tree.xpath('//section[@id="reading-work-area"][not(@hidden)]//form[@id="utilities-capture"]'))
        self.assertFalse(tree.xpath('//section[@id="reading-capture"]//form'))
        self.assertEqual(tree.xpath('//input[@name="record_id"]/@value'),[str(record)])
        self.assertEqual(tree.xpath('//input[@name="reading_value"]/@value'),['108.0'])
        self.assertEqual(tree.xpath('//input[@name="billing_month"]/@value'),['2026-09','2026-09'])
        self.assertEqual(tree.xpath('//select[@id="meter_id"]/option[@selected]/@value'),['99'])
        data=self.capture_form(edit);data.pop('csrf_token');data['reading_value']='-1'
        failed=self.capture_post(self.readings_url,**data)
        self.assertEqual(failed.status_code,400)
        invalid=html.fromstring(failed.data)
        self.assertEqual(invalid.xpath('//section[@id="reading-work-area"]//input[@name="reading_value"]/@value'),['-1'])
        self.assertTrue(invalid.xpath('//p[@role="alert"]'))
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
                self.assertFalse(tab.locator('#reading-work-area').is_visible())
                tab.select_option('#meter_id','99')
                self.assertTrue(tab.locator('#reading-work-area').is_visible())
                self.assertEqual(tab.locator('#reading-selected-meter').inner_text(),'ANG489')
                self.assertEqual(tab.locator('#reading-selected-unit').inner_text(),'kL')
                self.assertEqual(tab.locator('#reading-unit').inner_text(),'kL')
                tab.select_option('#meter_id','96')
                self.assertEqual(tab.locator('#reading-selected-unit').inner_text(),'kWh')
                tab.select_option('#meter_id','')
                self.assertFalse(tab.locator('#reading-work-area').is_visible())
                self.assertEqual(tab.locator('#reading-register-actions #reading-meter-filter').count(),1)
                self.assertEqual(tab.locator('#capture-actions').get_by_role('link',name='Cancel',exact=True).count(),1)
                self.assertEqual(tab.locator('#capture-actions').get_by_role('button',name='Save',exact=True).count(),1)
                tab.select_option('#reading-meter-filter','99')
                self.assertIn('item_id=99',tab.locator('#reading-output').get_attribute('href'))
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
                self.assertTrue(tab.locator('#reading-output').get_attribute('href').endswith('item_id='))
                tab.click('#reading-toggle');self.assertEqual(rows.count(),9)
            finally:browser.close()
        self.assertEqual(before,self.snapshot())
    def test_output_carries_meter_or_all_and_rejects_other_property(self):
        before=self.snapshot()
        base='/billing/utilities/26/output?source=readings&preview=1&item_id='
        page=self.client.get(base+'99',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//tbody/tr')),8)
        self.assertIn('ANG489',page.text);self.assertIn('view_item=99',page.text)
        self.assertEqual(len(html.fromstring(self.client.get(base,headers=self.headers).data).xpath('//tbody/tr')),9)
        self.assertEqual(self.client.get(base+'97',headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(base.replace('/26/','/28/'),headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(base).status_code,403)
        self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReadingFilterTests(name) for name in ReadingFilterTests.__dict__ if name.startswith('test_'))
