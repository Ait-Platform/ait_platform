"""Read-only chronological meter output and canonical monthly averages."""
import unittest
from decimal import Decimal as D
from pathlib import Path
from io import BytesIO
from sqlalchemy import text
from lxml import html
from pypdf import PdfReader
import test_pm_utilities_output as fixture

class OutputAverageTests(unittest.TestCase):
    def setUp(self):
        self.fx=fixture.UtilitiesOutputTests(methodName='test_inclusive_reading_dates_all_meters_units_and_empty')
        self.fx.setUp();self.addCleanup(self.fx.doCleanups);self.addCleanup(self.fx.tearDown)
        s=self.fx.session
        s.execute(text("UPDATE bil_property SET name='Almond' WHERE id=26"))
        s.execute(text("UPDATE bil_meter SET meter_number='ANG489' WHERE id=99"))
        s.execute(text('DELETE FROM bil_meter_reading WHERE meter_id IN(99,96)'))
        for meter,entries in [(99,[('2026-07-31',14820),('2026-08-30',14950),('2026-09-30',15043),('2026-10-30',15180)]),(96,[('2026-05-31',100),('2026-06-30',120),('2026-08-31',150),('2026-09-30',140),('2026-10-31',180)])]:
            for day,value in entries:s.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter,d=day,v=value))
        s.commit();self.fx.conn.commit()
    def report(self,**filters):
        before=self.fx.snapshot()
        with self.fx.app.test_request_context():data=self.fx.output.report(26,dict(source='readings',**filters))
        self.assertEqual(before,self.fx.snapshot());return data
    def test_ang489_example_and_chronological_all_history(self):
        data=self.report(item_id='99');self.assertEqual(len(data['meter_reports']),1)
        m=data['meter_reports'][0]
        self.assertEqual([str(r['reading_date']) for r in m['rows']],['2026-07-31','2026-08-30','2026-09-30','2026-10-30'])
        self.assertEqual([r['consumption'] for r in m['rows']],[None,D(130),D(93),D(137)])
        self.assertEqual([r['past_reading'] for r in m['rows'][1:]],list(map(D,(14820,14950,15043))))
        self.assertEqual(m['valid_months'],3);self.assertEqual(m['total_consumption'],360);self.assertEqual(m['average'],120)
        page=self.fx.preview(item='99',start='',end='');self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);self.assertIn('120 kL/month',tree.xpath('//table/tfoot')[0].text_content())
    def test_invalid_and_missing_months_excluded_electricity_uses_kwh(self):
        data=self.report(item_id='96');m=data['meter_reports'][0]
        self.assertEqual([r['consumption'] for r in m['rows']],[None,D(20),None,None,D(40)])
        self.assertEqual(m['valid_months'],2);self.assertEqual(m['average'],30);self.assertEqual(m['unit'],'kWh')
        all_meters=self.report()
        self.assertEqual(len(all_meters['meter_reports']),3)
        self.assertNotIn('PRIVATE-W',str(all_meters))
        empty=next(m for m in all_meters['meter_reports'] if m['meter']['id']==1)
        self.assertIsNone(empty['average']);self.assertEqual(empty['valid_months'],0)
    def test_additional_readings_count_month_once_and_use_monthly_maximum(self):
        self.fx.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(99,'2026-09-15',15060)"));self.fx.session.commit();self.fx.conn.commit()
        m=self.report(item_id='99')['meter_reports'][0]
        rows=m['rows'];self.assertEqual(len(rows),5)
        self.assertEqual([r['consumption'] for r in rows],[None,D(130),D(110),None,D(120)])
        self.assertEqual(m['valid_months'],3);self.assertEqual(m['average'],120)
    def test_filter_keeps_prior_month_baseline_and_detects_stale_preview(self):
        data=self.report(item_id='99',date_from='2026-09-01',date_to='2026-09-30')
        self.assertEqual(data['meter_reports'][0]['average'],93)
        page=self.fx.preview();fields=self.fx.email_fields(page)
        self.fx.session.execute(text("UPDATE bil_meter_reading SET reading_value=14960 WHERE meter_id=99 AND reading_date='2026-08-30'"));self.fx.session.commit();self.fx.conn.commit()
        stale=self.fx.client.get(self.fx.base,query_string=dict(fields,format='pdf'),headers=self.fx.headers)
        self.assertEqual(stale.status_code,409);self.assertIn('records changed after preview',stale.text)
    def test_download_pdf_contains_average_and_matches_preview(self):
        before=self.fx.snapshot();page=self.fx.preview(item='99',start='',end='')
        fields=self.fx.email_fields(page)
        response=self.fx.client.get(self.fx.base,query_string=dict(fields,format='pdf',download='1'),headers=self.fx.headers)
        self.assertEqual(response.status_code,200,response.text if response.mimetype!='application/pdf' else '')
        self.assertIn('attachment',response.headers['Content-Disposition'])
        pdf=PdfReader(BytesIO(response.data));content=' '.join(p.extract_text() or '' for p in pdf.pages)
        for expected in ('ANG489','Average Consumption','120 kL/month','14950','15043','15180'):
            self.assertIn(expected,content.replace(',',''))
        output=Path('output/pdf');output.mkdir(parents=True,exist_ok=True);(output/'utilities-output-ang489-average-test.pdf').write_bytes(response.data)
        import fitz
        document=fitz.open(stream=response.data,filetype='pdf');images=Path('tmp/pdfs/output-average');images.mkdir(parents=True,exist_ok=True)
        for i,p in enumerate(document):p.get_pixmap(matrix=fitz.Matrix(1.25,1.25)).save(str(images/(str(i+1)+'.png')))
        self.assertEqual(before,self.fx.snapshot())
    def test_hub_open_close_selection_and_download_dirty_protection(self):
        from playwright.sync_api import sync_playwright
        initial=self.fx.client.get(self.fx.base,headers=self.fx.headers)
        tree=html.fromstring(initial.data)
        self.assertTrue(tree.xpath('//*[@id="output-workspace"]/@hidden'))
        self.assertIn('Utilities Output - Almond',initial.text)
        self.assertIn('Back to Utilities',initial.text)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort());tab.set_content(initial.text)
                tab.add_script_tag(path='app/static/js/pm_utilities_output.js')
                self.assertFalse(tab.locator('#output-workspace').is_visible())
                tab.locator('#output-hub-open').click();self.assertTrue(tab.locator('#output-workspace').is_visible())
                self.assertEqual(tab.locator('#output-item').input_value(),'')
                tab.locator('#output-hub-close').click();self.assertFalse(tab.locator('#output-workspace').is_visible())
                page=self.fx.preview(item='99',start='',end='');tab.set_content(page.text);tab.add_script_tag(path='app/static/js/pm_utilities_output.js')
                self.assertTrue(tab.locator('#output-download').is_visible())
                tab.select_option('#output-item','96');self.assertFalse(tab.locator('#output-download').is_visible())
            finally:browser.close()
    def test_utilities_menu_has_property_scoped_output(self):
        from flask import render_template
        with self.fx.app.test_request_context():page=render_template('program_billing/utilities_hub.html',selected_property=dict(id=26,name='Almond',address=''),current_month='2026-10',properties=[])
        tree=html.fromstring(page);links=tree.xpath('//nav[@aria-label="Municipal Utilities"]/a[text()="Output"]/@href')
        self.assertEqual(len(links),1);self.assertIn('/26/output',links[0]);self.assertIn('billing_month=2026-10',links[0])
    def test_cross_user_meter_and_property_still_rejected(self):
        self.assertEqual(self.fx.preview(item='97').status_code,403)
        self.assertEqual(self.fx.client.get(self.fx.base.replace('/26/','/28/'),headers=self.fx.headers).status_code,403)

if __name__=='__main__':unittest.main()
