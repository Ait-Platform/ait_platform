"""Historical consumption remains available without fabricated rates or refunds."""
import unittest
from decimal import Decimal
from io import BytesIO
from pypdf import PdfReader
from sqlalchemy import text
from lxml import html
import test_pm_metsoa_bulk as fixture

class HistoricalReportTests(fixture.BulkMetsoaTests):
    def test_june_before_tariffs_shows_consumption_and_downloadable_incomplete_pdf(self):
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date=reading_date-interval '4 months' WHERE reading_date BETWEEN '2026-09-01' AND '2026-10-31'"))
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-08-01'"))
        self.session.commit();self.conn.commit();before=self.snapshot()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-06',recipient_id=11)
        measured={r['number']:r for r in data['pm_consumption_rows']}
        self.assertEqual(measured['CEL888']['consumption'],Decimal(31))
        self.assertEqual(measured['7150696S']['consumption'],Decimal(15))
        self.assertEqual(data['electricity']['rows'],[]);self.assertEqual(data['water']['meters'],[])
        self.assertFalse(data['pm_complete'])
        url=self.generation_url.replace('2026-10','2026-06')
        response=self.client.get(url,headers=self.headers)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        section=tree.get_element_by_id('metsoa-consumption')
        self.assertIn('31.0 kL',section.text_content())
        self.assertIn('Rate unavailable',section.text_content())
        self.assertIn('Water consumption bands',section.text_content())
        self.assertIn('Consumption report',section.text_content())
        self.assertIn('Final amount due unavailable',response.text)
        pdf=self.client.get(url+'&format=pdf&download=1',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        self.assertIn('attachment',pdf.headers['Content-Disposition'])
        content=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn('INCOMPLETE',content);self.assertIn('Rate unavailable',content)
        self.assertIn('CEL888',content)
        self.assertEqual(before,self.snapshot())

    def test_negative_bulk_difference_is_visible_without_negative_charges(self):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=20 WHERE meter_id=5 AND reading_date='2026-10-30'"))
        self.session.commit();self.conn.commit();before=self.snapshot();data=self.data()
        measured=next(r for r in data['pm_consumption_rows'] if r['number']=='ANG489')
        self.assertEqual(measured['bulk_difference'],Decimal(-12))
        self.assertEqual(measured['consumption'],Decimal(20))
        water=next(r for r in data['water']['meters'] if r['meter_number']=='ANG489')
        self.assertEqual(water['summary']['consumption'],0)
        self.assertTrue(all(r['due']>=0 for r in water['ws_breakdown']+water['sd_breakdown']))
        page=self.client.get(self.generation_url,headers=self.headers)
        self.assertNotIn('-12',html.fromstring(page.data).get_element_by_id('metsoa-consumption').text_content())
        self.assertTrue(data['pm_complete']);self.assertEqual(before,self.snapshot())

    def test_missing_fixed_component_cannot_be_hidden_by_other_fixed_rates(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2027-01-01' WHERE code='W_MGT'"))
        self.session.commit();self.conn.commit();data=self.data()
        self.assertFalse(data['pm_complete'])
        self.assertTrue(any(r['component']=='W_MGT' for r in data['pm_unpriced_components']))
        page=self.client.get(self.generation_url,headers=self.headers)
        self.assertIn('W_MGT',html.fromstring(page.data).get_element_by_id('metsoa-unpriced').text_content())
        self.assertIn('Final amount due unavailable',page.text)

    def test_surcharges_use_actual_dated_rates_or_remain_unavailable(self):
        self.session.execute(text("UPDATE bil_tariff SET rate=0.75 WHERE code='WSSurcharge'"))
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='SDSurcharge'"))
        self.session.commit();self.conn.commit();data=self.data()
        water=next(r for r in data['water']['meters'] if r['meter_number']=='CEL888')
        ws=next(r for r in water['ws_breakdown'] if r['desc']=='Water surcharge')
        self.assertEqual(ws['rate'],Decimal('0.75'))
        self.assertEqual(ws['due'],Decimal('23.25'))
        self.assertFalse(any(r['desc']=='Sewerage surcharge' for r in water['sd_breakdown']))
        self.assertFalse(any(r['component']=='Sewerage surcharge' for r in data['pm_unpriced_components']))
        self.assertFalse(any('sewerage surcharge tariff' in i for i in data['pm_issues']))
        self.assertFalse(data['pm_complete'])

    def test_available_consumption_survives_mid_period_tariff_change(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-10-15' WHERE utility_type='electricity'"))
        self.session.commit();self.conn.commit();data=self.data()
        measured=next(r for r in data['pm_consumption_rows'] if r['number']=='2188802S')
        self.assertEqual(measured['consumption'],Decimal(547))
        self.assertEqual(data['electricity']['rows'],[])
        self.assertTrue(any('Rate unavailable for electricity' in s for s in data['pm_issues']))


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(HistoricalReportTests(n) for n in HistoricalReportTests.__dict__ if n.startswith('test_'))
