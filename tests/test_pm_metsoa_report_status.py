"""Warnings do not block complete calculations; estimates never become verified."""
import unittest
from decimal import Decimal
from io import BytesIO
from lxml import html
from pypdf import PdfReader
from sqlalchemy import text
import test_pm_metsoa_bulk as fixture

class ReportStatusTests(fixture.BulkMetsoaTests):
    def negative_difference(self):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=82 WHERE meter_id=5 AND reading_date='2026-10-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=50 WHERE meter_id=1 AND reading_date='2026-10-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=649 WHERE meter_id=2 AND reading_date='2026-10-30'"))
        self.session.commit();self.conn.commit()
    def assert_screen_pdf_status(self,status,override=False):
        url=self.generation_url+('&use_current_tariffs=1' if override else '')
        page=self.client.get(url,headers=self.headers);self.assertEqual(page.status_code,200,page.text)
        self.assertEqual(html.fromstring(page.data).get_element_by_id('metsoa-report-status').text_content(),'(Report status: '+status+')')
        pdf=self.client.get(url+'&format=pdf',headers=self.headers);self.assertEqual(pdf.status_code,200)
        content=' '.join(' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages).split())
        self.assertIn('Report status: '+status,content)
        return page.text,content
    def test_verified_complete_is_final(self):
        self.setup_bulk();data=self.data()
        self.assertEqual(data['pm_status'],'FINAL');self.assertTrue(data['pm_complete'])
        page,pdf=self.assert_screen_pdf_status('FINAL')
        self.assertNotIn('INCOMPLETE',page);self.assertIn('Amount Due R',page)
    def test_negative_three_is_a_warning_not_an_incomplete_calculation(self):
        self.negative_difference();before=self.snapshot()
        audit=list(self.session.execute(text('SELECT * FROM bil_utilities_capture_audit ORDER BY id')))
        data=self.data()
        self.assertEqual(data['pm_status'],'FINAL');self.assertTrue(data['pm_complete'])
        self.assertEqual(data['pm_issues'],[])
        self.assertIn('bulk consumption 82.0 minus linked individual consumption 85.0 = -3.0 kL',str(data['pm_bulk_notes']))
        self.assertIn('no automatic refund',str(data['pm_bulk_notes']))
        measured=next(r for r in data['pm_consumption_rows'] if r['number']=='ANG489')
        self.assertEqual(measured['consumption'],Decimal(82));self.assertEqual(measured['bulk_difference'],Decimal(-3))
        priced=next(w for w in data['water']['meters'] if w['meter_number']=='ANG489')
        self.assertEqual(priced['summary']['consumption'],0) # Existing pricing floor unchanged.
        self.assertTrue(all(r['due']>=0 for r in priced['ws_breakdown']+priced['sd_breakdown']))
        page,pdf=self.assert_screen_pdf_status('FINAL')
        self.assertIn('-3.0 kL',page);self.assertIn('-3.0 kL',pdf);self.assertNotIn('INCOMPLETE',page)
        self.assertEqual(before,self.snapshot())
        self.assertEqual(audit,list(self.session.execute(text('SELECT * FROM bil_utilities_capture_audit ORDER BY id'))))
        self.assertEqual(self.client.get(self.generation_url.replace('/26/','/28/'),headers=self.headers).status_code,403)
    def test_missing_rate_still_blocks_final_even_with_negative_difference(self):
        self.negative_difference()
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='WSSurcharge'"));self.session.commit();self.conn.commit()
        data=self.data();self.assertEqual(data['pm_status'],'INCOMPLETE');self.assertFalse(data['pm_complete'])
        page,pdf=self.assert_screen_pdf_status('INCOMPLETE')
        self.assertIn('Rate unavailable',page);self.assertIn('NOT A FINAL AMOUNT DUE',pdf)
    def test_missing_reading_still_blocks_final(self):
        self.negative_difference()
        self.session.execute(text("DELETE FROM bil_meter_reading WHERE meter_id=1 AND reading_date='2026-10-30'"));self.session.commit();self.conn.commit()
        data=self.data();self.assertEqual(data['pm_status'],'INCOMPLETE');self.assertFalse(data['pm_complete'])
        page,pdf=self.assert_screen_pdf_status('INCOMPLETE')
        self.assertIn('No reading captured',page);self.assertIn('NOT A FINAL AMOUNT DUE',pdf)
    def test_estimate_with_negative_difference_is_estimated_not_verified(self):
        self.negative_difference()
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-11-01'"));self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10',recipient_id=11,use_current_tariffs=True)
        self.assertEqual(data['pm_status'],'ESTIMATED');self.assertTrue(data['pm_pricing_complete']);self.assertFalse(data['pm_complete'])
        page,pdf=self.assert_screen_pdf_status('ESTIMATED',True)
        self.assertIn('HISTORICAL ESTIMATE',pdf);self.assertIn('CURRENT TARIFFS APPLIED',pdf)
        self.assertNotIn('Amount Due R',page);self.assertNotIn('INCOMPLETE',page)
        self.assertIn('-3.0 kL',pdf)
    def test_partial_estimate_still_identifies_missing_components(self):
        self.negative_difference()
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-11-01'"))
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='WSSurcharge'"));self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10',recipient_id=11,use_current_tariffs=True)
        self.assertEqual(data['pm_status'],'ESTIMATED (INCOMPLETE)');self.assertFalse(data['pm_complete'])
        page,pdf=self.assert_screen_pdf_status('ESTIMATED (INCOMPLETE)',True)
        self.assertIn('Rate unavailable',page);self.assertIn('HISTORICAL ESTIMATE',pdf)
        self.assertNotIn('Amount Due R',page)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReportStatusTests(n) for n in ReportStatusTests.__dict__ if n.startswith('test_'))
