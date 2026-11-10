"""Bulk adjustments from canonical, period-matched readings, no historical writes."""
import unittest
from decimal import Decimal
from sqlalchemy import text
import test_pm_municipal_metsoa as fixture
class BulkMetsoaTests(fixture.MunicipalMetsoaTests):
    def setup_bulk(self):
        # Existing property meters, validated monthly pairs, with a future reading ignored.
        # A fully priced bulk fixture needs the now-supported fifth sewerage band.
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) VALUES('sewerage','S05','Above 1500',34.78,1501,NULL,999,'kL/day','2025-07-01')"))
        self.session.execute(text("UPDATE bil_meter SET parent_meter_id=5 WHERE id IN(1,2)"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-09-30' WHERE meter_id IN(1,2) AND reading_date<'2026-10-01'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-10-30' WHERE meter_id IN(1,2,5) AND reading_date BETWEEN '2026-10-01' AND '2026-10-31'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=100 WHERE meter_id=5"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2026-09-30',0)"))
        self.session.commit();self.conn.commit()
        self.confirm_bulk(5,[1,2])
    def confirm_bulk(self,parent,children,utility='water'):
        from app.program_billing.bulk_meter_linking import save
        from werkzeug.datastructures import MultiDict
        with self.app.test_request_context():save(26,MultiDict([('parent_id',str(parent)),('utility',utility)]+[('children',str(c)) for c in children]))
        self.session.commit();self.conn.commit()
    def data(self):
        with self.app.test_request_context():return self.metsoa.build(26,'2026-10',recipient_id=11)
    def test_water_adjustment_preserves_fixed_fees_individuals_and_history(self):
        self.setup_bulk();before=self.snapshot();data=self.data()
        self.assertTrue(data['pm_complete'],data['pm_issues'])
        water={w['meter_number']:w for w in data['water']['meters']}
        self.assertEqual(water['ANG489']['summary']['consumption'],Decimal(68))
        self.assertEqual(water['CEL888']['summary']['consumption'],Decimal(31))
        self.assertEqual(water['CXN998']['summary']['consumption'],Decimal(1))
        self.assertEqual(sum(w['summary']['consumption'] for w in water.values()),Decimal(100))
        for w in water.values():
            self.assertEqual(next(r['due'] for r in w['ws_breakdown'] if r['desc']=='Water Loss Levy (insurance)'),Decimal('19.30'))
        bulk=water['ANG489']
        self.assertEqual(next(r['due'] for r in bulk['ws_breakdown'] if r['desc']=='Water surcharge'),Decimal('100.64'))
        page=self.client.get(self.generation_url,headers=self.headers)
        self.assertEqual(page.status_code,200);self.assertNotIn('minus linked individual consumption',page.text)
        from io import BytesIO
        from pypdf import PdfReader
        pdf=self.client.get(self.generation_url+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        contents=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertNotIn('minus linked individual consumption',contents);self.assertIn('100.64',contents)
        self.assertEqual(before,self.snapshot())
    def test_electricity_incompatible_intervals_omit_bulk_only(self):
        self.confirm_bulk(4,[3],'electricity')
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-10-24' WHERE meter_id=3 AND reading_date='2026-10-26'"))
        self.session.commit();self.conn.commit();before=self.snapshot();data=self.data()
        rows={r['meter_number']:r for r in data['electricity']['rows']}
        self.assertNotIn('2188802S',rows)
        self.assertEqual(rows['7150696S']['consumption'],Decimal(15))
        self.assertFalse(data['pm_complete'])
        self.assertIn('Reading Date Mismatch',str(data['pm_issues']))
        self.assertEqual(before,self.snapshot())

    def test_missing_monthly_consumption_identifies_meter_and_reason(self):
        self.setup_bulk()
        self.session.execute(text("DELETE FROM bil_meter_reading WHERE meter_id=1 AND reading_date='2026-10-30'"));self.session.commit();self.conn.commit()
        before=self.snapshot();data=self.data()
        self.assertFalse(data['pm_complete'])
        self.assertIn('linked meter CEL888 monthly consumption for 2026-10 cannot be established',str(data['pm_issues']))
        self.assertIn('No reading captured in 2026-10.',str(data['pm_issues']))
        self.assertTrue(next(w for w in data['water']['meters'] if w['meter_number']=='ANG489')['consumption_unavailable'])
        self.assertEqual(before,self.snapshot())
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(1,'2026-08-30',0)"));self.session.commit();self.conn.commit()
        data=self.data();self.assertFalse(data['pm_complete'])
        self.assertIn('No reading captured in 2026-10',str(data['pm_issues']))
    def test_water_incompatible_intervals_omit_bulk_and_pdf_is_not_final(self):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-09-11' WHERE meter_id=2 AND reading_date='2026-09-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-10-11' WHERE meter_id=2 AND reading_date='2026-10-30'"))
        self.session.commit();self.conn.commit();before=self.snapshot();data=self.data()
        water={w['meter_number']:w for w in data['water']['meters']}
        self.assertTrue(water['ANG489']['consumption_unavailable'])
        self.assertIn('CXN998',water)
        self.assertFalse(data['pm_complete'])
        self.assertIn('Reading Date Mismatch',str(data['pm_issues']))
        page=self.client.get(self.generation_url,headers=self.headers)
        self.assertIn('INCOMPLETE',page.text)
        self.assertIn('Final amount due unavailable',page.text)
        from io import BytesIO
        from pypdf import PdfReader
        pdf=self.client.get(self.generation_url+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        contents=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn('INCOMPLETE',contents)
        self.assertEqual(before,self.snapshot())

    def test_mismatch_identifies_previous_current_or_both_dates(self):
        self.setup_bulk()
        for previous, current, label in [
            ('2026-09-29', '2026-10-30', 'Previous date'),
            ('2026-09-30', '2026-10-29', 'Current date'),
            ('2026-09-29', '2026-10-29', 'Previous date, Current date'),
        ]:
            with self.subTest(label=label):
                self.session.execute(text("UPDATE bil_meter_reading SET reading_date=:d WHERE meter_id=1 AND reading_date<'2026-10-01'"),dict(d=previous))
                self.session.execute(text("UPDATE bil_meter_reading SET reading_date=:d WHERE meter_id=1 AND reading_date BETWEEN '2026-10-01' AND '2026-10-31'"),dict(d=current))
                self.session.commit();self.conn.commit();before=self.snapshot()
                data=self.data()
                issue=next(i for i in data['pm_issues'] if i.startswith('Reading Date Mismatch'))
                self.assertIn('Reading Date Mismatch — ANG489',issue)
                self.assertIn('Bulk meter: ANG489\nReading period: 2026-09-30 to 2026-10-30',issue)
                self.assertIn('Linked meter: CEL888\nReading period: '+previous+' to '+current,issue)
                self.assertIn('Dates differing: '+label+'.',issue)
                self.assertIn('Correction required: The reading dates do not match.',issue)
                self.assertIn('METSOA cannot calculate the final amount until the discrepancy is resolved.',issue)
                self.assertFalse(data['pm_pricing_complete']);self.assertFalse(data['pm_complete'])
                self.assertEqual(before,self.snapshot())

    def test_almond_july_opening_mismatch_screen_pdf_and_corrected_date(self):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_meter SET meter_number='CWA074_CEL888' WHERE id=1"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-07-31' WHERE meter_id IN (1,2,5) AND reading_date='2026-09-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-08-31' WHERE meter_id IN (1,2,5) AND reading_date='2026-10-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-07-30' WHERE meter_id=1 AND reading_date='2026-07-31'"))
        self.session.commit();self.conn.commit();before=self.snapshot()
        url=self.generation_url.replace('2026-10','2026-08')
        page=self.client.get(url,headers=self.headers)
        self.assertEqual(page.status_code,200)
        from lxml import html
        warning=html.fromstring(page.data).xpath('//p[contains(text(), "Reading Date Mismatch")]')[0]
        self.assertEqual(warning.get('style'),'white-space: pre-line')
        for value in ('ANG489','CWA074_CEL888','2026-07-30','2026-07-31','2026-08-31','Dates differing: Previous date.'):
            self.assertIn(value,warning.text_content())
        self.assertIn('NOT A FINAL AMOUNT DUE',page.text)
        self.assertNotIn('Amount Due R',page.text)
        from io import BytesIO
        from pypdf import PdfReader
        pdf=self.client.get(url+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        contents=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        for value in ('Reading Date Mismatch','CWA074_CEL888','2026-07-30','2026-07-31','2026-08-31','NOT A FINAL AMOUNT DUE'):
            self.assertIn(value,contents)
        self.assertEqual(before,self.snapshot())
        # Only the test fixture corrects the saved date; report generation never does.
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-07-31' WHERE meter_id=1 AND reading_date='2026-07-30'"))
        self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-08',recipient_id=11)
        self.assertFalse(any('Reading Date Mismatch' in i for i in data['pm_issues']))
        bulk=next(w for w in data['water']['meters'] if w['meter_number']=='ANG489')
        self.assertEqual(bulk['summary']['consumption'],Decimal(68))

    def test_negative_bulk_difference_is_shown_without_negative_charge(self):
        self.setup_bulk();self.session.execute(text("UPDATE bil_meter_reading SET reading_value=20 WHERE meter_id=5 AND reading_date='2026-10-30'"));self.session.commit();self.conn.commit()
        data=self.data();self.assertTrue(data['pm_complete']);self.assertIn('= -12.0 kL',str(data['pm_bulk_notes']))
        water=next(w for w in data['water']['meters'] if w['meter_number']=='ANG489')
        self.assertEqual(water['summary']['consumption'],0)
        self.assertEqual(next(r['due'] for r in water['ws_breakdown'] if r['desc']=='Water surcharge'),0)
        self.assertEqual(next(r['due'] for r in water['ws_breakdown'] if r['desc']=='Water Loss Levy (insurance)'),Decimal('19.30'))
    def test_unconfirmed_legacy_links_do_not_make_any_meter_bulk(self):
        self.session.execute(text('UPDATE bil_meter SET parent_meter_id=4 WHERE id=3'))
        self.session.commit();self.conn.commit()
        data=self.data();rows={r['meter_number']:r for r in data['electricity']['rows']}
        self.assertEqual(rows['2188802S']['consumption'],Decimal(547))
        self.assertEqual(rows['7150696S']['consumption'],Decimal(15))
        self.assertEqual(data['pm_bulk_notes'],[])

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(BulkMetsoaTests(name) for name in BulkMetsoaTests.__dict__ if name.startswith('test_'))
