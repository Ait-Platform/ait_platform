"""Bulk adjustments from canonical, period-matched readings, no historical writes."""
import unittest
from decimal import Decimal
from sqlalchemy import text
import test_pm_municipal_metsoa as fixture
class BulkMetsoaTests(fixture.MunicipalMetsoaTests):
    def setup_bulk(self):
        # Existing property meters, validated monthly pairs, with a future reading ignored.
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
        self.assertEqual(page.status_code,200);self.assertIn('minus linked individual consumption',page.text)
        from io import BytesIO
        from pypdf import PdfReader
        pdf=self.client.get(self.generation_url+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        contents=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn('minus linked individual consumption',contents);self.assertIn('100.64',contents)
        self.assertEqual(before,self.snapshot())
    def test_electricity_adjustment_uses_billing_month_despite_different_dates(self):
        self.session.execute(text('UPDATE bil_meter SET parent_meter_id=4 WHERE id=3'))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-10-24' WHERE meter_id=3 AND reading_date='2026-10-26'"))
        self.session.commit();self.conn.commit();self.confirm_bulk(4,[3],'electricity');before=self.snapshot();data=self.data()
        rows={r['meter_number']:r for r in data['electricity']['rows']}
        self.assertEqual(rows['2188802S']['consumption'],Decimal(532))
        self.assertEqual(rows['7150696S']['consumption'],Decimal(15))
        self.assertEqual(data['electricity']['subtotal'],Decimal('1793.88'))
        self.assertEqual(before,self.snapshot())
    def test_missing_monthly_consumption_identifies_meter_and_reason(self):
        self.setup_bulk()
        self.session.execute(text("DELETE FROM bil_meter_reading WHERE meter_id=1 AND reading_date='2026-10-30'"));self.session.commit();self.conn.commit()
        before=self.snapshot();data=self.data()
        self.assertFalse(data['pm_complete'])
        self.assertIn('linked meter CEL888 monthly consumption for 2026-10 cannot be established',str(data['pm_issues']))
        self.assertIn('Only one reading',str(data['pm_issues']))
        self.assertNotIn('ANG489',[w['meter_number'] for w in data['water']['meters']])
        self.assertEqual(before,self.snapshot())
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(1,'2026-08-30',0)"));self.session.commit();self.conn.commit()
        data=self.data();self.assertFalse(data['pm_complete'])
        self.assertIn('No reading captured in 2026-10',str(data['pm_issues']))
    def test_october_100_minus_20_with_different_dates_is_80_and_pdf_agrees(self):
        self.setup_bulk();self.confirm_bulk(5,[2])
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-09-11' WHERE meter_id=2 AND reading_date='2026-09-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-10-11',reading_value=634 WHERE meter_id=2 AND reading_date='2026-10-30'"));self.session.commit();self.conn.commit()
        before=self.snapshot();data=self.data();self.assertTrue(data['pm_complete'],data['pm_issues'])
        water={w['meter_number']:w for w in data['water']['meters']}
        self.assertEqual(water['ANG489']['summary']['consumption'],Decimal(80))
        self.assertEqual(water['CXN998']['summary']['consumption'],Decimal(20))
        self.assertEqual(water['ANG489']['summary']['last_read'],Decimal(0))
        self.assertEqual(water['CXN998']['summary']['last_read'],Decimal(614))
        self.assertEqual(water['ANG489']['summary']['consumption']+water['CXN998']['summary']['consumption'],Decimal(100))
        self.assertNotEqual(water['ANG489']['summary']['new_date'],water['CXN998']['summary']['new_date'])
        page=self.client.get(self.generation_url,headers=self.headers)
        self.assertEqual(page.status_code,200);self.assertNotIn('INCOMPLETE',page.text)
        from lxml import html
        tree=html.fromstring(page.data)
        total=tree.xpath('//tr[td[contains(text(),"Municipality Utility Charges")]]/td[last()]')[0].text_content().strip()
        self.assertEqual(total,'R%.2f'%data['grand_total'])
        from io import BytesIO
        from pypdf import PdfReader
        pdf=self.client.get(self.generation_url+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        contents=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn(total,contents);self.assertNotIn('INCOMPLETE',contents)
        self.assertEqual(before,self.snapshot())
    def test_multiple_children_with_different_physical_dates_deduct_monthly_usage(self):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-10-21' WHERE meter_id=1 AND reading_date='2026-10-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-09-11' WHERE meter_id=2 AND reading_date='2026-09-30'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date='2026-10-11' WHERE meter_id=2 AND reading_date='2026-10-30'"));self.session.commit();self.conn.commit()
        data=self.data();self.assertTrue(data['pm_complete'],data['pm_issues'])
        self.assertEqual(next(w['summary']['consumption'] for w in data['water']['meters'] if w['meter_number']=='ANG489'),Decimal(68))
    def test_negative_bulk_difference_is_shown_without_negative_charge(self):
        self.setup_bulk();self.session.execute(text("UPDATE bil_meter_reading SET reading_value=20 WHERE meter_id=5 AND reading_date='2026-10-30'"));self.session.commit();self.conn.commit()
        data=self.data();self.assertFalse(data['pm_complete']);self.assertIn('= -12.0 kL',str(data['pm_issues']))
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
