"""Dated consumption allocation preserves monthly fees, records and completeness."""
import unittest
from datetime import date
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from pypdf import PdfReader
from sqlalchemy import text
from app.program_billing.metsoa_intervals import slices
import test_pm_metsoa_bulk as fixture

class IntervalDayTests(unittest.TestCase):
    def test_july_day_allocation_and_exact_consumption_conservation(self):
        parts=slices(date(2026,6,15),date(2026,7,15),Decimal(30))
        self.assertEqual([r['days'] for r in parts],[15,15]);self.assertEqual([r['consumption'] for r in parts],[15,15])
        parts=slices(date(2026,6,1),date(2026,7,1),Decimal(30))
        self.assertEqual([r['days'] for r in parts],[29,1]);self.assertEqual(sum(r['consumption'] for r in parts),30)
        fractional=slices(date(2026,6,19),date(2026,7,18),Decimal(31))
        self.assertEqual(sum(r['consumption'] for r in fractional),31)
        parts=slices(date(2026,5,31),date(2026,6,30),Decimal(100))
        self.assertEqual(len(parts),1);self.assertEqual(parts[0]['end'],date(2026,6,30))

class IntervalStatementTests(fixture.BulkMetsoaTests):
    def setup_period(self,month):
        self.setup_bulk()
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2025-07-01'"))
        self.session.execute(text("UPDATE bil_tariff SET rate=2 WHERE utility_type='electricity'"))
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) SELECT utility_type,code,description,CASE WHEN code='W_MGT' THEN 200 WHEN code='S_REFUSE' THEN 250 WHEN code='W_LOSS' THEN 21 ELSE rate*2 END,block_start,block_end,reduction_factor,unit,'2026-07-01' FROM bil_tariff"))
        opening='2026-05-31' if month=='2026-06' else '2026-06-15';closing='2026-06-30' if month=='2026-06' else '2026-07-15'
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date=:opening WHERE reading_date BETWEEN '2026-09-01' AND '2026-09-30'"),dict(opening=opening))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date=:closing WHERE reading_date BETWEEN '2026-10-01' AND '2026-10-31'"),dict(closing=closing))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=30055 WHERE meter_id=4 AND reading_date=:closing"),dict(closing=closing))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=49587 WHERE meter_id=3 AND reading_date=:closing"),dict(closing=closing))
        self.session.commit();self.conn.commit()
        self.period=month;self.url=self.generation_url.replace('2026-10',month)
    def statement(self):
        with self.app.test_request_context():return self.metsoa.build(26,self.period,recipient_id=11)
    def assert_fees(self,data,management,refuse):
        for description,side,expected in [('Management Fee','ws',management),('Refuse','sd',refuse)]:
            charges=[(w['meter_number'],r['due']) for w in data['water']['meters'] for r in w[side+'_breakdown'] if r['desc']==description]
            self.assertEqual(charges,[('ANG489',Decimal(expected))])
    def test_statement_30_june_uses_earlier_year_and_prices_bulk_monthly_fees(self):
        self.setup_period('2026-06');before=self.snapshot();rates=list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        data=self.statement();self.assertEqual(data['pm_statement_date'],date(2026,6,30))
        self.assertEqual(data['pm_status'],'FINAL',data['pm_issues']);self.assert_fees(data,'144.54','206.93')
        self.assertTrue(all(r['rate']==2 for r in data['electricity']['rows']))
        self.assertEqual(data['electricity']['subtotal'],80)
        pdf=self.client.get(self.url+'&format=pdf',headers=self.headers)
        content=' '.join(' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages).split())
        self.assertIn('2026-06-30',content);self.assertNotIn('INCOMPLETE',content)
        self.assertIn('144.54',content);self.assertIn('206.93',content);self.assertNotIn('4.0000',content)
        output=Path('tmp/metsoa-interval-review');output.mkdir(parents=True,exist_ok=True);(output/'june.pdf').write_bytes(pdf.data)
        import fitz
        document=fitz.open(stream=pdf.data,filetype='pdf')
        for i,p in enumerate(document):p.get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(str(output/('june-'+str(i+1)+'.png')))

        self.assertEqual(before,self.snapshot());self.assertEqual(rates,list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))
    def test_cross_july_prorates_electricity_and_tiered_water_without_duplicate_fees(self):
        self.setup_period('2026-07');before=self.snapshot();data=self.statement()
        self.assertEqual(data['pm_status'],'FINAL',data['pm_issues']);self.assert_fees(data,'200','250')
        electricity=next(r for r in data['electricity']['rows'] if r['meter_number']=='2188802S')
        self.assertEqual(electricity['consumption'],30);self.assertEqual(electricity['due'],90)
        self.assertEqual([r['days'] for r in electricity['rate_segments']],[15,15])
        self.assertEqual([r['rate'] for r in electricity['rate_segments']],[2,4])
        water=next(r for r in data['water']['meters'] if r['meter_number']=='CXN998')
        tier_rows=[r for r in water['ws_breakdown'] if r['desc'].startswith('W01')]
        self.assertEqual([r['cons'] for r in tier_rows],[Decimal('.5'),Decimal('.5')])
        self.assertEqual([r['rate'] for r in tier_rows],[Decimal('39.39'),Decimal('78.78')])
        self.assertEqual([r['due'] for r in tier_rows],[Decimal('19.70'),Decimal('39.39')])
        self.assertEqual(data['grand_total'],data['electricity']['subtotal']+sum(w['total'] for w in data['water']['meters']))
        self.assertEqual(before,self.snapshot())
        pdf=self.client.get(self.url+'&format=pdf',headers=self.headers);self.assertEqual(pdf.status_code,200)
        content=' '.join(' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages).split())
        self.assertNotIn('INCOMPLETE',content);self.assertIn('2.0000',content);self.assertIn('4.0000',content)
        self.assertIn('2026-06-16 to 2026-06-30',content);self.assertIn('2026-07-01 to 2026-07-15',content)
        output=Path('tmp/metsoa-interval-review');output.mkdir(parents=True,exist_ok=True);(output/'july.pdf').write_bytes(pdf.data)
        import fitz
        doc=fitz.open(stream=pdf.data,filetype='pdf')
        for i,p in enumerate(doc):p.get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(str(output/('july-'+str(i+1)+'.png')))
    def test_missing_prior_rate_keeps_incomplete_but_monthly_bulk_fees_are_priced(self):
        self.setup_period('2026-07')
        self.session.execute(text("DELETE FROM bil_tariff WHERE utility_type='water' AND code NOT LIKE '%Surcharge%' AND effective_date='2025-07-01'"));self.session.commit();self.conn.commit()
        data=self.statement();self.assertEqual(data['pm_status'],'INCOMPLETE');self.assert_fees(data,'200','250')
        bulk=next(w for w in data['water']['meters'] if w['meter_number']=='ANG489')
        self.assertTrue(bulk['consumption_unavailable']);self.assertIn('2026-06-16',str(data['pm_issues']))
        self.assertNotIn('could not be priced',str(data['pm_issues']))
    def test_missing_prior_surcharge_preserves_available_tier_charges_and_incomplete(self):
        self.setup_period('2026-07')
        self.session.execute(text("DELETE FROM bil_tariff WHERE code='WSSurcharge' AND effective_date='2025-07-01'"));self.session.commit();self.conn.commit()
        data=self.statement();self.assertEqual(data['pm_status'],'INCOMPLETE');self.assert_fees(data,'200','250')
        water=next(w for w in data['water']['meters'] if w['meter_number']=='CXN998')
        self.assertTrue(any(r['desc'].startswith('W01 (2026-06') for r in water['ws_breakdown']))
        self.assertIn('water surcharge from 2026-06-16',str(data['pm_issues']))

    def test_confirmed_bulk_root_wins_over_legacy_account_meter_pointer(self):
        self.setup_period('2026-06')
        self.session.execute(text('UPDATE bil_muni_account SET water_meter_id=1,is_bulk_account=true WHERE id=1'));self.session.commit();self.conn.commit()
        before=self.snapshot();data=self.statement()
        self.assertEqual(data['pm_status'],'FINAL',data['pm_issues']);self.assert_fees(data,'144.54','206.93')
        self.assertEqual(before,self.snapshot())

    def test_prior_year_estimate_remains_estimated_across_july(self):
        self.setup_period('2026-07')
        from types import SimpleNamespace
        from app.program_billing.tariff_substitutes import reference
        current=self.session.execute(text("SELECT id FROM bil_tariff WHERE code='W01' AND effective_date='2026-07-01'")).scalar()
        provenance=reference(SimpleNamespace(id=current,effective_date=date(2026,7,1)),date(2025,7,1),date(2026,11,7))
        self.session.execute(text('CREATE TEMP TABLE bil_tariff_verification(tariff_id integer PRIMARY KEY,municipal_category text,source_reference text)'))
        self.session.execute(text("INSERT INTO bil_tariff_verification SELECT id,NULL,:ref FROM bil_tariff WHERE code='W01' AND effective_date='2025-07-01'"),dict(ref=provenance))
        self.session.commit();self.conn.commit();before=self.snapshot()
        data=self.statement();self.assertEqual(data['pm_status'],'ESTIMATED');self.assertFalse(data['pm_complete'])
        self.assertTrue(any(r['code']=='W01' for r in data['pm_estimated_components']))
        page=self.client.get(self.url,headers=self.headers)
        self.assertIn('historical tariff verification pending.',page.text);self.assertNotIn('INCOMPLETE',page.text)
        self.assertEqual(before,self.snapshot())

    def test_missing_required_fixed_charge_still_prevents_final(self):
        self.setup_period('2026-07');self.session.execute(text("DELETE FROM bil_tariff WHERE code='W_MGT'"));self.session.commit();self.conn.commit()
        data=self.statement();self.assertEqual(data['pm_status'],'INCOMPLETE')
        self.assertIn('W_MGT',str(data['pm_unpriced_components']))
        self.assertTrue(all(r['calculated'] is None for r in data['pm_comparisons']))


def load_tests(loader,tests,pattern):return unittest.TestSuite([loader.loadTestsFromTestCase(IntervalDayTests),unittest.TestSuite(IntervalStatementTests(n) for n in IntervalStatementTests.__dict__ if n.startswith('test_'))])
