"""Sewerage regressions; database integration uses temporary fixture tables only."""
import ast
import importlib.util
import unittest
from pathlib import Path
from decimal import Decimal as D

spec = importlib.util.spec_from_file_location('sewerage', 'app/program_billing/metsoa_sewerage.py')
sd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sd)

class SewerageDischargeTests(unittest.TestCase):
    def setUp(self):
        self.verified = dict(verified=True, category='domestic_not_exceeding_two_storeys',
                             tank_arrangement='no_individual_break_pressure_tank',
                             first_band_exempt=False, tariff_category_confirmed=True,
                             source_reference='Isolated category fixture, not Almond verification')
        self.tiers = [dict(code=c, start=i, rate=D(r), label=c, multiplier=D('999'))
                      for i, (c, r) in enumerate(zip(sd.CODES, ('6.05','10.21','19.47','30.39','34.78')))]
    def test_blz080_32_days_175_kl(self):
        rows,total=sd.calculate(175,32,self.tiers,D('1.48'),self.verified)
        self.assertEqual([r['cons'] for r in rows[:5]],list(map(D,('6.080','15.192','4.008','10.400','76.200'))))
        self.assertEqual(sum(r['due'] for r in rows[:5]),D('3236.23'))
        self.assertEqual(rows[-1]['cons'],D('111.880'))
        self.assertEqual(rows[-1]['due'],D('165.58'))
        self.assertEqual(total,D('3401.81'))
        self.assertEqual(rows[3]['water_cons'],D('16'))
    def test_no_category_assumed(self):
        for v in (None,{},dict(self.verified,verified=False),dict(self.verified,category='tall_development'),dict(self.verified,source_reference=''),dict(self.verified,tariff_category_confirmed=False)):
            self.assertTrue(sd.readiness(self.tiers,v))
            self.assertEqual(sd.calculate(175,32,self.tiers,D('1.48'),v),([],D('0.00')))
    def test_missing_fifth_not_substituted(self):
        rows,total=sd.calculate(175,32,self.tiers[:-1],None,self.verified)
        self.assertIsNone(rows[4]['rate'])
        self.assertIsNone(rows[4]['due'])
        self.assertTrue(rows[4]['unpriced'])
        self.assertGreater(total,0)
        self.assertIn('S05',str(sd.readiness(self.tiers[:-1],self.verified,175,32)))
        self.assertFalse(sd.readiness(self.tiers[:-1],self.verified,20,32))
    def test_missing_used_band_does_not_hide_other_charges(self):
        rows,total=sd.calculate(175,32,self.tiers[1:],D('1.48'),self.verified)
        self.assertTrue(rows[0]['unpriced'])
        self.assertIsNone(rows[0]['due'])
        self.assertEqual(rows[3]['due'],D('316.06'))
        self.assertEqual(rows[-1]['due'],D('165.58'))
        self.assertGreater(total,0)
        self.assertIn('S01',str(sd.readiness(self.tiers[1:],self.verified,175,32)))

    def test_confirmed_profile_is_not_inferred_from_property_name(self):
        self.assertIsNone(sd.property_verification(dict(id=99,name='Almond'),[dict(account_number='83192896040')]))
        self.assertIsNone(sd.property_verification(dict(id=26,name='Almond'),[dict(account_number='OTHER')]))

    def test_exempt_volume_not_surcharged(self):
        rows,total=sd.calculate(175,32,self.tiers,D('1.48'),dict(self.verified,first_band_exempt=True))
        self.assertEqual(rows[0]['due'],0)
        self.assertEqual(rows[-1]['cons'],D('105.800'))
        self.assertEqual(rows[-1]['due'],D('156.58'))
    def test_dated_rate_inputs_preserved(self):
        rows,_=sd.calculate(175,32,self.tiers,None,self.verified)
        older=[dict(t,rate=t['rate']/2) for t in self.tiers]
        old,_=sd.calculate(175,32,older,None,self.verified)
        self.assertEqual([r['cons'] for r in rows],[r['cons'] for r in old])
        self.assertEqual(old[-1]['rate'],D('17.39'))
        self.assertEqual(rows[-1]['rate'],D('34.78'))
    def test_invalid_inputs_rejected(self):
        for value,days in ((-1,32),('NaN',32),(175,0)):
            with self.assertRaises(ValueError):sd.calculate(value,days,self.tiers,None,self.verified)
    def test_real_builders_and_interval_pricing(self):
        import sys
        from types import SimpleNamespace, ModuleType
        from unittest.mock import patch
        from datetime import date
        tree=ast.parse(Path('app/program_billing/routes.py').read_text(encoding='utf-8'))
        names={'build_water_rows','build_ws_sd_rows_for_meter','build_ws_breakdown','build_sd_breakdown','build_water_fee_block'}
        env={}
        exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),'routes.py','exec'),env)
        interval_spec=importlib.util.spec_from_file_location('app.program_billing.metsoa_intervals','app/program_billing/metsoa_intervals.py')
        intervals=importlib.util.module_from_spec(interval_spec);interval_spec.loader.exec_module(intervals)
        municipal=ModuleType('app.program_billing.municipal_metsoa')
        municipal.applicable_tariff_records=lambda *args:[]
        rates=dict(water=[dict(start=0,multiplier=D(999),rate=D(1),label='Water')],sewerage=self.tiers,
                   water_surcharge=D('1.48'),sewerage_surcharge=D('1.48'),sewerage_surcharge_applicable=True,
                   water_fixed=[],sewerage_fixed=[],records=[],electricity=None)
        # Synthetic dates give the confirmed 32-day interval; not asserted as Almond dates.
        r=SimpleNamespace(meter_id=1,meter_number='BLZ080_DBC933',utility_type='water',
                          last_date=date(2026,5,29),new_date=date(2026,6,30),days=32,
                          last_read=D(0),new_read=D(175),consumption=D(175),property_fee_meter_id=None)
        modules={'app.program_billing.metsoa_sewerage':sd,'app.program_billing.municipal_metsoa':municipal}
        with patch.dict(sys.modules,modules):
            line,_,issues=intervals.price(r,26,'2026-06',rates,lambda d:(rates,[]),(None,env['build_water_rows']),[])
            self.assertTrue(issues)
            self.assertTrue(line['sd_unavailable'])
            self.assertGreater(line['ws_cost'],0)
            self.assertEqual(line['sd_cost'],0)
            r.sewerage_verification=self.verified
            line,_,issues=intervals.price(r,26,'2026-06',rates,lambda d:(rates,[]),(None,env['build_water_rows']),[])
            self.assertFalse(issues)
            self.assertEqual(line['sd_cost'],D('3401.81'))
            self.assertFalse(line['sd_unavailable'])
            self.assertEqual(r.consumption,D(175))
            # Legacy daily multipliers must not determine sewer discharge widths.
            for t in rates['sewerage']:t['multiplier']=D('.001')
            line,_,issues=intervals.price(r,26,'2026-06',rates,lambda d:(rates,[]),(None,env['build_water_rows']),[])
            self.assertFalse(issues)
            self.assertEqual(line['sd_cost'],D('3401.81'))
    def test_small_consumption_does_not_overallocate(self):
        rows,_=sd.calculate(1,32,self.tiers,None,self.verified)
        self.assertEqual(sum(r['water_cons'] for r in rows),D(1))
        self.assertEqual(rows[0]['cons'],D('.95'))
        self.assertEqual(len(rows),1)

class SewerageStatementGuardTests(unittest.TestCase):
    def test_unverified_category_never_final_on_real_statement(self):
        import test_pm_municipal_metsoa as fixture
        from sqlalchemy import text
        fx=fixture.MunicipalMetsoaTests(methodName='test_permissions_and_token_free_pm_access_remain_unchanged')
        fx.setUp()
        self.addCleanup(fx.doCleanups)
        self.addCleanup(fx.tearDown)
        # Only temporary PostgreSQL fixture tables are modified.
        fx.session.execute(text("UPDATE bil_property SET name='Unverified property' WHERE id=26"))
        fx.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2026-09-25',10)"))
        fx.session.commit();fx.conn.commit()
        with fx.app.test_request_context():data=fx.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertFalse(data['pm_complete'])
        self.assertIn('INCOMPLETE',data['pm_status'])
        self.assertIn('category, tank arrangement, exemption status',str(data['pm_issues']))
        self.assertGreater(data['water']['total'],0)
        page=fx.client.get(fx.generation_url,headers=fx.headers)
        self.assertEqual(page.status_code,200)
        self.assertIn('Rate unavailable',page.text)
        self.assertIn('NOT A FINAL AMOUNT DUE',page.text)

    def fixture(self):
        import test_pm_municipal_metsoa as fixture
        fx=fixture.MunicipalMetsoaTests(methodName='test_permissions_and_token_free_pm_access_remain_unchanged')
        fx.setUp()
        self.addCleanup(fx.doCleanups);self.addCleanup(fx.tearDown)
        return fx

    def seven_meter_fixture(self):
        from sqlalchemy import text
        from lxml import html
        fx=self.fixture()
        for i in (6,7,8,9):
            fx.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(:i,26,:n,'Water','83192896040')"),dict(i=i,n='WATER-'+str(i)))
        fx.session.execute(text("UPDATE bil_meter SET meter_number='BLZ080_DBC933' WHERE id=1"))
        ids=(1,2,5,6,7,8,9)
        fx.session.execute(text("DELETE FROM bil_meter_reading WHERE meter_id IN (1,2,5,6,7,8,9)"))
        for i in ids:
            volume=175 if i==1 else 1000 if i==5 else 10
            for day,value in [('2026-09-29',0),('2026-10-31',volume)]:
                fx.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:i,:d,:v)'),dict(i=i,d=day,v=value))
        fx.session.commit();fx.conn.commit()
        # Five confirmed individual links plus a separately metered water supply.
        from app.program_billing.bulk_meter_linking import save
        from werkzeug.datastructures import MultiDict
        with fx.app.test_request_context():save(26,MultiDict([('parent_id','5'),('utility','water')]+[('children',str(i)) for i in (1,2,6,7,8)]))
        fx.session.commit();fx.conn.commit()
        return fx

    def test_seven_meters_real_route_partial_prices_and_no_writes(self):
        from sqlalchemy import text
        from lxml import html
        fx=self.seven_meter_fixture()
        before=fx.snapshot();rates=list(fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        with fx.app.test_request_context():data=fx.metsoa.build(26,'2026-10',recipient_id=11)
        water=data['water']['meters'];self.assertEqual(len(water),7)
        self.assertTrue(all(any(r.get('code')=='S01' and r['due']>0 for r in w['sd_breakdown']) for w in water))
        blz=next(w for w in water if w['meter_number']=='BLZ080_DBC933')
        bands=[r for r in blz['sd_breakdown'] if r.get('code')]
        self.assertEqual([r['cons'] for r in bands],list(map(D,('6.080','15.192','4.008','10.400','76.200'))))
        self.assertEqual([r['rate'] for r in bands[:4]],list(map(D,('5.45','9.20','17.54','27.38'))))
        self.assertEqual(sum(r['due'] for r in bands[:4]),D('527.96'))
        self.assertIsNone(bands[4]['due'])
        self.assertEqual(next(r['due'] for r in blz['sd_breakdown'] if r['desc']=='Sewerage surcharge'),D('165.58'))
        self.assertTrue(any(r['desc']=='Refuse' and r['due']==D('206.93') for w in water for r in w['sd_breakdown']))
        self.assertFalse(data['pm_complete']);self.assertIn('INCOMPLETE',data['pm_status'])
        self.assertIn('S05',str(data['pm_issues']))
        self.assertNotIn('category, tank arrangement',str(data['pm_issues']))
        page=fx.client.get(fx.generation_url,headers=fx.headers);self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        sections=tree.xpath('//section[contains(@class,"details")]')
        self.assertEqual(len(sections),7)
        self.assertTrue(all('S01' in section.text_content() for section in sections))
        blz_section=next(s for s in sections if 'BLZ080_DBC933' in s.text_content())
        self.assertIn('Rate unavailable',blz_section.text_content())
        self.assertIn('284.75',blz_section.text_content())
        self.assertIn('Final amount due unavailable',page.text)
        self.assertEqual(before,fx.snapshot());self.assertEqual(rates,list(fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))
        self.assertEqual(fx.client.get(fx.generation_url.replace('/26/','/28/'),headers=fx.headers).status_code,403)

    def test_functional_report_screen_and_pdf_without_classification_gate(self):
        from sqlalchemy import text
        from io import BytesIO
        from pypdf import PdfReader
        from lxml import html
        fx=self.seven_meter_fixture()
        # No classification profile matches: the real endpoint must use test mode.
        fx.session.execute(text("UPDATE bil_property SET name='Functional test fixture' WHERE id=26"))
        fx.session.commit();fx.conn.commit()
        before=fx.snapshot();tariffs=list(fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        url=fx.generation_url+'&test_report=1'
        with fx.app.test_request_context():data=fx.metsoa.build(26,'2026-10',recipient_id=11,test_mode=True)
        self.assertEqual(len(data['water']['meters']),7)
        self.assertGreater(data['electricity']['subtotal'],0)
        self.assertEqual(data['electricity']['rows'][0]['rate'],D('3.2795'))
        self.assertTrue(data['pm_issues'])
        self.assertTrue(all('S05' in issue for issue in data['pm_issues']),data['pm_issues'])
        self.assertFalse(data['pm_complete']);self.assertFalse(data['pm_pricing_complete'])
        self.assertIn('TEST / NOT FOR ISSUANCE',data['pm_status'])
        page=fx.client.get(url,headers=fx.headers);self.assertEqual(page.status_code,200,page.text)
        self.assertIn('TEST / NOT FOR ISSUANCE',page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//input[@name="test_report"]/@value'),['1'])
        self.assertTrue(all('test_report=1' in u for u in tree.xpath('//a[contains(@href,"format=pdf")]/@href')))
        output=Path('output/pdf');output.mkdir(parents=True,exist_ok=True)
        for name in ('partial','priced'):
            if name=='priced':
                # Low-use isolated fixture demonstrates a complete calculation
                # with the same four existing rates; no S05 is required.
                fx.session.execute(text("UPDATE bil_meter_reading SET reading_value=CASE WHEN meter_id=5 THEN 80 WHEN meter_id=1 THEN 20 ELSE reading_value END WHERE reading_date='2026-10-31'"))
                fx.session.commit();fx.conn.commit()
                with fx.app.test_request_context():priced=fx.metsoa.build(26,'2026-10',recipient_id=11,test_mode=True)
                self.assertFalse(priced['pm_issues'],priced['pm_issues'])
                self.assertTrue(priced['pm_pricing_complete'])
                self.assertFalse(priced['pm_complete'])
                self.assertEqual(len(priced['water']['meters']),7)
                self.assertIn('PRICING COMPLETE',priced['pm_status'])
            pdf=fx.client.get(url+'&format=pdf',headers=fx.headers)
            self.assertEqual(pdf.status_code,200)
            self.assertIn('TEST-',pdf.headers['Content-Disposition'])
            reader=PdfReader(BytesIO(pdf.data));content=' '.join(p.extract_text() or '' for p in reader.pages)
            self.assertTrue(all('TEST / NOT FOR ISSUANCE' in (p.extract_text() or '') for p in reader.pages))
            for meter in data['water']['meters']:self.assertIn(meter['meter_number'],content)
            self.assertIn('Electricity Sub-Total',content)
            self.assertIn('S & D Cost',content)
            self.assertNotIn('Amount Due R',content)
            if name=='partial':
                self.assertIn('Rate unavailable',content)
                self.assertIn('INCOMPLETE',content)
                self.assertIn('S05 sewerage consumption',content)
                self.assertEqual(before,fx.snapshot())
            else:
                self.assertNotIn('Rate unavailable',content)
                self.assertNotIn('incomplete',content.lower())
                self.assertIn('Test utilities total',content)
            (output/('metsoa-functional-test-'+name+'.pdf')).write_bytes(pdf.data)
            import fitz
            document=fitz.open(stream=pdf.data,filetype='pdf')
            images=Path('tmp/pdfs/metsoa-functional-test');images.mkdir(parents=True,exist_ok=True)
            for i,p in enumerate(document):p.get_pixmap(matrix=fitz.Matrix(1)).save(str(images/(name+'-'+str(i+1)+'.png')))
        self.assertEqual(tariffs,list(fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))
        self.assertEqual(fx.client.get(url.replace('/26/','/28/'),headers=fx.headers).status_code,403)

    def test_real_report_does_not_require_unused_fifth_rate(self):
        from sqlalchemy import text
        fx=self.fixture()
        fx.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2026-09-25',10)"))
        fx.session.commit();fx.conn.commit()
        with fx.app.test_request_context():data=fx.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertTrue(data['pm_complete'],data['pm_issues'])
        self.assertTrue(all(not w['sd_unavailable'] for w in data['water']['meters']))

if __name__ == '__main__':unittest.main()
