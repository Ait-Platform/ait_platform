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
        self.assertIn('S01-S05',sd.readiness(self.tiers[:-1],self.verified)[0])
        self.assertEqual(sd.calculate(175,32,self.tiers[:-1],None,self.verified)[0],[])
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
        self.assertEqual(rows[-1]['cons'],0)

class SewerageStatementGuardTests(unittest.TestCase):
    def test_unverified_category_never_final_on_real_statement(self):
        import test_pm_municipal_metsoa as fixture
        from sqlalchemy import text
        fx=fixture.MunicipalMetsoaTests(methodName='test_permissions_and_token_free_pm_access_remain_unchanged')
        fx.setUp()
        self.addCleanup(fx.doCleanups)
        self.addCleanup(fx.tearDown)
        # Only temporary PostgreSQL fixture tables are modified.
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

if __name__ == '__main__':unittest.main()
