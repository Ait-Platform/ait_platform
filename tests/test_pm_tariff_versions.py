"""Read-only version resolution against isolated session-temporary PostgreSQL tariffs."""
import ast,unittest,types,sys
from datetime import date
from decimal import Decimal,ROUND_HALF_UP
from sqlalchemy import text
from unittest.mock import patch
import test_pm_admin_template_paths as fixture
from test_pm_admin_landing import function
class TariffVersionTests(fixture.AuthenticatedTemplateTests):
    def setUp(self):
        super().setUp()
        app_module=types.ModuleType('app');app_module.__path__=[str(fixture.ROOT/'app')]
        utils_module=types.ModuleType('app.utils');utils_module.__path__=[str(fixture.ROOT/'app/utils')]
        packages=patch.dict(sys.modules,{'app':app_module,'app.utils':utils_module});packages.start();self.addCleanup(packages.stop)
        from app.utils import tariff_versions
        self.versions=tariff_versions
        self.session.execute(text('DELETE FROM bil_tariff'));self.session.commit();self.conn.commit()
        self.env=dict(db=types.SimpleNamespace(session=self.session),text=text,Decimal=Decimal,ROUND_HALF_UP=ROUND_HALF_UP,Q2=Decimal('.01'))
        tree=ast.parse((fixture.ROOT/'app/admin/programs/billing/water.py').read_text(encoding='utf-8'))
        for n in tree.body:
            if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('WATER_TIER_CODES','SAN_TIER_CODES') for t in n.targets):exec(compile(ast.Module(body=[n],type_ignores=[]),'water-constants','exec'),self.env)
        for name in ('_D','_q2','_is_per_kl','_side_from_utility','_tariff_latest_by_code','get_water_tiers','get_fixed_tariffs','calc_ws_sd_totals'):
            self.env[name]=function('app/admin/programs/billing/water.py',name,self.env)
    def add(self,id,code='ELEC01',utility='electricity',rate='3.2795',effective='2026-01-01',start=0,end=0,factor=1,unit='kWh'):
        self.session.execute(text('INSERT INTO bil_tariff(id,utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) VALUES(:id,:u,:c,:c,:r,:s,:e,:f,:unit,:d)'),dict(id=id,u=utility,c=code,r=rate,s=start,e=end,f=factor,unit=unit,d=effective));self.session.commit();self.conn.commit()
    def snapshot(self):return [dict(r) for r in self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]
    def test_single_historical_current_future_and_unchanged_rows(self):
        self.add(1,rate='2');self.add(2,rate='3',effective='2026-11-01');self.add(3,rate='9',effective='2027-01-01');before=self.snapshot()
        for billing,expected in [('2026-10',2),('2026-11',3),('2027-01',9),(date(2026,10,20),2)]:
            self.assertEqual(self.versions.one(self.session,billing,'ELEC01')['rate'],expected)
        self.assertEqual(self.snapshot(),before)
    def test_duplicate_effective_starts_rejected_without_id_tiebreak(self):
        self.add(1);self.add(2,rate='9')
        with self.assertRaises(self.versions.TariffSelectionError):self.versions.one(self.session,'2026-10','ELEC01')
    def test_missing_invalid_and_non_iso_effective_dates_rejected(self):
        for value in ('','unknown','2026-02-30','2026-1-1'):
            self.session.execute(text('DELETE FROM bil_tariff'));self.session.commit();self.conn.commit();self.add(1,effective=value)
            with self.assertRaises(self.versions.TariffSelectionError):self.versions.one(self.session,'2026-10','ELEC01')
    def test_future_only_date_and_identity_ambiguity(self):
        self.add(1,effective='2027-01-01')
        with self.assertRaises(self.versions.TariffSelectionError):self.versions.one(self.session,'2026-10','ELEC01')
        self.add(2,utility='water',rate='1',effective='2026-01-01')
        with self.assertRaises(self.versions.TariffSelectionError):self.versions.one(self.session,'2027-02','ELEC01')
    def test_unrelated_future_identity_does_not_change_current_tier_list(self):
        self.add(1,code='W01',utility='water',start=0,end=200)
        self.add(2,code='W02',utility='water',start=201,end=833,effective='2027-01-01')
        self.assertEqual([r['code'] for r in self.versions.rows(self.session,'2026-10',utility='water')],['W01'])
    def test_water_and_sewerage_tiers_one_version_each_preserve_factors(self):
        self.add(1,'Tier1_W&S','water','2',start=0,end=200,unit='kL')
        self.add(2,'Tier1_W&S','water','3',effective='2026-11-01',start=0,end=200,unit='kL')
        self.add(3,'Tier1_SD','sanitation','4',start=0,end=200,factor='.5',unit='kL')
        self.add(4,'Tier1_SD','sanitation','5',effective='2026-11-01',start=0,end=200,factor='.5',unit='kL')
        for month,rates in [('2026-10',(2,4)),('2026-11',(3,5))]:
            ws,sd=self.env['get_water_tiers'](month);self.assertEqual((len(ws),len(sd)),(1,1))
            self.assertEqual((ws[0]['rate'],sd[0]['rate']),rates);self.assertEqual(sd[0]['red'],.5)
            self.assertEqual((ws[0]['block_start'],ws[0]['block_end']),(0,200))
    def test_original_water_sewerage_formula_results_with_selected_versions(self):
        self.add(1,'Tier1_W&S','water','2',start=0,end=200,unit='kL')
        self.add(2,'Tier1_W&S','water','3',effective='2026-11-01',start=0,end=200,unit='kL')
        self.add(3,'Tier1_SD','sanitation','4',start=0,end=200,factor='.5',unit='kL')
        result=self.env['calc_ws_sd_totals'](meter_id=1,month_str='2026-10',consumption_kl=.1,days=31,include_fixed=False)
        self.assertEqual(result['ws_total'],Decimal('.20'));self.assertEqual(result['sd_total'],Decimal('.20'));self.assertEqual(result['water_cost'],Decimal('.40'))
    def test_fixed_charge_versions_and_999_metadata_unchanged(self):
        self.add(1,'W_LOSS','management','19.30',unit='fixed');self.add(2,'W_LOSS','management','25',effective='2027-01-01',unit='fixed')
        self.add(3,'W05','water','105.39',start=1501,end=99999,factor=999,unit='kL')
        before=self.snapshot();self.assertEqual(self.env['get_fixed_tariffs']('2026-10')['W_LOSS'],19.30)
        row=self.versions.one(self.session,'2026-10','W05');self.assertEqual(row['reduction_factor'],999);self.assertEqual((row['block_start'],row['block_end']),(1501,99999));self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(TariffVersionTests(name) for name in TariffVersionTests.__dict__ if name.startswith('test_'))
