"""Active billing adapters select the same versions without changing formula inputs."""
import unittest,types
from datetime import date
from decimal import Decimal,InvalidOperation
from sqlalchemy import text,inspect,event
import test_pm_tariff_versions as fixture
from test_pm_admin_landing import function
class ConsumerTests(fixture.TariffVersionTests):
    def setUp(self):
        super().setUp()
        common=dict(db=types.SimpleNamespace(session=self.session),text=text,date=date,Decimal=Decimal,InvalidOperation=InvalidOperation,SimpleNamespace=types.SimpleNamespace,inspect=inspect,RECURRING_SURCHARGE=Decimal('1.48'))
        self.adapters={}
        groups={
          'app/utils/billing_map.py':('_tariff_for_code_month','compute_electricity_due'),
          'app/utils/billing_metsoa.py':('_get_tariffs','_get_tariff_by_code','get_electricity_rate_for_month'),
          'app/utils/billing_metsoa_builder.py':('_tariffs_by_code','get_tariffs_for_ws','get_tariffs_for_sd','_month_first','get_meter_map_for_month','_split_by_blocks','calc_ws_sd_totals'),
          'app/program_billing/routes.py':('_effective_tariff_rows','_effective_electric_tariff','get_tariff_for_ws','get_tariff_for_sd'),
          'app/program_billing/metsoa_tariffs.py':('select',)}
        for file,names in groups.items():
            scope=dict(common)
            for name in names:scope[name]=function(file,name,scope)
            self.adapters[file]=scope
    def test_electricity_eleC01_current_historical_future_all_active_adapters(self):
        self.add(1,rate='2');self.add(2,rate='3',effective='2026-11-01');self.add(3,rate='9',effective='2027-01-01');before=self.snapshot()
        m=self.adapters['app/utils/billing_map.py'];legacy=self.adapters['app/utils/billing_metsoa.py'];builder=self.adapters['app/utils/billing_metsoa_builder.py'];program=self.adapters['app/program_billing/routes.py']
        for month,rate in [('2026-10',2),('2026-11',3),('2027-01',9)]:
            self.assertEqual(m['compute_electricity_due'](10,month),dict(rate=rate,due=10*rate))
            self.assertEqual(legacy['get_electricity_rate_for_month'](month),rate)
            self.assertEqual(builder['_tariffs_by_code'](month)['ELEC01']['rate'],rate)
            self.assertEqual(program['_effective_electric_tariff'](month).rate,rate)
        self.assertEqual(before,self.snapshot())
    def test_tiers_preserve_boundaries_and_factors_without_duplicate_versions(self):
        self.add(1,'Tier1_W&S','water','2',start=0,end=200,factor='.2',unit='kL')
        self.add(2,'Tier1_W&S','water','3',effective='2026-11-01',start=0,end=200,factor='.2',unit='kL')
        self.add(3,'Tier1_SD','sanitation','4',start=0,end=200,factor=999,unit='kL')
        self.add(4,'Tier1_SD','sanitation','5',effective='2026-11-01',start=0,end=200,factor=999,unit='kL')
        b=self.adapters['app/utils/billing_metsoa_builder.py'];legacy=self.adapters['app/utils/billing_metsoa.py'];program=self.adapters['app/program_billing/routes.py']
        for month,water,sewer in [('2026-10',2,4),('2026-11',3,5)]:
            ws=b['get_tariffs_for_ws'](month);sd=b['get_tariffs_for_sd'](month)
            self.assertEqual((len(ws),len(sd)),(1,1));self.assertEqual((ws[0]['rate'],sd[0]['rate']),(water,sewer))
            self.assertEqual(sd[0]['reduction_factor'],999);self.assertNotIn('reduction_factor',ws[0])
            self.assertEqual((ws[0]['block_start'],ws[0]['block_end']),(0,200))
            self.assertEqual(len(legacy['_get_tariffs']('water',month)),1)
            self.assertEqual(program['get_tariff_for_ws'](1,month)[0]['rate'],water)
    def test_builder_fixed_charge_uses_billing_version_and_no_financial_writes(self):
        self.add(1,'WaterLossLevy','water_fixed','19.30',unit='fixed');self.add(2,'WaterLossLevy','water_fixed','25',effective='2027-01-01',unit='fixed')
        self.session.execute(text('CREATE TEMP TABLE bil_meter_charge_map(meter_id integer,charge_code text,utility_type text,effective_start text,effective_end text,is_enabled integer,tariff_code_override text)'))
        self.session.execute(text("INSERT INTO bil_meter_charge_map VALUES(1,'WaterLossLevy','water_fixed','2026-01-01',NULL,1,NULL)"))
        self.session.execute(text('CREATE TEMP TABLE issued_statement_sentinel(id integer PRIMARY KEY,total numeric(18,2)); INSERT INTO issued_statement_sentinel VALUES(1,123.45)'));self.session.commit();self.conn.commit()
        statements=[]
        def record(conn,cursor,sql,parameters,context,many):statements.append(sql)
        event.listen(self.conn,'before_cursor_execute',record)
        try:
            for month,expected in [('2026-10',19.30),('2027-01',25)]:
                result=self.adapters['app/utils/billing_metsoa_builder.py']['calc_ws_sd_totals'](1,month,10,31)
                self.assertEqual(result['ws_amount'],expected)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertTrue(all(sql.lstrip().upper().startswith('SELECT') for sql in statements))
        self.assertEqual(self.session.execute(text('SELECT total FROM issued_statement_sentinel')).scalar(),Decimal('123.45'))
    def test_property_selector_reuses_same_versions_and_rejects_conflicts(self):
        self.add(1,rate='2');self.add(2,rate='3',effective='2026-11-01')
        select=self.adapters['app/program_billing/metsoa_tariffs.py']['select']
        for day,expected in [(date(2026,10,31),Decimal('2')),(date(2026,11,30),Decimal('3'))]:
            result,issues=select(day);self.assertEqual(result['electricity'],expected)
        self.add(3,rate='9',effective='2026-11-01')
        result,issues=select(date(2026,11,30));self.assertIsNone(result['electricity']);self.assertIn('conflicting records',' '.join(issues))
        with self.assertRaises(self.versions.TariffSelectionError):self.adapters['app/utils/billing_metsoa_builder.py']['_tariffs_by_code']('2026-11')

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ConsumerTests(name) for name in ConsumerTests.__dict__ if name.startswith('test_'))
