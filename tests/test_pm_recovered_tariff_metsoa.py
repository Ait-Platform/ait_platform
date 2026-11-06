"""Same live Python calculator with explicitly saved fixture-only recovered versions."""
import unittest,ast,types,sys
from decimal import Decimal
from datetime import date
from unittest.mock import patch
from sqlalchemy import text
from pathlib import Path
import test_pm_municipal_metsoa as fixture
from test_pm_recovered_tariff_review import recovered,ROOT

class RecoveredCalculationTests(fixture.MunicipalMetsoaTests):
    def test_prepared_rates_use_existing_tiers_and_metsoa_without_changing_latest(self):
        self.session.execute(text('CREATE TEMP TABLE bil_tariff_verification(tariff_id integer PRIMARY KEY,municipality text,municipal_category text,source_reference text,effective_date text,verified_by integer,verified_at timestamp DEFAULT now(),recorded_at timestamp DEFAULT now())'))
        self.session.commit();self.conn.commit()
        scope=types.ModuleType('recovered_tariff_save_fixture')
        source=ast.parse((ROOT/'app/admin/programs/billing/tariff_management.py').read_text(encoding='utf-8'))
        source.body=[n for n in source.body if not (isinstance(n,ast.ImportFrom) and (n.module or '').startswith('app.'))]
        from app.utils.tariff_versions import identity,resolve
        scope.__dict__.update(db=self.db,is_admin=lambda:True,identity=identity,resolve=resolve)
        exec(compile(source,'tariff_management.py','exec'),scope.__dict__)
        scope.current_user=types.SimpleNamespace(id=7,is_authenticated=True)
        old=[dict(r) for r in self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]
        b=next(s for s in recovered.review(old) if s['id']=='B')
        self.assertFalse(next(r for r in b['rows'] if r['variable']=='MthMgtFee14')['ready'])
        self.assertFalse(next(r for r in b['rows'] if r['variable']=='Tier1Pc')['ready'])
        self.assertTrue(next(r for r in b['rows'] if r['variable']=='SdRate1500')['ready'])
        for row in b['rows']:
            if not row['ready']:continue
            values,_,_=recovered.prepared(old,{},'B',row['variable'],row['target']['id'])
            with self.app.test_request_context():scope.save_rate(row['target']['id'],values)
        now=[dict(r) for r in self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]
        a=next(r for s in recovered.review(now) if s['id']=='A' for r in s['rows'] if r['variable']=='EleRate')
        values,_,_=recovered.prepared(now,{},'A','EleRate',a['target']['id']);values['effective_date']='2024-07-01'
        with self.app.test_request_context():scope.save_rate(a['target']['id'],values)
        self.session.commit();self.conn.commit()
        new=[dict(r) for r in self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')).mappings()]
        for row in old:self.assertIn(row,new)
        for row in new:
            if row in old:continue
            previous=next(r for r in old if (r['utility_type'],r['code'])==(row['utility_type'],row['code']))
            for key in ('utility_type','code','unit','block_start','block_end','reduction_factor'):self.assertEqual(row[key],previous[key])
        self.assertFalse(any(r['code']=='W05' and r['effective_date']=='2024-11-01' for r in new))
        self.assertFalse(any(r['code']=='W_MGT' and r['effective_date']=='2024-11-01' for r in new))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_value=29925 WHERE meter_id=4 AND reading_date='2026-08-26'"))
        self.session.execute(text("UPDATE bil_meter_reading SET reading_date=reading_date-interval '20 months'"))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(5,'2025-01-25',0),(4,'2024-09-26',100),(4,'2024-10-26',200)"))
        self.session.commit();self.conn.commit()
        with self.app.test_request_context():
            data=self.metsoa.build(26,'2025-02',recipient_id=11)
            earlier=self.metsoa.build(26,'2024-10',recipient_id=11)
        electricity=next(r for r in data['electricity']['rows'] if r['meter_number']=='2188802S')
        self.assertEqual(electricity['rate'],Decimal('3.5747'))
        self.assertEqual(next(r for r in earlier['electricity']['rows'] if r['meter_number']=='2188802S')['due'],Decimal('258.00'))
        water=next(r for r in data['water']['meters'] if r['meter_number']=='ANG489')
        self.assertEqual(water['summary']['consumption'],Decimal(20))
        self.assertEqual(water['ws_cost'],Decimal('950.84'))
        self.assertEqual(water['sd_cost'],Decimal('397.28'))
        self.assertEqual(next(r['due'] for r in water['ws_breakdown'] if r['desc']=='Water surcharge'),Decimal('29.60'))
        self.assertEqual(next(r['due'] for r in water['ws_breakdown'] if r['desc']=='Water Loss Levy (insurance)'),Decimal('17.12'))
        self.assertFalse(data['pm_complete']) # Missing fifth water tier / management fee are not fabricated.
        page=self.client.get(self.generation_url.replace('2026-10','2025-02'),headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        self.assertIn('INCOMPLETE',page.text);self.assertIn('17.12',page.text)
        from io import BytesIO
        from pypdf import PdfReader
        pdf=self.client.get(self.generation_url.replace('2026-10','2025-02')+'&format=pdf',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        content=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn('INCOMPLETE',content);self.assertIn('17.12',content)
        from app.program_billing import metsoa_tariffs
        with self.app.test_request_context():latest,_=metsoa_tariffs.select(date(2026,10,31))
        self.assertEqual(latest['electricity'],Decimal('3.2795'))

        self.assertEqual(self.client.get('/billing/utilities/28/generate-metsoa?billing_month=2024-11&recipient_id=11&generate=1',headers=self.headers).status_code,403)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(RecoveredCalculationTests(n) for n in RecoveredCalculationTests.__dict__ if n.startswith('test_'))
