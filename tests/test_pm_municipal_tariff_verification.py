"""Official text fixtures + isolated PostgreSQL; no live source dependency."""
import ast
from copy import deepcopy
from hashlib import sha256
import json
import sys
import types
import unittest
from unittest.mock import patch
from sqlalchemy import text
from test_pm_admin_landing import ROOT,function
from test_pm_municipal_tariff_sources import PublicationTests


class VerificationTests(PublicationTests):
    def setUp(self):
        super().setUp()
        self.fixture=json.loads((ROOT/'tests/fixtures/pm_tariff_sources/official_tables_2026.json').read_text(encoding='utf8'))
        self.verification=self.load('municipal_tariff_verification',dict(component=self.review.component,comparable_unit=self.review.comparable_unit,token=self.review.token,resolve=self.review.resolve,identity=self.review.identity,official_url=self.source.official_url,year_start=self.source.year_start))
        self.workflow=self.load('municipal_tariff_workflow',{})
        env=self.app.view_functions['admin_bp.billing_tariffs'].__globals__
        self.app.view_functions['admin_bp.billing_tariff_publication_verification']=function('app/admin/programs/billing/routes.py','billing_tariff_publication_verification',env)

    def load(self,short,env):
        name='app.admin.programs.billing.'+short;module=types.ModuleType(name);module.__dict__.update(env)
        tree=ast.parse((ROOT/('app/admin/programs/billing/'+short+'.py')).read_text(encoding='utf8'))
        tree.body=[node for node in tree.body if not (isinstance(node,ast.ImportFrom) and (node.level or (node.module or '').startswith('app.')))]
        exec(compile(tree,short+'.py','exec'),module.__dict__)
        change=patch.dict(sys.modules,{name:module});change.start();self.addCleanup(change.stop)
        return module

    def extraction(self,adoption=False):
        pages=list(self.fixture['pages'].values())
        if adoption:pages.insert(0,'These tariffs for 2026/2027 were adopted by the eThekwini Municipal Council on 2026-05-29; effective from 1 July 2026 to 30 June 2027. SYNTHETIC TEST ADOPTION ONLY')
        fake=types.SimpleNamespace(is_encrypted=False,pages=[types.SimpleNamespace(extract_text=lambda text=value:text) for value in pages])
        metadata=dict(financial_year='2026/27',source_url=self.fixture['source'],sha256=sha256(self.pdf).hexdigest(),publication_status='FINAL_LABELLED')
        with patch.object(self.verification,'PdfReader',return_value=fake):return self.verification.extract(self.pdf,metadata)

    def test_actual_official_rows_are_extracted_without_assuming_adoption(self):
        result=self.extraction()
        self.assertIsNone(result['adoption'])
        expected=dict(zip(('W01','W02','W03','W04','W05','S01','S02','S03','S04','S05'),('44.10','52.30','69.60','107.40','118.00','6.53','11.00','21.00','32.80','37.60')))
        for code,rate in expected.items():self.assertEqual(result['extracted'][code][0]['rate'],rate,code)
        for code in ('W_SURCHARGE','S_SURCHARGE'):self.assertEqual(result['extracted'][code][0]['rate'],'1.48')
        self.assertEqual(result['extracted']['W_LOSS'][0]['rate'],'21.60')
        self.assertFalse(result['extracted']['W_MGT'][0]['supported'])
        self.assertTrue(all(row['status']=='HOLD' for row in self.verification.compare(result,[],{})['rows']))

    def test_adoption_requires_specific_binding_year_dates_and_no_draft(self):
        self.assertTrue(self.extraction(True)['adoption'])
        for value in ('Council approved the 2026/2027 budget.','These tariffs for 2025/2026 were adopted by the eThekwini Council on 2025-05-01','DRAFT These tariffs for 2026/2027 were adopted by eThekwini Council on 2026-05-29 effective from 1 July 2026 to 30 June 2027'):
            self.assertIsNone(self.verification.adopted(value,'2026/27'))

    def test_comparison_exact_changes_new_unchanged_and_exceptions(self):
        result=self.extraction(True)
        self.seed(80,'W01','44.1000','2025-07-01','water','kL')
        self.seed(81,'S01','6.5301','2025-07-01','sewerage','kL')
        self.conn.execute(text('UPDATE bil_tariff SET block_start=0 WHERE id IN (80,81)'));self.conn.commit()
        report=self.verification.compare(result,self.tariff_service.records(),{})
        rows={row['code']:row for row in report['rows']}
        self.assertEqual(rows['W01']['classification'],'UNCHANGED')
        self.assertEqual(rows['S01']['classification'],'UPDATED')
        self.assertEqual(rows['W02']['classification'],'NEW')
        self.assertEqual(rows['W_MGT']['classification'],'EXCEPTION')
        self.assertEqual(rows['ELEC01']['classification'],'EXCEPTION')

    def test_duplicate_missing_units_and_unsupported_mapping_stay_on_hold(self):
        result=self.extraction(True);result['extracted']['W01']*=2
        self.seed(82,'W02','1','2025-07-01','water','kWh')
        rows={r['code']:r for r in self.verification.compare(result,self.tariff_service.records(),{})['rows']}
        self.assertEqual(rows['W01']['status'],'HOLD');self.assertEqual(rows['W02']['status'],'HOLD')
        result=self.extraction(True);result['extracted']['W02'][0]['supported']=False
        self.assertEqual(next(r for r in self.verification.compare(result,[],{})['rows'] if r['code']=='W02')['classification'],'EXCEPTION')

    def test_report_route_is_admin_only_and_read_only(self):
        self.run_download();before=self.snapshot()
        with self.app.app_context():record=self.documents.listing(self.documents.backend(),'2026/27')[0]
        path=self.publication_url+'/2026-27/'+record['receipt_id']+'/verification'
        response=self.client.get(path)
        self.assertEqual(response.status_code,200,response.text)
        self.assertIn('Verification Status',response.text);self.assertIn('HOLD',response.text)
        self.assertEqual(before,self.snapshot())
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"));self.conn.commit()
        self.assertEqual(self.client.get(path).status_code,403)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(VerificationTests(name) for name in VerificationTests.__dict__ if name.startswith('test_'))
