"""Recovered values stay inactive; explicit Admin saves reuse dated version auditing."""
import unittest,importlib.util,sys
from unittest.mock import patch
from datetime import date
from decimal import Decimal
from sqlalchemy import text,event
from lxml import html
import test_pm_tariff_management as fixture
from test_pm_admin_landing import ROOT,function

spec=importlib.util.spec_from_file_location('app.program_billing.recovered_tariffs',ROOT/'app/program_billing/recovered_tariffs.py')
recovered=importlib.util.module_from_spec(spec);spec.loader.exec_module(recovered)

class RecoveredReviewTests(fixture.ManagementTests):
    def setUp(self):
        super().setUp()
        mock=patch.dict(sys.modules,{'app.program_billing.recovered_tariffs':recovered});mock.start();self.addCleanup(mock.stop)
        env=self.app.view_functions['admin_bp.billing_tariffs'].__globals__
        self.app.view_functions['admin_bp.billing_tariff_recovered']=function('app/admin/programs/billing/routes.py','billing_tariff_recovered',env)
    def test_exact_source_values_and_stated_transition_are_inactive(self):
        data=recovered.catalogue();a,b=data['schedules']
        self.assertEqual((len(a['values']),len(b['values'])),(15,21))
        self.assertEqual(a['values']['Unit25'],'19');self.assertEqual(a['values']['Rate6'],'0')
        self.assertEqual(b['values']['EleRate'],'3.5747');self.assertEqual(b['values']['Unit833'],'0.833')
        self.assertEqual(b['values']['MthMgtFee14'],'24.09')
        self.assertEqual(recovered.selected_schedule('2024-10'),'A');self.assertEqual(recovered.selected_schedule('2024-11'),'B')
        unbounded=recovered.review([dict(id=1,utility_type='electricity',code='ELEC01',effective_date='2024-01-01',rate=1,unit='R/kWh')])
        self.assertFalse(next(r for s in unbounded if s['id']=='B' for r in s['rows'] if r['variable']=='EleRate')['ready'])
        before=self.snapshot();queries=[]
        def record(conn,cursor,sql,params,context,many):queries.append(sql.lower().lstrip())
        event.listen(self.conn,'before_cursor_execute',record)
        try:page=self.client.get('/admin/billing/tariffs/recovered?month=2024-10')
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(page.status_code,200,page.text)
        self.assertIn('INACTIVE RECOVERED SETTINGS',page.text);self.assertIn('Schedule A',page.text)
        self.assertIn('MthMgtFee14',page.text);self.assertIn('basis',page.text)
        self.assertEqual(before,self.snapshot());self.assertFalse(any(q.startswith(('insert ','update ','delete ')) for q in queries))
        listing=self.client.get('/admin/billing/tariffs');self.assertIn('Recovered schedules',listing.text)
        self.assertIn('Water Infrastructure Surcharge',self.client.get('/admin/billing/tariffs/new').text)

    def test_prepared_b_and_explicit_a_save_keep_latest_version_and_audit(self):
        before=self.snapshot()['bil_tariff'][0]
        path='/admin/billing/tariffs/1/edit?recovered_schedule=B&recovered_variable=EleRate'
        page=self.client.get(path);self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//input[@name="rate"]/@value'),['3.5747'])
        self.assertEqual(tree.xpath('//input[@name="effective_date"]/@value'),['2024-11-01'])
        self.assertEqual(self.snapshot()['bil_tariff'],[before])
        response=self.post(path,dict(base_id='1',rate='3.5747',effective_date='2024-11-01'))
        self.assertEqual(response.status_code,302,response.text)
        a_path='/admin/billing/tariffs/1/edit?recovered_schedule=A&recovered_variable=EleRate'
        page=self.client.get(a_path);self.assertEqual(page.status_code,200)
        self.assertEqual(html.fromstring(page.data).xpath('//input[@name="effective_date"]/@value'),[''])
        self.assertEqual(self.post(a_path,dict(base_id='1',rate='2.58',effective_date='2024-07-01')).status_code,302)
        rows=self.snapshot()['bil_tariff'];self.assertIn(before,rows)
        for month,expected in [('2024-10',2.58),('2024-11',3.5747),('2026-10',3.2795)]:
            selected,issues=self.tariff_service.resolve(rows,month)
            self.assertFalse(issues);self.assertEqual(selected[0]['rate'],expected)
        evidence=list(self.conn.execute(text('SELECT source_reference,verified_by FROM bil_tariff_verification ORDER BY tariff_id')))
        self.assertEqual(len(evidence),2)
        self.assertTrue(all('Recovered Almond VB6' in r[0] and r[1]==7 for r in evidence))
        self.assertIn('EleRate=3.5747',evidence[0][0])

    def test_unmapped_fields_foreign_targets_and_transition_changes_cannot_activate(self):
        before=self.snapshot()
        for query in ('recovered_schedule=B&recovered_variable=MthMgtFee14','recovered_schedule=B&recovered_variable=Tier1Pc','recovered_schedule=A&recovered_variable=EleRate'):
            self.assertEqual(self.client.get('/admin/billing/tariffs/1/edit?'+query).status_code,400)
        path='/admin/billing/tariffs/1/edit?recovered_schedule=B&recovered_variable=EleRate'
        self.assertEqual(self.post(path,dict(base_id='1',rate='3.5747',effective_date='2024-10-01')).status_code,400)
        self.assertEqual(before,self.snapshot())
        with patch.object(self.tariff_service,'is_admin',return_value=False):
            self.assertEqual(self.client.get('/admin/billing/tariffs/recovered').status_code,403)
        with self.client.session_transaction() as state:state.clear()
        self.assertEqual(self.client.get('/admin/billing/tariffs/recovered').status_code,302)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(RecoveredReviewTests(n) for n in RecoveredReviewTests.__dict__ if n.startswith('test_'))
