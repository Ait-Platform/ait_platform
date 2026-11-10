"""Annual admin review using real routes and isolated PostgreSQL temp records."""
import ast
from datetime import date
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

from lxml import html
from sqlalchemy import event, text
from test_pm_admin_landing import ROOT, function
from test_pm_tariff_management import ManagementTests


class AnnualRegisterTests(ManagementTests):
    url = '/admin/billing/tariffs/annual'

    def setUp(self):
        super().setUp()
        name = 'app.admin.programs.billing.annual_tariff_register'
        module = types.ModuleType(name)
        module.__file__ = str(ROOT / 'app/admin/programs/billing/annual_tariff_register.py')
        module.identity = self.tariff_service.identity
        module.resolve = self.tariff_service.resolve
        tree = ast.parse(Path(module.__file__).read_text(encoding='utf8'))
        tree.body = [node for node in tree.body if not (
            isinstance(node, ast.ImportFrom) and (node.module or '').startswith('app.'))]
        exec(compile(tree, module.__file__, 'exec'), module.__dict__)
        mocked = patch.dict(sys.modules, {name: module})
        mocked.start();self.addCleanup(mocked.stop)
        # Legacy register tests isolate new monitoring infrastructure; dedicated
        # monitor tests replace this stub with the real service and private storage.
        monitor_name='app.admin.programs.billing.municipal_tariff_monitor'
        monitor=types.ModuleType(monitor_name);monitor.summary=lambda:[]
        monitor_patch=patch.dict(sys.modules,{monitor_name:monitor})
        monitor_patch.start();self.addCleanup(monitor_patch.stop)
        overview_name='app.admin.programs.billing.municipal_tariff_overview'
        overview=types.ModuleType(overview_name)
        overview.build=lambda year:dict(download_status='Not downloaded',extracted=0,passing=0,exceptions=None,updated=0)
        overview_patch=patch.dict(sys.modules,{overview_name:overview})
        overview_patch.start();self.addCleanup(overview_patch.stop)
        self.review = module
        activation_name='app.admin.programs.billing.annual_tariff_activation'
        activation=types.ModuleType(activation_name)
        activation.__dict__.update({key:getattr(module,key) for key in ('identity','resolve','candidates','component','decimal','token')})
        activation_tree=ast.parse((ROOT/'app/admin/programs/billing/annual_tariff_activation.py').read_text(encoding='utf8'))
        activation_tree.body=[node for node in activation_tree.body if not (
            isinstance(node,ast.ImportFrom) and (node.module or '').startswith('app.'))]
        exec(compile(activation_tree,'annual_tariff_activation.py','exec'),activation.__dict__)
        mocked_activation=patch.dict(sys.modules,{activation_name:activation})
        mocked_activation.start();self.addCleanup(mocked_activation.stop)
        self.activation=activation
        env = self.app.view_functions['admin_bp.billing_tariffs'].__globals__
        self.app.view_functions['admin_bp.billing_tariff_annual'] = function(
            'app/admin/programs/billing/routes.py', 'billing_tariff_annual', env)
        self.app.view_functions['admin_bp.billing_tariff_annual_activate']=function(
            'app/admin/programs/billing/routes.py','billing_tariff_annual_activate',env)
        self.conn.execute(text("UPDATE bil_tariff SET code='ELEC01',effective_date='2025-07-01' WHERE id=1"))
        self.conn.commit()

    def seed(self, identifier, code, rate, effective, utility='water', unit='kL'):
        self.conn.execute(text('INSERT INTO bil_tariff(id,utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) VALUES(:id,:u,:c,:c,:r,0,200,0.2,:unit,:d)'),
                          dict(id=identifier,u=utility,c=code,r=rate,unit=unit,d=effective))
        self.conn.commit()

    def get(self, year='2026/27', **values):
        return self.client.get(self.url, query_string=dict(financial_year=year,workspace='register',**values))

    def row(self, response, component):
        return html.fromstring(response.data).xpath('//tr[@data-component="'+component+'"]')[0]

    def test_admin_only_and_initial_hub_has_no_work_area(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('id="annual-tariff-workspace"', response.text)
        self.assertIn('Rates extracted', response.text)
        self.assertNotIn('NOT APPROVED',response.text)
        self.assertIn('Annual Tariff Register', self.client.get('/admin/billing/tariffs').text)
        self.assertIn('financial_year=2025/26', self.get('2025/26').text)
        with self.client.session_transaction() as state: state.clear()
        self.assertEqual(self.client.get(self.url).status_code, 302)
        self.sign_in()
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"));self.conn.commit()
        self.assertEqual(self.get().status_code, 403)

    def test_year_selection_uses_existing_resolver_and_preserves_actual_dates(self):
        self.seed(2,'ELEC01','3.5747','2026-07-01','electrical','kWh')
        old = self.row(self.get('2025/26'), 'ELEC01').text_content()
        new = self.row(self.get('2026/27'), 'ELEC01').text_content()
        self.assertIn('Version ID: 1', old);self.assertIn('R3.2795', old)
        self.assertIn('Version ID: 2', new);self.assertIn('R3.5747', new)
        self.assertEqual(self.get('2023/24').status_code,400)
        for year in self.review.YEARS:
            response=self.get(year)
            self.assertEqual(response.status_code,200)
            self.assertEqual(len(html.fromstring(response.data).xpath('//tr[@data-component]')),16)

    def test_carried_rate_equal_price_versions_and_midyear_coverage(self):
        self.seed(10,'W01','39.39','2025-07-01')
        self.seed(11,'W01','39.39','2026-06-05')
        old = self.row(self.get('2025/26'),'W01').text_content()
        self.assertIn('Version ID: 11',old);self.assertIn('1 July version recorded',old)
        carried = self.row(self.get('2026/27'),'W01').text_content()
        self.assertIn('2026-06-05',carried);self.assertIn('Missing 1 July version',carried)
        self.assertIn('Older version carried forward',carried)
        self.assertIn('+R4.7100',carried)
        self.seed(12,'W02','46.70','2026-06-05')
        midyear=self.row(self.get('2025/26'),'W02').text_content()
        self.assertIn('Within-year version only',midyear)
        self.assertIn('Missing 1 July version',midyear)

    def test_missing_and_same_start_conflicts_do_not_guess_versions(self):
        self.assertIn('Missing',self.row(self.get(),'S05').text_content())
        self.seed(20,'S05','34.78','2025-07-01','sewerage')
        self.seed(21,'S05','34.78','2025-07-01','sewerage')
        row=self.row(self.get(),'S05').text_content()
        self.assertIn('Conflicting',row);self.assertIn('conflicting records effective',row)
        self.assertIn('ID 20',row);self.assertIn('ID 21',row)
        self.assertNotIn('Version ID:',row)

    def test_candidates_holds_and_noncomparable_property_fee(self):
        self.seed(30,'W_MGT','144.54','2026-07-01','water_fixed','fixed')
        self.seed(31,'S_REFUSE','206.93','2026-07-01','sewerage_fixed','fixed')
        page=self.get()
        management=self.row(page,'W_MGT').text_content()
        self.assertIn('R144.5400',management);self.assertIn('R30.5000',management)
        self.assertIn('Not comparable',management);self.assertIn('HOLD',management)
        water=self.row(self.get('2025/26'),'W01').text_content()
        self.assertIn('conflicting historical publications',water)
        self.assertIn('No selected code',water)

    def test_evidence_and_estimates_never_approve_candidates(self):
        self.seed(40,'ELEC01','3.5747','2026-07-01','electricity','kWh')
        source=next(c['Source / PDF page'] for c in self.review.candidates('2026/27') if c['component']=='ELEC01')
        evidence={40:dict(municipality='eThekwini',municipal_category='Residential',source_reference=source,verified_by=7,verified_at=date(2026,7,1))}
        stored=self.tariff_service.records()
        result=self.review.review(stored,evidence,'2026/27')
        self.assertEqual(next(r for r in result['rows'] if r['component']=='ELEC01')['status'],'Verified')
        evidence[40]['source_reference']='AIT_CURRENT_RATE_SUBSTITUTE_V1:'+source
        result=self.review.review(stored,evidence,'2026/27')
        row=next(r for r in result['rows'] if r['component']=='ELEC01')
        self.assertTrue(row['estimated']);self.assertEqual(row['status'],'Pending review')
        evidence[40]['source_reference']=source
        evidence[40]['municipal_category']='Non-residential commercial'
        result=self.review.review(stored,evidence,'2026/27')
        self.assertEqual(next(r for r in result['rows'] if r['component']=='ELEC01')['status'],'Pending review')
        self.assertIn('Unapproved candidate',self.get().text)

    def test_alias_codes_preserved_and_missing_verification_is_read_only(self):
        self.seed(50,'SIS01','1.48','2026-07-01','sewerage_surcharge','R/kL')
        with patch.object(self.tariff_service,'verification_records',return_value=None):
            page=self.get()
        row=self.row(page,'S_SURCHARGE').text_content()
        self.assertIn('SIS01',row);self.assertIn('Version ID: 50',row)
        self.assertIn('+R0.0000',row);self.assertIn('Pending review',row)
        self.assertIn('verification records are unavailable',page.text)

    def test_multiple_electricity_scales_and_invalid_units_are_not_guessed(self):
        self.seed(60,'OTHER','9','2026-07-01','electricity','kWh')
        self.assertIn('Multiple electricity identities',self.row(self.get(),'ELEC01').text_content())
        self.seed(61,'W01','44.10','2026-07-01',unit='unknown')
        self.assertIn('Not comparable',self.row(self.get(),'W01').text_content())

    def test_surcharge_comparison_keeps_existing_unit_and_scale_safeguards(self):
        self.seed(62,'S_SURCHARGE','1.48','2026-07-01','sewerage_surcharge','kL')
        self.seed(63,'W_SURCHARGE','1.48','2026-07-01','water_surcharge','kL')
        self.assertIn('Not comparable',self.row(self.get(),'S_SURCHARGE').text_content())
        self.assertIn('Not comparable',self.row(self.get(),'W_SURCHARGE').text_content())
        self.conn.execute(text('UPDATE bil_tariff SET reduction_factor=1 WHERE id=62'));self.conn.commit()
        self.assertIn('+R0.0000',self.row(self.get(),'S_SURCHARGE').text_content())
        self.assertIn('Not comparable',self.row(self.get(),'W_SURCHARGE').text_content())

    def test_browser_hub_review_and_close_preserve_year(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlsplit
        self.seed(70,'W01','39.39','2025-07-01')
        self.seed(71,'W_MGT','144.54','2025-07-01','water_fixed','fixed')
        before=self.snapshot()
        def route_request(route):
            url=urlsplit(route.request.url)
            response=self.client.get(url.path+('?' + url.query if url.query else ''))
            route.fulfill(status=response.status_code,body=response.data,content_type='text/html; charset=utf-8')
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(viewport=dict(width=1440,height=1000))
                page.route('http://tariff-review.test/**',route_request)
                page.goto('http://tariff-review.test'+self.url)
                self.assertEqual(page.locator('#annual-tariff-workspace').count(),0)
                artifact=ROOT/'output/annual_tariff_register_simplified';artifact.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(artifact/'hub.png'),full_page=True)
                page.select_option('#financial-year','2025/26')
                page.get_by_role('button',name='Review Exceptions').click()
                self.assertTrue(page.locator('#annual-tariff-workspace').is_visible())
                self.assertEqual(page.locator('tr[data-component]').count(),16)
                self.assertIn('HOLD',page.locator('[data-component="W_MGT"]').inner_text())
                self.assertFalse(page.locator('input[name="rate"],button[name="activate"]').count())
                artifact=ROOT/'output/annual_tariff_register_stage1'
                artifact.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(artifact/'annual-register-desktop.png'),full_page=True)
                page.get_by_role('link',name='Close',exact=True).click()
                self.assertEqual(page.locator('#annual-tariff-workspace').count(),0)
                self.assertEqual(page.locator('#financial-year').input_value(),'2025/26')
            finally:browser.close()
        self.assertEqual(before,self.snapshot())

    def test_review_gets_execute_selects_only_and_post_cannot_activate(self):
        before=self.snapshot()
        audit_before=[dict(r) for r in self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings()]
        self.session.commit();self.conn.commit()
        self.conn.execute(text('SET TRANSACTION READ ONLY'))
        queries=[]
        def record(conn,cursor,sql,parameters,context,many):queries.append(sql)
        event.listen(self.conn,'before_cursor_execute',record)
        try:
            for year in self.review.YEARS:self.assertEqual(self.get(year).status_code,200)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertTrue(queries)
        self.assertTrue(all(sql.lstrip().upper().startswith('SELECT') for sql in queries),queries)
        self.conn.rollback()
        self.assertEqual(before,self.snapshot())
        self.assertEqual(audit_before,[dict(r) for r in self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings()])
        csrf=html.fromstring(self.client.get('/admin/billing/tariffs/new').data).xpath('//input[@name="csrf_token"]/@value')[0]
        self.assertEqual(self.client.post(self.url,data=dict(csrf_token=csrf,rate='1',action='activate')).status_code,405)
        self.assertEqual(before,self.snapshot())


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(AnnualRegisterTests(name) for name in AnnualRegisterTests.__dict__ if name.startswith('test_'))
