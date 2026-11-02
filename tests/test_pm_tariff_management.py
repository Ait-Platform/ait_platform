"""Scoped authenticated tariff UI; isolated PostgreSQL temporary tables only."""
import unittest
from datetime import date
from sqlalchemy import text
from lxml import html
from test_pm_admin_landing import function,declared_route
from test_pm_admin_template_paths import AuthenticatedTemplateTests

class ManagementTests(AuthenticatedTemplateTests):
    def setUp(self):
        super().setUp()
        self.tariff_service.today=lambda:date(2026,11,2)
        env=self.app.view_functions['admin_bp.billing_tariffs'].__globals__
        for name in ('billing_tariff_new','billing_tariff_edit','billing_tariff_history'):
            path,endpoint=declared_route('app/admin/programs/billing/routes.py',name,'admin_bp','/admin')
            self.app.view_functions[endpoint]=function('app/admin/programs/billing/routes.py',name,env)
        # A private temp sequence prevents touching the real local sequence.
        self.conn.execute(text('CREATE TEMP SEQUENCE tariff_test_ids START 100'))
        self.conn.execute(text("ALTER TABLE bil_tariff ALTER COLUMN id SET DEFAULT nextval('tariff_test_ids')"));self.conn.commit()
        for rule in self.app.url_map.iter_rules():
            if rule.endpoint=='admin_bp.billing_tariffs':rule.methods.add('POST')
        self.sign_in()
    def post(self, path, data):
        doc=html.fromstring(self.client.get(path).data)
        data=dict(data,csrf_token=doc.xpath('//input[@name="csrf_token"]/@value')[0])
        response=self.client.post(path,data=data)
        self.conn.commit()  # End the fixture's externally owned request transaction.
        return response
    def test_add_validated_tariff_and_duplicate_identity(self):
        before=self.snapshot()['bil_tariff']
        data=dict(utility_type='water',code='W-NEW',description='New tier',rate='25.1234',effective_date='2026-12-01',block_start='0',block_end='200',reduction_factor='999',unit='kL')
        path='/admin/billing/tariffs/new'
        response=self.post(path,data);self.assertEqual(response.status_code,302,response.text)
        rows=self.snapshot()['bil_tariff'];self.assertEqual(rows[:1],before);self.assertEqual(len(rows),2)
        self.assertEqual(rows[-1]['reduction_factor'],999)
        self.assertEqual(self.post(path,dict(data,code='w-new')).status_code,400)
        self.assertEqual(len(self.snapshot()['bil_tariff']),2)
    def test_add_invalid_dates_numbers_tiers_units_and_csrf(self):
        path='/admin/billing/tariffs/new'
        base=dict(utility_type='water',code='W-NEW',rate='25',effective_date='2026-12-01',block_start='0',block_end='200',reduction_factor='1',unit='kL')
        before=self.snapshot()
        for delta in (dict(effective_date='2026-02-30'),dict(rate='NaN'),dict(rate='-1'),dict(rate='1.12345'),dict(block_end='0'),dict(unit='kWh'),dict(reduction_factor='0')):
            response=self.post(path,dict(base,**delta));self.assertEqual(response.status_code,400,response.text)
            self.assertIn('role="alert"',response.text)
        self.assertEqual(self.client.post(path,data=base).status_code,400)
        self.assertEqual(self.snapshot(),before)
    def test_list_separate_forms_history_and_read_only_gets(self):
        before=self.snapshot()
        response=self.client.get('/admin/billing/tariffs');self.assertEqual(response.status_code,200)
        doc=html.fromstring(response.data)
        self.assertEqual(doc.xpath('//th/text()'),['Type','Code','Description','Consumption Tier','Current Rate','Actions'])
        self.assertFalse(doc.xpath('//form'));self.assertEqual(len(doc.xpath('//td/div[@class="actions"]/a')),2)
        for path in ('new','1/edit','1/history'):
            self.assertEqual(self.client.get('/admin/billing/tariffs/'+path).status_code,200)
        self.assertEqual(before,self.snapshot())
    def test_tariff_pages_are_administrator_only(self):
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"));self.conn.commit()
        for suffix in ('','/new','/1/edit','/1/history'):
            self.assertEqual(self.client.get('/admin/billing/tariffs'+suffix).status_code,403)
    def test_history_current_scheduled_and_inclusive_end(self):
        self.session.execute(text("INSERT INTO bil_tariff(id,utility_type,code,description,rate,effective_date) VALUES(2,'electricity','TEST-E','Future',4,'2026-12-01')"));self.session.commit();self.conn.commit()
        response=self.client.get('/admin/billing/tariffs/1/history')
        self.assertIn('2026-11-30',response.text);self.assertIn('Scheduled',response.text);self.assertIn('Current',response.text)
        listing=self.client.get('/admin/billing/tariffs');self.assertIn('R3.2795',listing.text);self.assertNotIn('R4.0000',listing.text)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ManagementTests(name) for name in ManagementTests.__dict__ if name.startswith('test_'))
