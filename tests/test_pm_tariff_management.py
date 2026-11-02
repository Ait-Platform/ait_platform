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
        self.sign_in()
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
