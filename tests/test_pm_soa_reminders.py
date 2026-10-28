"""Due rent reminders exercise the same authenticated POST service on PostgreSQL."""
import unittest
from unittest.mock import patch
from lxml import html
from sqlalchemy import text
from flask import render_template_string
import test_pm_soa_rent_plan as fixture


class RentReminderTests(unittest.TestCase):
    setUp=fixture.RentPlanTests.setUp
    path=fixture.RentPlanTests.path
    fixed_form=fixture.RentPlanTests.fixed_form
    save=fixture.RentPlanTests.save
    plan_form=fixture.RentPlanTests.plan_form
    act=fixture.RentPlanTests.act
    def workspace(self):
        if 'workspace_test' not in self.app.view_functions:
            from app.program_billing.lease_soa_reminders import reminders
            self.app._got_first_request=False
            def workspace_test():
                return render_template_string('<div id="workspace-tile">{% include "program_billing/pm_soa_rent_reminders.html" %}</div>',pm_rent_reminders=reminders())
            self.app.add_url_rule('/workspace-test',view_func=workspace_test)
        return self.client.get('/workspace-test',headers=self.headers)
    def test_one_reminder_per_tenant_later_is_session_only(self):
        page=self.workspace();tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//*[@id="workspace-tile"]//*[@data-rent-reminder]')),1)
        self.assertIn('January 2026 rent is due.',page.text)
        for _ in range(2):self.assertEqual(self.workspace().text.count('data-rent-reminder'),1)
        form=tree.xpath('//form[button[text()="Later"]]')[0]
        data={e.get('name'):e.get('value') for e in form.xpath('./input')}
        self.assertEqual(self.client.post(form.get('action'),data=data,headers=self.headers).status_code,302)
        self.assertNotIn('data-rent-reminder',self.workspace().text)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_recurring WHERE pm_rent_schedule')).scalar(),0)
        with self.client.session_transaction() as visit:visit['pm_rent_visit']={'ids':[],'until':0}
        self.assertIn('data-rent-reminder',self.workspace().text)
    def test_post_now_posts_one_month_only_and_retry_is_idempotent(self):
        page=self.workspace();tree=html.fromstring(page.data)
        form=tree.xpath('//form[button[text()="Post Now"]]')[0]
        data={e.get('name'):e.get('value') for e in form.xpath('./input')}
        response=self.client.post(form.get('action'),data=data,headers=self.headers)
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(self.session.execute(text('SELECT month,ref FROM bil_tenant_ledger')).all(),[('2026-01','D0001')])
        # An old reminder may be stale after initial assignment; it must not duplicate money.
        self.assertIn(self.client.post(form.get('action'),data=data,headers=self.headers).status_code,(302,409))
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)
        self.assertIn('February 2026 rent is due.',self.workspace().text)
    def test_reminder_post_requires_csrf_and_scope(self):
        page=self.workspace();form=html.fromstring(page.data).xpath('//form[button[text()="Later"]]')[0]
        data={e.get('name'):e.get('value') for e in form.xpath('./input')}
        self.assertEqual(self.client.post(form.get('action'),data=dict(data,csrf_token='bad'),headers=self.headers).status_code,400)
        self.assertEqual(self.client.post(form.get('action'),data=dict(data,posting_key='bad'),headers=self.headers).status_code,409)
        self.assertEqual(self.client.post(form.get('action'),data=data).status_code,403)
        self.user.id=8
        self.assertNotIn('data-rent-reminder',self.workspace().text)

    def test_actual_workspace_context_processor_supplies_in_tile_reminders(self):
        import types
        from flask import request,current_app
        import test_pm_lease_lessee_persistence as loader
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(context_processor=lambda f:f),request=request,db=self.db,current_app=current_app)
        loader.functions('app/program_billing/routes.py',{'pm_soa_workspace_reminders'},env)
        self.app._got_first_request=False
        self.app.context_processor(env['pm_soa_workspace_reminders'])
        def real_workspace():
            return render_template_string('<div id="workspace-tile">{% include "program_billing/pm_soa_rent_reminders.html" %}</div>')
        self.app.view_functions['billing_bp.learner_dashboard']=real_workspace
        response=self.client.get('/soa-test/learner_dashboard',headers=self.headers)
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(len(html.fromstring(response.data).xpath('//*[@id="workspace-tile"]//*[@data-rent-reminder]')),1)
        # Unrelated PM pages do not receive repeated workspace reminders.
        self.assertNotIn('data-rent-reminder',self.client.get(self.path()+'/fixed',headers=self.headers).text)
