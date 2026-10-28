"""Lease Kit rent assignment and posting on isolated PostgreSQL."""
import json
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from lxml import html
from sqlalchemy import text
import test_pm_soa_utilities as fixture


class RentPlanTests(unittest.TestCase):
    path=fixture.PMUtilityDefinitionTests.path
    fixed_form=fixture.PMUtilityDefinitionTests.fixed_form
    save=fixture.PMUtilityDefinitionTests.save
    def setUp(self):
        fixture.PMUtilityDefinitionTests.setUp(self)
        from app.program_billing import lease_soa_rent_plan
        self.plan_module=lease_soa_rent_plan
        clock=patch.object(lease_soa_rent_plan,'today',return_value=date(2026,10,28));clock.start();self.addCleanup(clock.stop)
        row=self.session.execute(text('SELECT items FROM bil_lease_draft LIMIT 1')).scalar()
        row['_amounts']={'monthly_rent':'35000.00'}
        row['_period']={'commencement_date':'2026-01-31','duration_months':12}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),dict(items=json.dumps(row)));self.session.commit()
    def plan_form(self,action='assign',due=None):
        page=self.client.get(self.path()+'/fixed/rent',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        key=tree.xpath('//form[input[@name="rent_action"][@value="assign"]]/input[@name="posting_key"]/@value')[0]
        return dict(csrf_token=self.csrf,posting_key=key,rent_action=action,due=due or '')
    def act(self,data,prop=26,tenant=11):
        return self.client.post(self.path(prop,tenant)+'/fixed/rent',data=data,headers=self.headers)
    def test_wip_full_term_total_assignment_and_posted_reference(self):
        page=self.client.get(self.path()+'/fixed/rent',headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//*[@id="monthly-rent-plan"]/tbody/tr')),12)
        self.assertIn('420,000.00',page.text);self.assertIn('2026-02-28',page.text)
        data=self.plan_form();self.assertEqual(self.act(data).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
        schedule=self.session.execute(text('SELECT * FROM bil_tenant_recurring WHERE pm_rent_schedule')).mappings().one()
        self.assertEqual(schedule['amount'],Decimal('35000'));self.assertEqual(schedule['day_of_month'],31)
        data=self.plan_form('post','2026-02-28')
        self.assertEqual(self.act(data).status_code,302)
        self.assertEqual(self.act(data).status_code,302)
        row=self.session.execute(text('SELECT ref,amount FROM bil_tenant_ledger')).one()
        self.assertEqual(tuple(row),('D0001',Decimal('35000')))
        self.assertIn('Posted — D0001',self.client.get(self.path()+'/fixed/rent',headers=self.headers).text)
        with self.app.app_context():self.assertEqual(self.fixed.run_due(),dict(posted=8,failed=0))
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),9)
    def test_future_unassigned_tampered_csrf_and_scope_rejected(self):
        self.assertEqual(self.act(self.plan_form()).status_code,302)
        self.assertEqual(self.act(self.plan_form('post','2026-12-31')).status_code,400)
        self.assertEqual(self.act(self.plan_form('post','2026-02-27')).status_code,400)
        self.assertEqual(self.act(dict(self.plan_form(),posting_key='bad')).status_code,409)
        self.assertEqual(self.act(dict(self.plan_form(),csrf_token='bad')).status_code,400)
        self.assertEqual(self.act(self.plan_form(),28,33).status_code,404)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),0)
    def test_changed_draft_is_not_assigned_from_stale_page(self):
        data=self.plan_form()
        self.session.execute(text('UPDATE bil_lease_draft SET version=version+1'));self.session.commit()
        self.assertEqual(self.act(data).status_code,409)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_recurring WHERE pm_rent_schedule')).scalar(),0)
    def test_missing_particulars_clear_message_no_schedule_or_ledger_write(self):
        self.session.execute(text("UPDATE bil_lease_draft SET items=items-'_amounts'"));self.session.commit()
        response=self.client.get(self.path()+'/fixed/rent',headers=self.headers)
        self.assertEqual(response.status_code,200)
        self.assertIn('Save a valid monthly rent',response.text)
        self.assertNotIn('id="monthly-rent-plan"',response.text)

    def test_legacy_period_fields_and_tenant_isolation(self):
        row=self.session.execute(text('SELECT items FROM bil_lease_draft LIMIT 1')).scalar()
        row.pop('_period');row['s1-17']={'value':'1 year'};row['s1-18']={'value':'2026-02-15'}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),dict(items=json.dumps(row)));self.session.commit()
        page=self.client.get(self.path()+'/fixed/rent',headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(len(tree.xpath('//*[@id="monthly-rent-plan"]/tbody/tr')),12)
        self.assertIn('2026-02-15',page.text)
        unrelated=self.client.get(self.path(26,44)+'/fixed/rent',headers=self.headers)
        self.assertEqual(unrelated.status_code,200)
        self.assertIn('No saved Lease Kit draft selects this registered tenant.',unrelated.text)
        self.assertNotIn('id="monthly-rent-plan"',unrelated.text)

    def test_assignment_preserves_existing_financial_history(self):
        self.session.execute(text("INSERT INTO bil_tenant_ledger(tenant_id,description,kind,amount,ref) VALUES(11,'Historical rent','charge',100,'D0099')"));self.session.commit()
        before=self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all()
        self.assertEqual(self.act(self.plan_form()).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT * FROM bil_tenant_ledger')).all(),before)

    def test_fixed_chooser_and_dedicated_rent_navigation(self):
        page=self.client.get(self.path()+'/fixed',headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//a[text()="Rent" or text()="Utilities"]/text()'),['Rent','Utilities'])
        self.assertFalse(tree.xpath('//form|//table'))
        rent=self.client.get(self.path()+'/fixed/rent',headers=self.headers)
        tree=html.fromstring(rent.data)
        self.assertEqual(tree.xpath('//a[text()="Back to Fixed Charges"]/@href'),[self.path()+'/fixed'])
        self.assertEqual(tree.xpath('//*[@id="monthly-rent-plan"]/thead/tr/th/text()'),['Due Date','Amount','Status','Action'])
        self.assertEqual(len(tree.xpath('//*[@id="monthly-rent-plan"]//button[@disabled]')),3)

    def test_unassigned_post_only_records_selected_due_month(self):
        response=self.act(self.plan_form('post','2026-03-31'))
        self.assertEqual(response.status_code,302,response.text)
        self.assertEqual(response.location,self.path()+'/fixed/rent')
        self.assertEqual(self.session.execute(text('SELECT month FROM bil_tenant_ledger')).scalars().all(),['2026-03'])
        self.assertEqual(self.act(self.plan_form('post','2026-03-31')).status_code,302)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_tenant_ledger')).scalar(),1)
