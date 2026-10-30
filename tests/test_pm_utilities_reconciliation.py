"""Bulk reconciliation uses property-only persisted consumption, without posting."""
import sys,types
from unittest.mock import patch
from flask import request,abort,render_template
from sqlalchemy import text
from lxml import html
import test_pm_utilities_readings as fixture
import test_pm_lease_lessee_persistence as loader

class ReconciliationTests(fixture.UtilitiesReadingsTests):
    def setUp(self):
        super().setUp()
        from app.program_billing import utilities_reconciliation
        scoped=patch.object(utilities_reconciliation,'db',self.db);scoped.start();self.addCleanup(scoped.stop)
        self.reconcile=utilities_reconciliation.reconcile
        models=sys.modules['app.models.billing']
        def auth(f):
            def wrapped(*a,**kw):
                if request.headers.get('X-Test-Owner')!='yes':abort(403)
                return f(*a,**kw)
            return wrapped
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(route=lambda *a,**kw:lambda f:f),login_required=auth,request=request,abort=abort,render_template=render_template,current_user=self.user)
        loader.functions('app/program_billing/routes.py',{'get_all_property_meters','consumption_review'},env)
        self.app.view_functions['billing_bp.consumption_review']=env['consumption_review']
        self.app._got_first_request=False
        self.app.add_url_rule('/consumption-email/<int:property_id>/<month>',endpoint='billing_bp.email_consumption',view_func=lambda **kw:'Existing email')
    def group(self,bulk,child):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,status) VALUES(2,26,'BULK','Water','active')"))
        self.session.execute(text('UPDATE bil_meter SET parent_meter_id=2 WHERE id=1'))
        self.session.execute(text("UPDATE bil_consumption SET consumption=:c,last_date='2026-10-01',new_date='2026-10-31' WHERE id=1"),dict(c=child))
        self.session.execute(text("INSERT INTO bil_consumption(meter_id,month,consumption,last_date,new_date) VALUES(2,'2026-10',:c,'2026-10-01','2026-10-31')"),dict(c=bulk));self.session.commit()
    def test_positive_zero_and_negative_differences_without_financial_writes(self):
        self.group(100,70)
        for bulk,actual,charge in [(100,30,30),(70,0,0),(50,-20,0)]:
            self.session.execute(text('UPDATE bil_consumption SET consumption=:v WHERE meter_id=2'),dict(v=bulk));self.session.commit()
            before=self.snapshot()
            with self.app.test_request_context():result=self.reconcile(26,'2026-10')
            self.assertEqual(result[0]['difference'],actual);self.assertEqual(result[0]['chargeable_difference'],charge)
            self.assertEqual(result[0]['allocation'],'Owner');self.assertEqual(before,self.snapshot())
    def test_consumption_view_is_property_scoped_and_keeps_menu_context(self):
        self.group(50,70)
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number) VALUES(3,28,'PRIVATE','Water','MET-26')"));self.session.commit()
        page=self.client.get('/utilities/consumption_review/26/2026-10',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        self.assertIn('Actual difference',page.text);self.assertIn('-20',page.text);self.assertNotIn('PRIVATE',page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//nav[@aria-label="Utilities"]/a/text()'),['Readings','Consumption','METSOA','Setup Wizard','Municipal Accounts'])
        self.assertEqual(self.client.get('/utilities/consumption_review/28/2026-10',headers=self.headers).status_code,403)
    def test_incomplete_period_is_not_silently_treated_as_zero(self):
        self.group(100,70)
        self.session.execute(text("UPDATE bil_consumption SET new_date='2026-10-30' WHERE meter_id=1"));self.session.commit()
        with self.app.test_request_context():result=self.reconcile(26,'2026-10')
        self.assertIsNone(result[0]['chargeable_difference']);self.assertIn('mismatched',result[0]['status'])
