"""Existing METSOA statement/exception routes, scope and checkout on PostgreSQL."""
import sys,types
from flask import request,abort,render_template,redirect,url_for,flash,session
from sqlalchemy import text,Table,MetaData
from sqlalchemy.orm import registry
from flask_sqlalchemy.query import Query
from lxml import html
import test_pm_utilities_reconciliation as fixture
import test_pm_lease_lessee_persistence as loader

class UtilitiesStatementTests(fixture.ReconciliationTests):
    def setUp(self):
        super().setUp()
        self.app.jinja_env.globals['current_user']=types.SimpleNamespace(email='owner@example.com')
        auth=types.ModuleType('app.models.auth');auth.AitTokenWallet=types.SimpleNamespace(query=types.SimpleNamespace(filter_by=lambda **kw:types.SimpleNamespace(first=lambda:types.SimpleNamespace(balance=1000))))
        from unittest.mock import patch
        auth_patch=patch.dict(sys.modules,{'app.models.auth':auth});auth_patch.start();self.addCleanup(auth_patch.stop)
        self.session.execute(text('CREATE TEMP TABLE bil_statement_payment(id integer PRIMARY KEY,manager_id integer,month text,amount_paid_cents integer)'))
        self.session.execute(text('CREATE TEMP TABLE bil_platform_settings(id integer PRIMARY KEY,base_price_cents integer,included_meters integer,extra_meter_price_cents integer)'))
        self.session.execute(text("INSERT INTO bil_statement_payment VALUES(1,7,'2026-10',10000)"))
        self.session.execute(text('INSERT INTO bil_platform_settings VALUES(1,10000,2,1500)'));self.session.commit();self.conn.commit()
        mapper=registry();models=sys.modules['app.models.billing']
        for name,table in [('BilStatementPayment','bil_statement_payment'),('BilPlatformSettings','bil_platform_settings')]:
            cls=type(name,(),{});record=Table(table,MetaData(),autoload_with=self.conn);mapper.map_imperatively(cls,record,primary_key=[record.c.id]);cls.query=Query(cls,session=self.session);setattr(models,name,cls)
        def auth(f):
            def wrapped(*a,**kw):
                if request.headers.get('X-Test-Owner')!='yes':abort(403)
                return f(*a,**kw)
            return wrapped
        self.built=[]
        def builder(p,m,**kw):self.built.append((p,m,kw));return [],0
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(route=lambda *a,**kw:lambda f:f),login_required=auth,request=request,abort=abort,render_template=render_template,redirect=redirect,url_for=url_for,flash=flash,session=session,current_user=self.user,db=self.db,build_electrical_rows=builder,build_water_rows=builder)
        loader.functions('app/program_billing/routes.py',{'metsoa','exception_metsoa','billing_checkout'},env)
        self.app._got_first_request=False;self.app.view_functions['billing_bp.metsoa']=env['metsoa']
        self.app.add_url_rule('/exceptions/<int:property_id>/<month>',endpoint='billing_bp.exception_metsoa',view_func=env['exception_metsoa'])
        self.app.add_url_rule('/checkout/<month>',endpoint='billing_bp.billing_checkout',view_func=env['billing_checkout'])
        for endpoint in ('email_metsoa','email_exception_metsoa'):
            self.app.add_url_rule('/email/'+endpoint+'/<int:property_id>/<month>',endpoint='billing_bp.'+endpoint,view_func=lambda **kw:'Existing mailer')
        self.app.add_url_rule('/unlock/<month>',endpoint='billing_bp.billing_unlock',view_func=lambda **kw:'Existing checkout')
    def test_standard_and_exception_views_share_property_context_without_posting(self):
        before=self.snapshot()
        for path in ('/utilities/metsoa/26/2026-10','/exceptions/26/2026-10'):
            page=self.client.get(path,headers=self.headers);self.assertEqual(page.status_code,200,page.text)
            tree=html.fromstring(page.data)
            self.assertEqual(tree.xpath('//nav[@aria-label="Utilities"]/a/text()'),['Readings','Consumption','METSOA','Setup Wizard'])
            self.assertIn('Synthetic property',page.text);self.assertIn('2026-10',page.text)
        self.assertEqual(before,self.snapshot());self.assertTrue(all(p==26 for p,m,kw in self.built))
        self.assertEqual(self.client.get('/exceptions/28/2026-10',headers=self.headers).status_code,403)
    def test_unpaid_statement_checkout_keeps_property_and_uses_actual_property_id(self):
        response=self.client.get('/utilities/metsoa/26/2026-11',headers=self.headers)
        self.assertEqual(response.status_code,302);self.assertEqual(response.location,'/checkout/2026-11?property_id=26')
        page=self.client.get(response.location,headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        with self.client.session_transaction() as stored:self.assertEqual(stored['metro_billing_meters'],1);self.assertEqual(stored['metro_billing_property_id'],26)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_statement_payment')).scalar(),1)
        self.assertEqual(self.client.get('/checkout/2026-11?property_id=28',headers=self.headers).status_code,403)
