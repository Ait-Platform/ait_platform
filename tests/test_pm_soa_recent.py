"""Existing SOA account table/actions, scoped search and pagination on local PostgreSQL."""
import sys,types,unittest
from functools import wraps
from datetime import datetime
from unittest.mock import patch
from sqlalchemy import text,Column,Integer,String,DateTime,Boolean,func,cast
from sqlalchemy.orm import declarative_base
from flask import request,abort,current_app,render_template,flash
from lxml import html
import test_pm_lease_lessee_persistence as fixture
functions=fixture.functions

class RecentSOATests(unittest.TestCase):
    def setUp(self):
        fixture.LesseePersistenceTests.setUp(self)
        self.session.execute(text('CREATE TEMP TABLE debtor(id integer PRIMARY KEY,user_id integer,slug_reference text,reference_id integer,name text,email text,phone text,created_at timestamp)'))
        self.session.execute(text('CREATE TEMP TABLE debtor_ledger(id integer PRIMARY KEY,debtor_id integer,kind text,amount integer)'))
        self.session.execute(text("INSERT INTO debtor SELECT n,CASE WHEN n=99 THEN 8 ELSE 7 END,'billing',11,'Account '||n,'account@example.test',NULL,'2026-10-01'::timestamp+n*interval '1 day' FROM generate_series(1,15) n"))
        self.session.execute(text("INSERT INTO debtor VALUES (99,8,'billing',11,'Private account',NULL,NULL,'2026-12-01')"))
        self.session.execute(text("INSERT INTO debtor_ledger VALUES (1,15,'debit',12345),(2,15,'credit',1000)"))
        self.session.execute(text("UPDATE bil_tenant SET notes='[AIT Tenant Reference: TR-1234567890ABCDEF]' WHERE id=11"));self.session.commit()
        self.session.execute(text("INSERT INTO bil_property VALUES (27,'Agent managed second property','Other address',7,false),(28,'Unrelated property','Private address',8,false)"))
        self.session.execute(text("UPDATE bil_tenant SET property_id=27 WHERE id=22"))
        self.session.execute(text("INSERT INTO debtor VALUES (16,7,'billing',22,'Second property tenant',NULL,NULL,'2026-10-27'),(100,8,'billing',33,'Unrelated tenant account',NULL,NULL,'2026-10-27')"))
        self.session.execute(text("INSERT INTO bil_tenant(id,manager_id,property_id,party_type,name,identity_number) VALUES(33,8,28,'natural_person','Unrelated Tenant','PRIVATE')"));self.session.commit()
        base=declarative_base();session=self.session
        class Debtor(base):
            __tablename__='debtor'
            id=Column(Integer,primary_key=True);user_id=Column(Integer);slug_reference=Column(String);reference_id=Column(Integer);name=Column(String);email=Column(String);phone=Column(String);created_at=Column(DateTime)
            @property
            def ledgers(row):return [types.SimpleNamespace(**dict(entry)) for entry in session.execute(text('SELECT kind,amount FROM debtor_ledger WHERE debtor_id=:id'),dict(id=row.id)).mappings()]
        class Tenant(base):
            __tablename__='bil_tenant'
            id=Column(Integer,primary_key=True);manager_id=Column(Integer);property_id=Column(Integer);name=Column(String);notes=Column(String)
        class Property(base):
            __tablename__='bil_property'
            id=Column(Integer,primary_key=True);manager_id=Column(Integer);name=Column(String);is_archived=Column(Boolean)
        class Query:
            def __get__(self,instance,owner):return session.query(owner)
        Debtor.query=Query()
        module=types.ModuleType('app.models.debtors');module.Debtor=Debtor
        modelpatch=patch.dict(sys.modules,{'app.models.debtors':module});modelpatch.start();self.addCleanup(modelpatch.stop)
        def authenticated(fn):
            @wraps(fn)
            def wrapped(*args,**kw):
                if request.headers.get('X-Test-Owner')!='yes':abort(403)
                return fn(*args,**kw)
            return wrapped
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(route=lambda *args,**kw:lambda f:f),login_required=authenticated,abort=abort,db=types.SimpleNamespace(session=session,func=func),BilTenant=Tenant,BilProperty=Property,current_user=self.user,request=request,render_template=render_template,current_app=current_app,flash=flash)
        functions('app/program_billing/routes.py',{'tenant_accounts'},env)
        self.app._got_first_request=False;self.app.add_url_rule('/manager/tenant_accounts',endpoint='billing_bp.tenant_accounts',view_func=env['tenant_accounts'])
        for endpoint in ('learner_dashboard','tenant_ledger','utilities_hub','property_portfolio'):
            if 'billing_bp.'+endpoint not in self.app.view_functions:self.app.add_url_rule('/soa-test/'+endpoint,endpoint='billing_bp.'+endpoint,view_func=lambda:'Existing action')
        self.app.add_url_rule('/manager/leases',endpoint='billing_bp.pm_lease_management',defaults={'property_id':None},view_func=self.app.view_functions['billing_bp.pm_lease_management'])
        self.url='/manager/tenant_accounts';self.headers={'X-Test-Owner':'yes'}
    def rows(self,response):return html.fromstring(response.data).xpath('//tbody/tr')
    def test_property_first_prompt_auto_submit_and_exact_existing_actions(self):
        before=list(self.session.execute(text('SELECT * FROM debtor_ledger ORDER BY id')).mappings())
        initial=self.client.get(self.url,headers=self.headers);self.assertEqual(initial.status_code,200,initial.text)
        tree=html.fromstring(initial.data)
        self.assertEqual(tree.xpath('//*[@id="soa-property"]/option/@value'),['','27','26'])
        self.assertFalse(self.rows(initial));self.assertIn('Select a property to display',initial.text)
        self.assertEqual(tree.xpath('//select/@name'),['property_id'])
        self.assertEqual(len(tree.xpath('//script[contains(@src,"pm_soa_property.js")]')),1)
        for label in ('Search Tenant','Show Recent','Show More','start_date','end_date'):
            self.assertNotIn(label,initial.text)
        response=self.client.get(self.url+'?property_id=26',headers=self.headers);self.assertEqual(response.status_code,200,response.text)
        rows=self.rows(response);self.assertEqual(len(rows),15)
        tree=html.fromstring(response.data);self.assertEqual(tree.xpath('//thead/tr/th/text()'),['Tenant','Contact','Balance Owed','Actions'])
        self.assertEqual([link.text_content().strip() for link in tree.xpath('//tbody/tr/td[4]/a')],['View Ledger →']*15)
        self.assertIn('113.45',response.text);self.assertNotIn('Private account',response.text);self.assertNotIn('Second property tenant',response.text)
        switched=self.client.get(self.url+'?property_id=27',headers=self.headers);self.assertEqual(len(self.rows(switched)),1);self.assertIn('Second property tenant',switched.text)
        self.assertEqual(self.client.get(self.url+'?property_id=28',headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.url+'?property_id=invalid',headers=self.headers).status_code,400)
        self.assertEqual(self.client.get(self.url).status_code,403)
        after=list(self.session.execute(text('SELECT * FROM debtor_ledger ORDER BY id')).mappings());self.assertEqual(before,after)
    def test_assigned_manager_agent_uses_existing_scope_and_other_users_are_isolated(self):
        # Manager 7 need not be a registered owner: existing manager_id assignment grants access.
        response=self.client.get(self.url,headers=self.headers);self.assertNotIn('Unrelated property',response.text)
        self.user.id=8
        response=self.client.get(self.url,headers=self.headers);tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//*[@id="soa-property"]/option/@value'),['','28'])
        self.assertEqual(self.client.get(self.url+'?property_id=26',headers=self.headers).status_code,404)
        allowed=self.client.get(self.url+'?property_id=28',headers=self.headers);self.assertEqual(len(self.rows(allowed)),1);self.assertIn('Unrelated tenant account',allowed.text)

if __name__=='__main__':unittest.main()
