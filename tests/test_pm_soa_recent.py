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
            id=Column(Integer,primary_key=True);manager_id=Column(Integer);name=Column(String)
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
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(route=lambda *args,**kw:lambda f:f),login_required=authenticated,db=types.SimpleNamespace(session=session,func=func),BilTenant=Tenant,BilProperty=Property,current_user=self.user,request=request,render_template=render_template,current_app=current_app,flash=flash)
        functions('app/program_billing/routes.py',{'tenant_accounts'},env)
        self.app._got_first_request=False;self.app.add_url_rule('/manager/tenant_accounts',endpoint='billing_bp.tenant_accounts',view_func=env['tenant_accounts'])
        for endpoint in ('learner_dashboard','tenant_ledger','utilities_hub','property_portfolio'):
            if 'billing_bp.'+endpoint not in self.app.view_functions:self.app.add_url_rule('/soa-test/'+endpoint,endpoint='billing_bp.'+endpoint,view_func=lambda:'Existing action')
        self.app.add_url_rule('/manager/leases',endpoint='billing_bp.pm_lease_management',defaults={'property_id':None},view_func=self.app.view_functions['billing_bp.pm_lease_management'])
        self.url='/manager/tenant_accounts';self.headers={'X-Test-Owner':'yes'}
    def rows(self,response):return html.fromstring(response.data).xpath('//tbody/tr')
    def test_recent_ten_search_more_financial_records_and_exact_existing_actions(self):
        before=list(self.session.execute(text('SELECT * FROM debtor_ledger ORDER BY id')).mappings())
        response=self.client.get(self.url,headers=self.headers);self.assertEqual(response.status_code,200,response.text)
        rows=self.rows(response);self.assertEqual(len(rows),10)
        self.assertEqual(rows[0].xpath('./td[1]/text()')[0],'Account 15');self.assertEqual(rows[-1].xpath('./td[1]/text()')[0],'Account 6')
        tree=html.fromstring(response.data)
        self.assertEqual(tree.xpath('//thead/tr/th/text()'),['Tenant','Contact','Balance Owed','Actions'])
        self.assertEqual([link.text_content().strip() for link in tree.xpath('//tbody/tr/td[4]/a')],['View Ledger →']*10)
        self.assertNotIn('Open Ledger',response.text);self.assertNotIn('start_date',response.text);self.assertNotIn('end_date',response.text)
        self.assertIn('Show More',response.text);self.assertIn('113.45',response.text);self.assertNotIn('Private account',response.text)
        more=self.client.get(self.url+'?limit=20',headers=self.headers);self.assertEqual(len(self.rows(more)),15);self.assertNotIn('Show More',more.text)
        by_name=self.client.get(self.url+'?q=Account+15',headers=self.headers);self.assertEqual(len(self.rows(by_name)),1)
        for query in ('synthetic','TR-1234567890ABCDEF','Natural Lessee'):
            result=self.client.get(self.url,query_string={'q':query},headers=self.headers);self.assertEqual(len(self.rows(result)),10,result.text)
        result=self.client.get(self.url+'?q=private',headers=self.headers);self.assertFalse(self.rows(result))
        self.assertEqual(self.client.get(self.url).status_code,403)
        after=list(self.session.execute(text('SELECT * FROM debtor_ledger ORDER BY id')).mappings());self.assertEqual(before,after)
    def test_soa_navigation_and_heading_only_presentation_rename(self):
        response=self.client.get(self.url,headers=self.headers)
        tree=html.fromstring(response.data);self.assertEqual(tree.xpath('//h1/text()'),['SOA'])
        self.assertNotIn('Occupier SOA',response.text)
        self.assertIn('Search Tenant',response.text);self.assertIn('Show Recent',response.text)

if __name__=='__main__':unittest.main()
