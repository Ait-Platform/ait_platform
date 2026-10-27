"""PM-only lease associations and property/agent isolation; local PostgreSQL only."""
import copy,json,types,unittest
from functools import wraps
from unittest.mock import patch
from sqlalchemy import text,Column,Integer,String,Boolean
from sqlalchemy.orm import declarative_base
from flask import request,abort,render_template
from lxml import html
import test_pm_lease_lessee_persistence as fixture

class RecentSOATests(unittest.TestCase):
    def setUp(self):
        fixture.LesseePersistenceTests.setUp(self)
        self.session.execute(text('CREATE TEMP TABLE bil_lease(id integer PRIMARY KEY,tenant_id integer,property_id integer)'))
        self.session.execute(text("CREATE TEMP TABLE debtor(id integer PRIMARY KEY,name text); INSERT INTO debtor VALUES (1,'ProShop sentinel')"))
        self.session.execute(text("INSERT INTO bil_property VALUES (27,'Agent managed second property','Other address',7,false),(28,'Unrelated property','Private address',8,false)"))
        self.session.execute(text("UPDATE bil_tenant SET property_id=NULL WHERE id=11"))
        self.session.execute(text("INSERT INTO bil_lease VALUES(1,22,27)"))
        self.session.execute(text("INSERT INTO bil_tenant(id,manager_id,property_id,party_type,name,identity_number) VALUES (33,8,28,'natural_person','Private Tenant','PRIVATE'),(44,7,26,'natural_person','Unselected Register Tenant','UNSELECTED')"))
        self.session.execute(text("INSERT INTO bil_lease VALUES(2,33,28)"))
        items=copy.deepcopy(self.initial);items['_parties']['lessees']=[{'tenant_id':11,'party_type':'natural_person','legal_name':'Natural Lessee','identity_number':'NAT-11'}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb),updated_at=now()'),dict(items=json.dumps(items)));self.session.commit()
        from app.program_billing import lease_soa
        scoped=patch.object(lease_soa,'db',self.db);scoped.start();self.addCleanup(scoped.stop)
        base=declarative_base()
        class Property(base):
            __tablename__='bil_property'
            id=Column(Integer,primary_key=True);manager_id=Column(Integer);name=Column(String);is_archived=Column(Boolean)
        def authenticated(fn):
            @wraps(fn)
            def wrapped(*args,**kw):
                if request.headers.get('X-Test-Owner')!='yes':abort(403)
                return fn(*args,**kw)
            return wrapped
        env=dict(__package__='app.program_billing',billing_bp=types.SimpleNamespace(route=lambda *args,**kw:lambda f:f),login_required=authenticated,abort=abort,db=self.db,BilProperty=Property,current_user=self.user,request=request,render_template=render_template)
        fixture.functions('app/program_billing/routes.py',{'tenant_accounts'},env)
        self.app._got_first_request=False;self.app.add_url_rule('/manager/tenant_accounts',endpoint='billing_bp.tenant_accounts',view_func=env['tenant_accounts'])
        for endpoint in ('learner_dashboard','utilities_hub','property_portfolio','lease_live_view'):
            if 'billing_bp.'+endpoint not in self.app.view_functions:self.app.add_url_rule('/soa-test/'+endpoint,endpoint='billing_bp.'+endpoint,view_func=lambda:'Existing PM action')
        self.app.add_url_rule('/manager/leases',endpoint='billing_bp.pm_lease_management',defaults={'property_id':None},view_func=self.app.view_functions['billing_bp.pm_lease_management'])
        self.url='/manager/tenant_accounts';self.headers={'X-Test-Owner':'yes'}
    def rows(self,response):return html.fromstring(response.data).xpath('//tbody/tr')
    def test_saved_lease_selection_discovers_tenant_without_property_or_debtor_link(self):
        before=list(self.session.execute(text('SELECT * FROM bil_tenant ORDER BY id')).mappings())
        initial=self.client.get(self.url,headers=self.headers);self.assertEqual(initial.status_code,200)
        tree=html.fromstring(initial.data);self.assertEqual(tree.xpath('//*[@id="soa-property"]/option/@value'),['','27','26']);self.assertFalse(self.rows(initial))
        self.assertEqual(len(tree.xpath('//script[contains(@src,"pm_soa_property.js")]')),1)
        response=self.client.get(self.url+'?property_id=26',headers=self.headers);self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(len(self.rows(response)),1);self.assertIn('Natural Lessee',response.text);self.assertNotIn('Unselected Register Tenant',response.text)
        for forbidden in ('Job Card','creditors','materials','View Ledger','ProShop sentinel','Balance Owed'):
            self.assertNotIn(forbidden,response.text)
        self.assertIn('View Lease',response.text);self.assertIn('draft_id='+str(self.identifier),response.text)
        switched=self.client.get(self.url+'?property_id=27',headers=self.headers);self.assertEqual(len(self.rows(switched)),1);self.assertIn('Entity Lessee',switched.text)
        self.assertEqual(before,list(self.session.execute(text('SELECT * FROM bil_tenant ORDER BY id')).mappings()))
        self.assertEqual(self.session.execute(text('SELECT name FROM debtor')).scalar(),'ProShop sentinel')
    def test_agent_scope_current_selection_and_empty_state(self):
        self.assertEqual(self.client.get(self.url+'?property_id=28',headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.url).status_code,403)
        items=copy.deepcopy(self.initial);items['_parties']['lessees']=[]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb)'),dict(items=json.dumps(items)));self.session.commit()
        empty=self.client.get(self.url+'?property_id=26',headers=self.headers);self.assertIn('No lease tenants found for this property.',empty.text)
        self.user.id=8
        page=self.client.get(self.url,headers=self.headers);self.assertEqual(html.fromstring(page.data).xpath('//*[@id="soa-property"]/option/@value'),['','28'])
        self.assertEqual(self.client.get(self.url+'?property_id=26',headers=self.headers).status_code,404)
        self.assertIn('Private Tenant',self.client.get(self.url+'?property_id=28',headers=self.headers).text)
    def test_no_shared_debtor_or_job_card_reads(self):
        from sqlalchemy import event
        statements=[]
        def record(conn,cursor,statement,parameters,context,executemany):statements.append(statement.lower())
        event.listen(self.conn,'before_cursor_execute',record)
        try:response=self.client.get(self.url+'?property_id=26',headers=self.headers)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(response.status_code,200)
        self.assertTrue(any('bil_lease_draft' in sql for sql in statements))
        self.assertFalse(any('debtor' in sql or 'job_card' in sql or 'invoice' in sql for sql in statements))
        self.assertTrue(all(sql.lstrip().startswith('select') for sql in statements))

    def test_legacy_unit_relationship_without_lease_property_column_returns_200(self):
        self.session.execute(text('ALTER TABLE bil_lease RENAME COLUMN property_id TO sectional_unit_id'))
        self.session.execute(text('CREATE TEMP TABLE bil_sectional_unit(id integer PRIMARY KEY,property_id integer)'))
        self.session.execute(text('INSERT INTO bil_sectional_unit VALUES(27,27),(28,28),(26,26)'))
        self.session.execute(text('INSERT INTO bil_lease VALUES(3,11,26)'));self.session.commit()
        from sqlalchemy import inspect
        self.assertNotIn('property_id',{c['name'] for c in inspect(self.conn).get_columns('bil_lease')})
        response=self.client.get(self.url+'?property_id=26',headers=self.headers)
        self.assertEqual(response.status_code,200,response.text);self.assertEqual(len(self.rows(response)),1);self.assertIn('Natural Lessee',response.text)
        second=self.client.get(self.url+'?property_id=27',headers=self.headers);self.assertIn('Entity Lessee',second.text);self.assertNotIn('Natural Lessee',second.text)
        self.assertEqual(self.client.get(self.url+'?property_id=28',headers=self.headers).status_code,404)
    def test_legacy_tenant_relationship_without_lease_property_or_unit_column(self):
        self.session.execute(text('ALTER TABLE bil_lease DROP COLUMN property_id'))
        self.session.execute(text('UPDATE bil_tenant SET property_id=27 WHERE id=22'));self.session.commit()
        response=self.client.get(self.url+'?property_id=27',headers=self.headers)
        self.assertEqual(response.status_code,200,response.text);self.assertIn('Entity Lessee',response.text)
    def test_real_local_public_schema_read_only_without_fixture_tables(self):
        # A second connection has no session-temporary fixture tables: inspect and query
        # the existing public catalog, never create/migrate/seed application data.
        from sqlalchemy import inspect
        from app.program_billing import lease_soa
        with self.engine.connect() as connection:
            connection.execute(text('SET TRANSACTION READ ONLY'))
            self.assertEqual(connection.execute(text('SELECT current_database()')).scalar(),'ait_local_db')
            catalog=inspect(connection)
            actual={row['name'] for row in catalog.get_columns('bil_lease',schema='public')}
            self.assertIn('tenant_id',actual)
            property_row=connection.execute(text('SELECT id,manager_id FROM public.bil_property WHERE manager_id IS NOT NULL AND is_archived=false ORDER BY id LIMIT 1')).mappings().first()
            if property_row is None:self.skipTest('Actual local database has no managed property; catalog inspection completed.')
            previous=self.user.id;self.user.id=property_row['manager_id']
            try:
                with patch.object(lease_soa,'db',types.SimpleNamespace(session=types.SimpleNamespace(connection=lambda:connection,execute=connection.execute))):
                    entries=lease_soa.tenants_for(property_row['id'])
                for entry in entries:
                    if 'property_id' in actual:
                        linked=connection.execute(text('SELECT 1 FROM public.bil_lease WHERE tenant_id=:t AND property_id=:p LIMIT 1'),dict(t=entry['id'],p=property_row['id'])).first()
                        self.assertIsNotNone(linked)
            finally:self.user.id=previous;connection.rollback()
