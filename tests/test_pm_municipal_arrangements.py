"""Municipal arrangements on isolated PostgreSQL, no application startup or live writes."""
import ast,importlib.util,json,types,sys,unittest,uuid
from datetime import date
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import text,event
from sqlalchemy.orm import Session
from alembic.operations import Operations
from alembic.migration import MigrationContext
from flask import request,session,current_app,abort
from flask_login import current_user
from flask_wtf.csrf import CSRFProtect,generate_csrf
from lxml import html
import test_pm_admin_landing as fixture
ROOT=fixture.ROOT

class ArrangementTests(fixture.AdminLandingTests):
    url='/billing/utilities/26/payment-arrangements'
    def setUp(self):
        super().setUp()
        self.session=Session(self.conn);self.addCleanup(self.session.close);self.db=types.SimpleNamespace(session=self.session)
        self.conn.execute(text('SET search_path TO pg_temp,public'))
        for sql in ('CREATE TEMP TABLE "user"(id integer PRIMARY KEY,email text,is_active integer)',
                    'CREATE TEMP TABLE auth_approved_admin(id integer PRIMARY KEY,email text,active boolean)',
                    'CREATE TEMP TABLE bil_property(id integer PRIMARY KEY,manager_id integer,name text,address text)',
                    'CREATE TEMP TABLE bil_muni_account(id integer PRIMARY KEY,property_id integer,account_number text,ca_contract_number text,ca_agreement_amount double precision,ca_installments_raised double precision,ca_installment_amount double precision,ca_amount_owing double precision,ca_remaining_periods integer)',
                    'CREATE TEMP TABLE bil_muni_payment(id integer PRIMARY KEY,account_id integer,month text,payment_date date,amount double precision,reference text)',
                    'CREATE TEMP TABLE bil_tenant_ledger(id integer PRIMARY KEY,amount numeric(18,2))',
                    'CREATE TEMP TABLE bil_statement(id integer PRIMARY KEY,tenant_id integer)',
                    'CREATE TEMP TABLE bil_metsoa_meter_month(id integer PRIMARY KEY,total_due numeric(18,2))'):
            self.conn.execute(text(sql))
        self.conn.execute(text("INSERT INTO \"user\" VALUES(7,'admin@example.test',1),(8,'other@example.test',1)"))
        self.conn.execute(text("INSERT INTO auth_approved_admin VALUES(1,'admin@example.test',true)"))
        self.conn.execute(text("INSERT INTO bil_property VALUES(26,7,'Almond','Test address'),(27,7,'Dale','Test address'),(28,8,'Private','Private address')"))
        self.conn.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number) VALUES(1,26,'MET-26'),(2,27,'MET-27'),(3,28,'PRIVATE')"))
        self.conn.execute(text("INSERT INTO bil_muni_payment VALUES(1,1,'2026-10','2026-10-15',100,'MUNI-P1'),(2,2,'2026-10','2026-10-15',100,'OTHER-P2')"))
        self.conn.execute(text('INSERT INTO bil_tenant_ledger VALUES(1,123.45)'))
        self.conn.execute(text('INSERT INTO bil_statement VALUES(1,12)'))
        self.conn.execute(text('INSERT INTO bil_metsoa_meter_month VALUES(1,444.44)'));self.conn.commit()
        before=self.financial_snapshot()
        spec=importlib.util.spec_from_file_location('arrangement_migration',ROOT/'migrations/versions/pm_muni_arrangements_001.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.conn.commit();self.assertEqual(before,self.financial_snapshot())
        self.assertEqual(self.conn.execute(text("SELECT relpersistence FROM pg_class WHERE oid='bil_municipal_arrangement'::regclass")).scalar(),'t');self.conn.commit()
        self.service=types.ModuleType('app.program_billing.municipal_arrangements');self.service.__package__='app.program_billing'
        self.service.__dict__.update(db=self.db,is_admin=fixture.function('app/utils/roles.py','is_admin',dict(current_user=current_user,session=session)))
        self.service.property_scope=fixture.function('app/program_billing/metsoa_setup_safety.py','property_scope',dict(db=self.db,current_user=current_user,text=text,abort=abort))
        tree=ast.parse((ROOT/'app/program_billing/municipal_arrangements.py').read_text())
        tree.body=[n for n in tree.body if not (isinstance(n,ast.ImportFrom) and ((n.module or '').startswith('app.') or n.level))]
        exec(compile(tree,'municipal_arrangements.py','exec'),self.service.__dict__);self.service.today=lambda:date(2026,11,2)
        modules=patch.dict(sys.modules,{self.service.__name__:self.service});modules.start();self.addCleanup(modules.stop)
        self.app.context_processor(fixture.function('app/program_billing/routes.py','municipal_arrangement_navigation',dict(__package__='app.program_billing')))
        CSRFProtect(self.app);self.app.jinja_env.globals['csrf_token']=generate_csrf
        for name in ('municipal_payment_arrangements','municipal_arrangement_detail'):
            path,endpoint=fixture.declared_route('app/program_billing/routes.py',name,'billing_bp')
            view=fixture.function('app/program_billing/routes.py',name,dict(__package__='app.program_billing'))
            self.app.add_url_rule(path,endpoint=endpoint,view_func=view,methods=['GET','POST'])
        for name in ('learner_dashboard','municipal_accounts','meter_readings','metro_reconciliation','meter_exceptions','generate_municipal_metsoa','bulk_meter_linking'):
            path,endpoint=fixture.declared_route('app/program_billing/routes.py',name,'billing_bp')
            self.app.add_url_rule(path,endpoint=endpoint,view_func=lambda **kw:'Unrelated page')
        from sqlalchemy import Table,MetaData
        from sqlalchemy.orm import registry
        from flask_sqlalchemy.query import Query
        from flask import render_template,redirect,url_for,flash
        table=Table('bil_property',MetaData(),autoload_with=self.conn)
        mapper=registry();model=type('BilProperty',(),{});mapper.map_imperatively(model,table);model.query=Query(model,session=self.session)
        model_module=types.ModuleType('app.models.billing');model_module.BilProperty=model
        mock=patch.dict(sys.modules,{'app.models.billing':model_module});mock.start();self.addCleanup(mock.stop)
        scoped_module=types.ModuleType('app.program_billing.metsoa_setup_safety');scoped_module.property_scope=self.service.property_scope
        role_module=types.ModuleType('app.utils.roles');role_module.is_admin=self.service.is_admin
        mock=patch.dict(sys.modules,{'app.program_billing.metsoa_setup_safety':scoped_module,'app.utils.roles':role_module});mock.start();self.addCleanup(mock.stop)
        self.app.view_functions['billing_bp.utilities_hub']=fixture.function('app/program_billing/routes.py','utilities_hub',dict(__package__='app.program_billing',request=request,current_user=current_user,abort=abort,flash=flash,redirect=redirect,url_for=url_for,render_template=lambda name,**context:sys.modules['app.program_billing.lease_management'].render_pm_template(name,**context)))
        for rule in self.app.url_map.iter_rules():
            if rule.endpoint=='billing_bp.utilities_hub':rule.methods.add('POST')
        @self.app.after_request
        def finish_fixture_transaction(response):
            self.session.commit();self.conn.commit();self.session.close()
            return response
        self.sign_in()
    def financial_snapshot(self):
        return {name:[tuple(r) for r in self.conn.execute(text('SELECT * FROM '+name+' ORDER BY id'))] for name in ('bil_muni_payment','bil_tenant_ledger','bil_statement','bil_metsoa_meter_month')}
    def form(self,**changes):
        data=dict(action='create',request_key=str(uuid.uuid4()),account_id='1',reference='ARR-'+uuid.uuid4().hex[:8],agreement_date='2026-09-01',original_amount='250.00',instalment_amount='100.00',instalments='3',frequency='monthly',first_payment_date='2026-09-30',document_reference='Municipal contract reference')
        data.update(changes);return data
    def post(self,url,data):
        response=self.client.get(self.url+'?new=1')
        token=html.fromstring(response.data).xpath('//input[@name="csrf_token"]/@value')[0]
        response=self.client.post(url,data=dict(data,csrf_token=token));self.conn.commit();return response
    def create(self,**changes):
        response=self.post(self.url,self.form(**changes));self.assertEqual(response.status_code,302,response.text)
        return response.location
    def payment(self,**changes):
        data=dict(action='payment',request_key=str(uuid.uuid4()),payment_date='2026-10-15',amount='100.00',payment_reference='PAY-'+uuid.uuid4().hex[:8]);data.update(changes);return data
    def test_create_schedule_overdue_last_instalment_and_financial_preservation(self):
        before=self.financial_snapshot();target=self.create()
        page=self.client.get(target);self.assertEqual(page.status_code,200,page.text)
        rows=dict(self.conn.execute(text('SELECT * FROM bil_municipal_arrangement')).mappings().one())
        schedule=self.service.schedule(rows)
        self.assertEqual([r['due_date'] for r in schedule],[date(2026,9,30),date(2026,10,30),date(2026,11,30)])
        self.assertEqual([r['amount'] for r in schedule],[Decimal('100'),Decimal('100'),Decimal('50')])
        self.assertEqual([r['status'] for r in schedule],['Overdue','Overdue','Upcoming'])
        self.assertIn('R 250.00',page.text);self.assertEqual(before,self.financial_snapshot())
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_municipal_arrangement_event')).scalar(),1)
    def test_payment_idempotency_balance_overpayment_and_duplicate_reference(self):
        target=self.create();before=self.financial_snapshot();data=self.payment()
        self.assertEqual(self.post(target,data).status_code,302);self.assertEqual(self.post(target,data).status_code,302)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_municipal_arrangement_payment')).scalar(),1)
        self.assertIn('R 150.00',self.client.get(target).text)
        self.assertEqual(self.post(target,self.payment(payment_reference=data['payment_reference'])).status_code,409)
        self.assertEqual(self.post(target,self.payment(amount='150.01')).status_code,400)
        self.assertEqual(self.post(target,self.payment(amount='150.00')).status_code,302)
        self.assertIn('Completed',self.client.get(target).text);self.assertEqual(before,self.financial_snapshot())
    def test_link_existing_payment_preserves_source_and_rejects_other_account(self):
        target=self.create();before=self.financial_snapshot()
        self.assertEqual(self.post(target,self.payment(source_payment_id='2')).status_code,403)
        self.assertEqual(self.post(target,self.payment(source_payment_id='1',amount='999')).status_code,302)
        row=self.conn.execute(text('SELECT * FROM bil_municipal_arrangement_payment')).mappings().one()
        self.assertEqual(row['amount'],Decimal('100'));self.assertEqual(row['reference'],'MUNI-P1')
        other=self.create();self.assertEqual(self.post(other,self.payment(source_payment_id='1')).status_code,409)
        self.assertEqual(before,self.financial_snapshot())
    def test_permissions_property_account_isolation_and_csrf(self):
        self.assertEqual(self.client.get(self.url.replace('/26/','/28/')).status_code,403)
        self.assertEqual(self.post(self.url,self.form(account_id='2')).status_code,403)
        target=self.create();self.assertEqual(self.client.get(target.replace('/26/','/27/')).status_code,404)
        self.assertEqual(self.client.post(self.url,data=self.form()).status_code,400)
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"))
        self.conn.execute(text('UPDATE auth_approved_admin SET active=false'))
        self.conn.execute(text('UPDATE "user" SET is_active=0 WHERE id=7'));self.conn.commit()
        self.assertEqual(self.client.get(self.url).status_code,403)
        self.assertEqual(self.client.get(target).status_code,403)
    def test_invalid_amounts_terms_and_duplicate_create(self):
        data=self.form();first=self.post(self.url,data);self.assertEqual(first.status_code,302)
        self.assertEqual(self.post(self.url,data).location,first.location)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_municipal_arrangement')).scalar(),1)
        self.assertEqual(self.post(self.url,self.form(reference=data['reference'])).status_code,409)
        for changes in (dict(original_amount='NaN'),dict(original_amount='-1'),dict(original_amount='0'),dict(instalment_amount='0'),dict(instalments='0'),dict(instalments='601'),dict(original_amount='301'),dict(first_payment_date='2026-08-01'),dict(frequency='daily')):
            response=self.post(self.url,self.form(**changes));self.assertEqual(response.status_code,400,response.text)
    def test_status_document_history_stale_form_and_cancelled_payment(self):
        target=self.create();data=dict(action='status',request_key=str(uuid.uuid4()),version='1',status='on_hold',document_reference='Letter A',note='Municipality agreed a pause')
        self.assertEqual(self.post(target,data).status_code,302);self.assertEqual(self.post(target,data).status_code,302)
        self.assertEqual(self.post(target,dict(data,request_key=str(uuid.uuid4()))).status_code,409)
        self.assertIn('Municipality agreed a pause',self.client.get(target).text)
        self.assertEqual(self.post(target,dict(data,version='2',status='cancelled',request_key=str(uuid.uuid4()))).status_code,302)
        self.assertEqual(self.post(target,self.payment()).status_code,409)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_municipal_arrangement_event')).scalar(),3)
    def test_weekly_fortnightly_quarterly_and_month_end(self):
        row=dict(original_amount=Decimal('300'),instalment_amount=Decimal('100'),instalments=3,first_payment_date=date(2028,1,31))
        for frequency,expected in [('monthly',[date(2028,1,31),date(2028,2,29),date(2028,3,31)]),('quarterly',[date(2028,1,31),date(2028,4,30),date(2028,7,31)]),('weekly',[date(2028,1,31),date(2028,2,7),date(2028,2,14)]),('fortnightly',[date(2028,1,31),date(2028,2,14),date(2028,2,28)])]:
            result=self.service.schedule(dict(row,frequency=frequency),Decimal('150'),on=date(2028,2,1))
            self.assertEqual([r['due_date'] for r in result],expected);self.assertEqual(result[1]['outstanding'],Decimal('50'))
    def test_browser_create_payment_history_and_navigation(self):
        from playwright.sync_api import sync_playwright
        from http.server import BaseHTTPRequestHandler,HTTPServer
        from threading import Thread
        self.conn.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number) VALUES(4,26,'SECOND-26')"));self.conn.commit()
        case=self;before=self.financial_snapshot()
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.respond(case.client.get(self.path))
            def do_POST(self):
                payload=self.rfile.read(int(self.headers['Content-Length']))
                self.respond(case.client.post(self.path,data=payload,content_type=self.headers.get('Content-Type')))
            def respond(self,response):
                self.send_response(response.status_code);self.send_header('Content-Type',response.content_type)
                if response.location:self.send_header('Location',response.location)
                self.end_headers();self.wfile.write(response.data)
        server=HTTPServer(('127.0.0.1',0),Handler);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(timeout=2)))
        origin='http://127.0.0.1:'+str(server.server_port)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.goto(origin+self.targets['billing_bp.utilities_hub']+'?billing_month=2026-10')
                page.locator('#property_id').select_option('26')
                page.get_by_role('button',name='Municipal Payment Arrangements',exact=True).click()
                self.assertIn(self.url,page.url);self.assertIn('billing_month=2026-10',page.url)
                self.assertEqual(page.locator('h1').inner_text(),'Municipal Payment Arrangements')
                page.locator('#arrangement-account').select_option('4')
                page.wait_for_url('**account_id=4')
                page.get_by_role('link',name='+ Create Payment Arrangement').click()
                form=page.locator('#arrangement-create');self.assertEqual(form.locator('[name="account_id"]').input_value(),'4')
                for key,value in dict(reference='BROWSER-ARR',agreement_date='2026-09-01',original_amount='250',instalment_amount='100',instalments='3').items():form.locator('[name="'+key+'"]').fill(value)
                page.get_by_role('button',name='Save Arrangement',exact=True).click()
                self.assertEqual(page.locator('#arrangement-schedule tbody tr').count(),3)
                self.assertIn('R 250.00',page.locator('#arrangement-outstanding').inner_text())
                form=page.locator('#arrangement-payment')
                for key,value in dict(payment_date='2026-10-15',amount='100',payment_reference='BROWSER-PAY').items():form.locator('[name="'+key+'"]').fill(value)
                page.get_by_role('button',name='Record / Link Payment',exact=True).click()
                self.assertIn('R 150.00',page.locator('#arrangement-outstanding').inner_text())
                self.assertIn('BROWSER-PAY',page.locator('#arrangement-payments').inner_text())
                page.reload();self.assertEqual(page.locator('#arrangement-payments tbody tr').count(),1)
                form=page.locator('#arrangement-status');form.locator('[name="status"]').select_option('on_hold');form.locator('[name="note"]').fill('Formal pause')
                page.get_by_role('button',name='Save Status / Note',exact=True).click()
                self.assertIn('Formal pause',page.locator('#arrangement-history').inner_text())
                page.get_by_role('link',name='Back to Arrangements',exact=True).click()
                self.assertIn('billing_month=2026-10',page.url)
                self.assertEqual(page.locator('#arrangement-account').input_value(),'4')
                self.assertIn('BROWSER-ARR',page.locator('#arrangement-register').inner_text())
                self.assertEqual(page.get_by_role('link',name='Municipal Payment Arrangements',exact=True).count(),1)
            finally:browser.close()
        self.assertEqual(self.financial_snapshot(),before)
    def test_concurrent_payments_and_duplicate_request_cannot_overpay(self):
        import re
        from threading import Barrier
        from concurrent.futures import ThreadPoolExecutor
        from werkzeug.exceptions import HTTPException
        self.create(original_amount='100',instalment_amount='100',instalments='1')
        prefix='public.pm_arr_test_'+uuid.uuid4().hex+'_'
        originals=['user','auth_approved_admin','bil_property','bil_muni_account','bil_muni_payment']+list(self.service.TABLES)
        tables={name:prefix+'t'+str(i) for i,name in enumerate(originals)}
        for original,target in tables.items():
            self.assertRegex(target,r'^public\.pm_arr_test_[a-f0-9]{32}_t[0-9]+$')
            self.conn.execute(text('CREATE TABLE '+target+' (LIKE '+('\"user\"' if original=='user' else original)+' INCLUDING CONSTRAINTS INCLUDING INDEXES)'))
            self.conn.execute(text('INSERT INTO '+target+' SELECT * FROM '+('\"user\"' if original=='user' else original)))
            if original in self.service.TABLES:
                self.conn.execute(text('ALTER TABLE '+target+' ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (START WITH 1000)'))
                for column in ('recorded_at',) if original!='bil_municipal_arrangement' else ('created_at','modified_at'):
                    self.conn.execute(text('ALTER TABLE '+target+' ALTER COLUMN '+column+' SET DEFAULT now()'))
        self.conn.commit()
        def cleanup():
            self.session.rollback();self.conn.rollback()
            for target in reversed(list(tables.values())):self.conn.execute(text('DROP TABLE '+target))
            self.conn.commit()
        self.addCleanup(cleanup)
        source=ast.parse((ROOT/'app/program_billing/municipal_arrangements.py').read_text())
        source.body=[n for n in source.body if not (isinstance(n,ast.ImportFrom) and ((n.module or '').startswith('app.') or n.level))]
        pattern=re.compile(r'\b('+'|'.join(tables)+r')\b')
        def run(forms):
            barrier=Barrier(len(forms))
            def save(values):
                with self.conn.engine.connect() as connection:
                    with Session(connection) as unit:
                        module=types.ModuleType('concurrent_arrangement')
                        module.__dict__.update(db=types.SimpleNamespace(session=unit),is_admin=lambda:True)
                        exec(compile(source,'municipal_arrangements.py','exec'),module.__dict__)
                        module.text=lambda sql:text(pattern.sub(lambda match:tables[match.group(0)],sql.replace(chr(34)+'user'+chr(34),'user')))
                        module.TABLES=tuple(tables[name] for name in self.service.TABLES)
                        module.current_user=types.SimpleNamespace(id=7,is_authenticated=True)
                        module.property_scope=fixture.function('app/program_billing/metsoa_setup_safety.py','property_scope',dict(db=module.db,current_user=module.current_user,text=module.text,abort=abort))
                        module.today=lambda:date(2026,11,2)
                        barrier.wait(timeout=10)
                        try:
                            module.record_payment(26,1,values);unit.commit();connection.commit();return 201
                        except HTTPException as exc:unit.rollback();connection.rollback();return exc.code
            with ThreadPoolExecutor(max_workers=len(forms)) as pool:return list(pool.map(save,forms))
        self.assertCountEqual(run([self.payment(amount='70'),self.payment(amount='70')]),[201,400])
        repeated=self.payment(amount='30')
        self.assertEqual(run([repeated,repeated]),[201,201])
        self.assertEqual(self.conn.execute(text('SELECT sum(amount) FROM '+tables['bil_municipal_arrangement_payment'])).scalar(),Decimal('100'))
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM '+tables['bil_municipal_arrangement_payment'])).scalar(),2)
    def test_utilities_landing_menu_matches_access_and_preserves_property_month(self):
        hub=self.targets['billing_bp.utilities_hub']
        # Portfolio authority works independently of role labels.
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='user'"));self.conn.commit()
        page=self.client.get(hub+'?billing_month=2026-10');self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//button[@name="action"][@value="arrangements"]/text()'),['Municipal Payment Arrangements'])
        token=tree.xpath('//input[@name="csrf_token"]/@value')[0]
        response=self.client.post(hub,data=dict(csrf_token=token,action='arrangements',property_id='26',billing_month='2026-10'))
        self.assertEqual(response.status_code,302,response.text)
        self.assertIn(self.url,response.location);self.assertIn('billing_month=2026-10',response.location)
        self.assertEqual(self.client.get(response.location).status_code,200)
        page=self.client.get(hub+'?property_id=26&billing_month=2026-10')
        self.assertEqual(page.status_code,200,page.text)
        link=html.fromstring(page.data).xpath('//nav[@aria-label="Municipal Utilities"]/a[text()="Municipal Payment Arrangements"]')
        self.assertEqual(len(link),1)
        self.assertIn(self.url,link[0].get('href'));self.assertIn('billing_month=2026-10',link[0].get('href'))
        self.assertEqual(self.client.get(link[0].get('href')).status_code,200)
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"))
        self.conn.execute(text('UPDATE auth_approved_admin SET active=false'))
        self.conn.execute(text('UPDATE "user" SET is_active=0 WHERE id=7'));self.conn.commit()
        self.assertNotIn('Municipal Payment Arrangements',self.client.get(hub).text)
        response=self.client.post(hub,data=dict(csrf_token=token,action='arrangements',property_id='26',billing_month='2026-10'))
        self.assertEqual(response.status_code,403)
        self.assertEqual(self.client.get(self.url).status_code,403)
    def test_municipal_bill_summary_is_separate_from_payment_evidence(self):
        target=self.create()
        before=self.financial_snapshot()
        evidence={t:list(self.conn.execute(text('SELECT * FROM '+t+' ORDER BY id'))) for t in self.service.TABLES}
        self.conn.execute(text("UPDATE bil_muni_account SET account_number='83187242565',ca_contract_number='CA0815202400000098',ca_agreement_amount=12677.56,ca_installments_raised=6890,ca_installment_amount=530,ca_amount_owing=5787.56,ca_remaining_periods=11 WHERE id=1"))
        self.conn.commit()
        for url in (self.url,target):
            page=self.client.get(url);self.assertEqual(page.status_code,200)
            tree=html.fromstring(page.data)
            section=tree.xpath('//section[@aria-label="Credit Authority Agreement"]')[0]
            fields={d.xpath('./dt/text()')[0]:d.xpath('./dd/text()')[0] for d in section.xpath('.//dl/div')}
            self.assertEqual(fields,{'Contract Number':'CA0815202400000098','Agreement Amount':'R 12,677.56','Instalments Raised':'R 6,890.00','Current Instalment':'R 530.00','Amount Owing':'R 5,787.56','Remaining Periods':'11 months'})
            self.assertIn('83187242565',section.text_content())
            self.assertNotIn('570.74',section.text_content())
        self.assertEqual(self.financial_snapshot(),before)
        self.assertEqual(evidence,{t:list(self.conn.execute(text('SELECT * FROM '+t+' ORDER BY id'))) for t in self.service.TABLES})
        self.conn.execute(text("UPDATE bil_muni_account SET ca_contract_number='PRIVATE-CONTRACT' WHERE id=3"));self.conn.commit()
        self.assertNotIn('PRIVATE-CONTRACT',self.client.get(self.url).text)

    def test_bill_almond_production_navigation_authority(self):
        from flask import Blueprint,render_template
        from jinja2 import ChoiceLoader,FileSystemLoader
        # Production: Bill (644), active user, owns Almond (26), no active grant.
        self.conn.execute(text("UPDATE pm_admin_test_identity SET id=644,name='Bill',role='user' WHERE id=7"))
        self.conn.execute(text('UPDATE "user" SET id=644 WHERE id=7'))
        self.conn.execute(text('UPDATE bil_property SET manager_id=644 WHERE manager_id=7'))
        self.conn.execute(text('ALTER TABLE auth_approved_admin ALTER COLUMN active TYPE integer USING active::integer'))
        # No admin registry is needed for this ordinary portfolio function.
        self.conn.execute(text('DROP TABLE auth_approved_admin'));self.conn.commit()
        with self.client.session_transaction() as state:
            state['_user_id']='644'
            state['role']='admin';state['is_admin']=True
        # Match production blueprint-scoped context, with the real Utilities template.
        self.app.template_context_processors[None]=[f for f in self.app.template_context_processors[None] if f.__name__!='municipal_arrangement_navigation']
        blueprint=Blueprint('billing_bp',__name__)
        blueprint.context_processor(fixture.function('app/program_billing/routes.py','municipal_arrangement_navigation',dict(__package__='app.program_billing')))
        self.app.register_blueprint(blueprint)
        self.app.jinja_loader=ChoiceLoader([self.app.jinja_loader,FileSystemLoader(str(ROOT/'templates'))])
        self.app.view_functions['billing_bp.utilities_hub']=fixture.function('app/program_billing/routes.py','utilities_hub',dict(__package__='app.program_billing',request=request,current_user=current_user,abort=abort,flash=__import__('flask').flash,redirect=__import__('flask').redirect,url_for=__import__('flask').url_for,render_template=render_template))
        url=self.targets['billing_bp.utilities_hub']+'?property_id=26'
        page=self.client.get(url);self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertIn('Almond',page.text)
        self.assertEqual(len(tree.xpath('//nav[@aria-label="Municipal Utilities"]/a')),7)
        links=tree.xpath('//nav[@aria-label="Municipal Utilities"]/a')
        self.assertEqual(links[-1].text,'Municipal Payment Arrangements')
        self.assertEqual(self.client.get(links[-1].get('href')).status_code,200)
        before=self.financial_snapshot()
        target=self.create()
        self.assertEqual(self.post(target,self.payment(amount='50')).status_code,302)
        self.assertIn('R 200.00',self.client.get(target).text)
        self.assertEqual(self.conn.execute(text('SELECT recorded_by FROM bil_municipal_arrangement_payment')).scalar(),644)
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM bil_municipal_arrangement_event WHERE actor_id=644')).scalar(),2)
        self.assertEqual(self.financial_snapshot(),before)
        self.assertEqual(self.client.get(self.url.replace('/26/','/28/')).status_code,403)
        token=html.fromstring(self.client.get(target).data).xpath('//input[@name="csrf_token"]/@value')[0]
        # Neither forged role labels nor owning another property grants target access.
        self.conn.execute(text('UPDATE bil_property SET manager_id=8 WHERE id=26'));self.conn.commit()
        self.assertEqual(self.client.get(url).status_code,403)
        self.assertEqual(self.client.get(target).status_code,403)
        self.assertEqual(self.client.post(target,data=dict(self.payment(),csrf_token=token)).status_code,403)
        self.conn.execute(text('UPDATE bil_property SET manager_id=8 WHERE id=27'));self.conn.commit()
        self.assertNotIn('Municipal Payment Arrangements',self.client.get(self.targets['billing_bp.utilities_hub']).text)
        self.assertEqual(self.client.get(target).status_code,403)
        self.conn.execute(text('UPDATE bil_property SET manager_id=644 WHERE id=26'));self.conn.commit()
        self.assertEqual(self.client.get(target).status_code,200)
        self.conn.execute(text('UPDATE "user" SET is_active=0 WHERE id=644'));self.conn.commit()
        self.assertNotIn('Municipal Payment Arrangements',self.client.get(url).text)
        self.assertEqual(self.client.get(self.url).status_code,403)

    def test_account_selector_filters_and_carries_account_into_creation(self):
        from urllib.parse import urlsplit,parse_qs
        self.conn.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number) VALUES(4,26,'SECOND-26')"));self.conn.commit()
        first=self.create(reference='FIRST-ACCOUNT')
        second=self.create(account_id='4',reference='SECOND-ACCOUNT')
        before=self.financial_snapshot()
        evidence={t:list(self.conn.execute(text('SELECT * FROM '+t+' ORDER BY id'))) for t in self.service.TABLES}
        page=self.client.get(self.url+'?account_id=4&billing_month=2026-10')
        self.assertEqual(page.status_code,200)
        tree=html.fromstring(page.data)
        self.assertEqual(tree.xpath('//select[@id="arrangement-account"]/option/@value'),['1','4'])
        self.assertEqual(tree.xpath('//select[@id="arrangement-account"]/option[@selected]/@value'),['4'])
        register=tree.xpath('//table[@id="arrangement-register"]')[0].text_content()
        self.assertIn('SECOND-ACCOUNT',register);self.assertNotIn('FIRST-ACCOUNT',register)
        create_url=tree.xpath('//a[text()="+ Create Payment Arrangement"]/@href')[0]
        self.assertEqual(parse_qs(urlsplit(create_url).query)['account_id'],['4'])
        form=self.client.get(create_url)
        form_tree=html.fromstring(form.data)
        self.assertEqual(form_tree.xpath('//form[@id="arrangement-create"]/input[@name="account_id"]/@value'),['4'])
        token=form_tree.xpath('//input[@name="csrf_token"]/@value')[0]
        result=self.client.post(create_url,data=dict(self.form(account_id='4',reference='SELECTED-CREATE'),csrf_token=token))
        self.assertEqual(result.status_code,302)
        self.assertEqual(self.conn.execute(text("SELECT account_id FROM bil_municipal_arrangement WHERE reference='SELECTED-CREATE'")).scalar(),4)
        back=html.fromstring(self.client.get(result.location).data).xpath('//a[text()="Back to Arrangements"]/@href')[0]
        self.assertEqual(parse_qs(urlsplit(back).query)['account_id'],['4'])
        self.assertEqual(parse_qs(urlsplit(back).query)['billing_month'],['2026-10'])
        self.assertEqual(self.client.post(create_url,data=dict(self.form(account_id='1'),csrf_token=token)).status_code,400)
        for account_id in ('2','3','999'):
            self.assertEqual(self.client.get(self.url+'?account_id='+account_id).status_code,403)
            self.assertEqual(self.client.post(self.url,data=dict(self.form(account_id=account_id),csrf_token=token)).status_code,403)
        self.assertEqual(self.client.get(self.url+'?account_id=1&account_id=4').status_code,400)
        self.assertEqual(self.financial_snapshot(),before)
        for t,rows in evidence.items():
            self.assertEqual(list(self.conn.execute(text('SELECT * FROM '+t+' ORDER BY id LIMIT :n'),dict(n=len(rows)))),rows)

    def test_simplified_form_defaults_and_historical_terms_preserved(self):
        before=self.financial_snapshot()
        page=self.client.get(self.url+'?new=1&account_id=1&billing_month=2026-10')
        tree=html.fromstring(page.data)
        removed=('first_payment_date','document_reference','frequency')
        for field in removed:self.assertFalse(tree.xpath('//*[@name="'+field+'"]'))
        data=self.form(reference='SIMPLIFIED')
        for field in removed:data.pop(field)
        result=self.post(self.url+'?account_id=1&billing_month=2026-10',data)
        self.assertEqual(result.status_code,302,result.text)
        row=dict(self.conn.execute(text("SELECT * FROM bil_municipal_arrangement WHERE reference='SIMPLIFIED'")).mappings().one())
        self.assertEqual(row['first_payment_date'],row['agreement_date'])
        self.assertEqual(row['frequency'],'monthly')
        self.assertEqual(self.post(result.location,self.payment(amount='50')).status_code,302)
        self.assertIn('R 200.00',self.client.get(result.location).text)
        historical=self.create(reference='HISTORICAL',frequency='quarterly',first_payment_date='2026-09-30',document_reference='Historical Letter')
        old=dict(self.conn.execute(text("SELECT * FROM bil_municipal_arrangement WHERE reference='HISTORICAL'")).mappings().one())
        schedule=self.service.schedule(old)
        history=list(self.conn.execute(text('SELECT * FROM bil_municipal_arrangement_event ORDER BY id')))
        payments=list(self.conn.execute(text('SELECT * FROM bil_municipal_arrangement_payment ORDER BY id')))
        status=dict(action='status',request_key=str(uuid.uuid4()),version='1',status='on_hold',note='Preserve historical terms')
        self.assertEqual(self.post(historical,status).status_code,302)
        saved=dict(self.conn.execute(text('SELECT * FROM bil_municipal_arrangement WHERE id=:id'),dict(id=old['id'])).mappings().one())
        for field in removed:self.assertEqual(saved[field],old[field])
        self.assertEqual(self.service.schedule(saved),schedule)
        detail=html.fromstring(self.client.get(historical).data)
        for field in removed:self.assertFalse(detail.xpath('//*[@name="'+field+'"]'))
        self.assertFalse(detail.xpath('//dt[text()="Payment frequency" or text()="First payment date" or text()="Final payment date"]'))
        self.assertEqual(list(self.conn.execute(text('SELECT * FROM bil_municipal_arrangement_event ORDER BY id LIMIT :n'),dict(n=len(history)))),history)
        self.assertEqual(list(self.conn.execute(text('SELECT * FROM bil_municipal_arrangement_payment ORDER BY id'))),payments)
        self.assertEqual(self.financial_snapshot(),before)

    def test_missing_migration_safe_unavailable(self):
        token=html.fromstring(self.client.get(self.url+'?new=1').data).xpath('//input[@name="csrf_token"]/@value')[0]
        for name in reversed(self.service.TABLES):self.conn.execute(text('DROP TABLE '+name))
        self.conn.commit();response=self.client.get(self.url)
        self.assertEqual(response.status_code,200);self.assertIn('temporarily unavailable',response.text)
        response=self.client.post(self.url,data=dict(self.form(),csrf_token=token))
        self.assertEqual(response.status_code,503)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ArrangementTests(name) for name in ArrangementTests.__dict__ if name.startswith('test_'))
