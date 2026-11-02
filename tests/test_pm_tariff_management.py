"""Scoped authenticated tariff UI; isolated PostgreSQL temporary tables only."""
import unittest,ast,types,uuid,re,importlib.util
from unittest.mock import patch
from alembic.operations import Operations
from alembic.migration import MigrationContext
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from sqlalchemy.orm import Session
from test_pm_admin_landing import ROOT
from datetime import date
from sqlalchemy import text,event
from lxml import html
from test_pm_admin_landing import function,declared_route
from test_pm_admin_template_paths import AuthenticatedTemplateTests

class ManagementTests(AuthenticatedTemplateTests):
    def setUp(self):
        super().setUp()
        self.tariff_service.today=lambda:date(2026,11,2)
        self.conn.execute(text('SET search_path TO pg_temp,public'))
        self.conn.execute(text('ALTER TABLE bil_tariff ADD PRIMARY KEY(id)'))
        self.conn.execute(text('CREATE TEMP TABLE "user" (id integer PRIMARY KEY)'))
        self.conn.execute(text('INSERT INTO "user" VALUES(7)'));self.conn.commit()
        spec=importlib.util.spec_from_file_location('tariff_authority_migration',ROOT/'migrations/versions/pm_tariff_authority_001.py')
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        before=self.snapshot()
        with patch.object(migration,'op',Operations(MigrationContext.configure(self.conn))):migration.upgrade()
        self.conn.commit();self.assertEqual(before,self.snapshot())
        spec=importlib.util.spec_from_file_location('tariff_optional_migration',ROOT/'migrations/versions/pm_tariff_authority_002.py')
        optional=importlib.util.module_from_spec(spec);spec.loader.exec_module(optional)
        with patch.object(optional,'op',Operations(MigrationContext.configure(self.conn))):optional.upgrade()
        self.conn.commit()
        self.assertEqual(self.conn.execute(text("SELECT relpersistence FROM pg_class WHERE oid='bil_tariff_verification'::regclass")).scalar(),'t')
        self.conn.commit()
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
        data=dict(dict(municipality='Verified Test Municipality',municipal_category='Official test category',source_reference='Synthetic test municipal bill reference'),**data)
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
        self.assertEqual(rows[-1]['code'],'W01')
        self.assertEqual(self.post(path,dict(data,code='w-new')).status_code,302)
        self.assertEqual(self.snapshot()['bil_tariff'][-1]['code'],'W02')
    def test_add_invalid_dates_numbers_tiers_units_and_csrf(self):
        path='/admin/billing/tariffs/new'
        base=dict(utility_type='water',code='W-NEW',rate='25',effective_date='2026-12-01',block_start='0',block_end='200',reduction_factor='1',unit='kL')
        before=self.snapshot()
        for delta in (dict(effective_date='2026-02-30'),dict(rate='NaN'),dict(rate='-1'),dict(rate='1.12345'),dict(block_end='0'),dict(unit='kWh'),dict(reduction_factor='0')):
            response=self.post(path,dict(base,**delta));self.assertEqual(response.status_code,400,response.text)
            self.assertIn('role="alert"',response.text)
        self.assertEqual(self.client.post(path,data=base).status_code,400)
        self.assertEqual(self.snapshot(),before)
    def test_edit_appends_version_and_preserves_historical_metadata(self):
        self.session.execute(text("UPDATE bil_tariff SET reduction_factor=999,block_start=1501,block_end=99999,unit=NULL WHERE id=1"));self.session.commit();self.conn.commit()
        before=self.snapshot()['bil_tariff'][0]
        path='/admin/billing/tariffs/1/edit'
        values=dict(base_id='1',rate='4.1234',description='New description',effective_date='2026-12-01',block_start='0',reduction_factor='1',code='HACK')
        response=self.post(path,values);self.assertEqual(response.status_code,302,response.text)
        rows=self.snapshot()['bil_tariff'];self.assertEqual(rows[0],before)
        for key in ('code','utility_type','block_start','block_end','reduction_factor','unit'):
            self.assertEqual(rows[1][key],before[key])
        self.assertEqual(rows[1]['rate'],4.1234)
        selected,_=self.tariff_service.resolve(rows,date(2026,10,31));self.assertEqual(selected[0]['rate'],before['rate'])
        selected,_=self.tariff_service.resolve(rows,date(2026,12,1));self.assertEqual(selected[0]['rate'],4.1234)
        self.assertEqual(self.post(path,values).status_code,400)
        self.assertEqual(len(self.snapshot()['bil_tariff']),2)
    def test_edit_rejects_preceding_duplicate_and_ambiguous_records(self):
        path='/admin/billing/tariffs/1/edit';before=self.snapshot()
        for day in ('2025-12-31','2026-01-01','bad'):
            response=self.post(path,dict(base_id='1',rate='5',effective_date=day,description='Entered description'))
            self.assertEqual(response.status_code,400);self.assertIn('Entered description',response.text)
        self.assertEqual(before,self.snapshot())
        self.session.execute(text("INSERT INTO bil_tariff(id,utility_type,code,rate,effective_date) VALUES(2,'electricity','TEST-E',8,'2026-01-01')"));self.session.commit();self.conn.commit()
        response=self.post(path,dict(base_id='2',rate='9',effective_date='2026-12-01'))
        self.assertEqual(response.status_code,400);self.assertIn('conflicting records',response.text)
    def test_old_overwrite_delete_endpoint_is_disabled(self):
        before=self.snapshot()
        with self.client.session_transaction() as state:
            from flask_wtf.csrf import generate_csrf
        token=html.fromstring(self.client.get('/admin/billing/tariffs/new').data).xpath('//input[@name="csrf_token"]/@value')[0]
        for action in ('edit','delete','add'):
            response=self.client.post('/admin/billing/tariffs',data=dict(action=action,tariff_id='1',rate='99',csrf_token=token))
            self.assertEqual(response.status_code,405)
        self.assertEqual(before,self.snapshot())
    def test_browser_layout_add_edit_history_and_conditional_fields(self):
        from playwright.sync_api import sync_playwright
        from http.server import BaseHTTPRequestHandler,HTTPServer
        from threading import Thread
        case=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.respond(case.client.get(self.path))
            def do_POST(self):
                payload=self.rfile.read(int(self.headers['Content-Length']))
                response=case.client.post(self.path,data=payload,content_type=self.headers.get('Content-Type'))
                case.conn.commit();self.respond(response)
            def respond(self,response):
                self.send_response(response.status_code)
                self.send_header('Content-Type',response.content_type)
                if response.location:self.send_header('Location',response.location)
                self.end_headers();self.wfile.write(response.data)
        server=HTTPServer(('127.0.0.1',0),Handler);thread=Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(timeout=2)))
        origin='http://127.0.0.1:'+str(server.server_port)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(viewport={'width':1280,'height':900})
                page.goto(origin+'/admin/billing/tariffs')
                self.assertTrue(page.evaluate('document.documentElement.scrollWidth <= innerWidth'))
                edit=page.get_by_role('link',name='Edit',exact=True).bounding_box()
                history=page.get_by_role('link',name='View History',exact=True).bounding_box()
                self.assertLess(edit['y'],history['y']);self.assertAlmostEqual(edit['x']+edit['width'],history['x']+history['width'],delta=1)
                page.get_by_role('link',name='+ Add New Tariff / Charge',exact=True).click()
                self.assertFalse(page.locator('input[name="block_start"]').is_visible())
                page.locator('#charge-type').select_option('water');self.assertTrue(page.locator('input[name="block_start"]').is_visible())
                page.locator('#charge-type').select_option('electricity')
                for name,value in dict(description='Browser tariff',rate='2.5000',effective_date='2026-11-02').items():page.locator('[name="'+name+'"]').fill(value)
                for name in ('municipality','municipal_category','source_reference'):self.assertFalse(page.locator('[name="'+name+'"]').evaluate('(input)=>input.required'))
                page.locator('[name="unit"]').select_option('kWh');page.get_by_role('button',name='Save Tariff',exact=True).click()
                self.assertIn('Tariff saved successfully.',page.locator('.tariff-tile').inner_text())
                row=page.locator('tbody tr').filter(has_text='ELEC01');row.get_by_role('link',name='Edit',exact=True).click()
                self.assertEqual(page.locator('input[readonly]').count(),3)
                page.locator('[name="rate"]').fill('3.5000');page.locator('[name="effective_date"]').fill('2026-12-01')
                for name in ('municipality','municipal_category','source_reference'):self.assertFalse(page.locator('[name="'+name+'"]').evaluate('(input)=>input.required'))
                page.get_by_role('button',name='Save New Rate',exact=True).click()
                page.locator('tbody tr').filter(has_text='ELEC01').get_by_role('link',name='View History').click()
                self.assertIn('R2.5000',page.locator('table').inner_text());self.assertIn('R3.5000',page.locator('table').inner_text());self.assertIn('Scheduled',page.locator('table').inner_text())
                page.get_by_role('link',name='Back to Tariff Management',exact=True).click()
                page.set_viewport_size({'width':390,'height':844})
                self.assertTrue(page.evaluate('document.documentElement.scrollWidth <= innerWidth'))
            finally:browser.close()
    def test_concurrent_identity_and_version_creation_serializes(self):
        # A fresh disposable schema is visible to both independent PG sessions.
        table='public.test_tariff_'+uuid.uuid4().hex
        self.assertRegex(table,r'^public\.test_tariff_[a-f0-9]{32}$')
        self.conn.execute(text('CREATE TABLE '+table+' (LIKE public.bil_tariff INCLUDING CONSTRAINTS)'))
        self.conn.execute(text('ALTER TABLE '+table+' ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY'))
        verification_table=table+'_verification'
        self.conn.execute(text('CREATE TABLE '+verification_table+' (LIKE bil_tariff_verification INCLUDING ALL)'))
        self.conn.commit()
        def cleanup():
            self.conn.rollback();self.conn.execute(text('DROP TABLE '+verification_table));self.conn.execute(text('DROP TABLE '+table));self.conn.commit()
        self.addCleanup(cleanup)
        source=ast.parse((ROOT/'app/admin/programs/billing/tariff_management.py').read_text())
        source.body=[n for n in source.body if not (isinstance(n,ast.ImportFrom) and (n.module or '').startswith('app.'))]
        def run(mode):
            barrier=Barrier(2)
            def save(_):
                with self.conn.engine.connect() as connection:
                    with Session(connection) as session:
                        module=types.ModuleType('concurrent_tariff')
                        module.__dict__.update(db=types.SimpleNamespace(session=session),identity=self.tariff_service.identity,resolve=self.tariff_service.resolve,is_admin=lambda:True)
                        exec(compile(source,'tariff_management.py','exec'),module.__dict__);module.today=lambda:date(2026,11,2)
                        module.text=lambda sql:text(re.sub(r'\bbil_tariff\b',table,re.sub(r'\bbil_tariff_verification\b',verification_table,sql)))
                        module.current_user=types.SimpleNamespace(id=7,is_authenticated=True)
                        barrier.wait(timeout=10)
                        try:
                            evidence=dict(municipality='Synthetic municipality',municipal_category='Test category',source_reference='Test source')
                            original_new=module.save_new;original_rate=module.save_rate
                            module.save_new=lambda values:original_new(dict(values,**evidence))
                            module.save_rate=lambda identifier,values:original_rate(identifier,dict(values,**evidence))
                            if mode=='new':module.save_new(dict(utility_type='electricity',code='CONCURRENT',rate='2',effective_date='2026-01-01',unit='kWh'))
                            else:module.save_rate(1,dict(base_id='1',rate='3',effective_date='2026-07-01',description='Version'))
                            connection.commit();return 'saved'
                        except ValueError:
                            session.rollback();connection.rollback();return 'rejected'
            with ThreadPoolExecutor(max_workers=2) as pool:return list(pool.map(save,range(2)))
        self.assertCountEqual(run('new'),['saved','saved'])
        self.assertCountEqual(run('edit'),['saved','rejected'])
        self.assertEqual(self.conn.execute(text('SELECT count(*) FROM '+table)).scalar(),3)
        self.assertEqual([r[0] for r in self.conn.execute(text('SELECT DISTINCT code FROM '+table+' ORDER BY code'))],['ELEC01','ELEC02'])
    def test_unknown_history_dates_are_not_invented_or_current(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='' WHERE id=1"));self.session.commit();self.conn.commit()
        response=self.client.get('/admin/billing/tariffs/1/history')
        self.assertIn('Unknown',response.text);self.assertNotIn('>Current<',response.text)
        self.assertNotIn('2026-01-01',response.text)
    def test_thirteen_original_identities_dates_rates_and_factors_remain_intact(self):
        rows=[('electricity','ELEC01',3.2795,0,0,None,'2026-01-01')]
        for code,rate,lo,hi,factor in [('W01',39.39,0,200,.2),('W02',46.70,201,833,.633),('W03',62.17,834,1000,.167),('W04',95.91,1001,1500,.5),('W05',105.39,1501,99999,999)]:
            rows.append(('water',code,rate,lo,hi,factor,'2026-06-05'))
        for code,rate,lo,hi,factor in [('S01',5.45,0,200,.2),('S02',9.20,201,833,.633),('S03',17.54,834,1000,.167),('S04',27.38,1001,1500,999)]:
            rows.append(('sewerage',code,rate,lo,hi,factor,'2026-06-05'))
        rows.extend([('water_fixed','W_LOSS',19.30,0,0,1,'2026-06-05'),('water_fixed','W_MGT',144.54,0,0,1,'2026-06-05'),('sewerage_fixed','S_REFUSE',206.93,0,0,1,'2026-06-05')])
        self.session.execute(text('DELETE FROM bil_tariff'))
        for id,(utility,code,rate,lo,hi,factor,start) in enumerate(rows,1):
            self.session.execute(text('INSERT INTO bil_tariff(id,utility_type,code,rate,block_start,block_end,reduction_factor,effective_date) VALUES(:id,:u,:c,:r,:lo,:hi,:f,:d)'),dict(id=id,u=utility,c=code,r=rate,lo=lo,hi=hi,f=factor,d=start))
        self.session.commit();self.conn.commit();before=self.snapshot()['bil_tariff']
        self.assertEqual(len(html.fromstring(self.client.get('/admin/billing/tariffs').data).xpath('//tbody/tr')),13)
        response=self.post('/admin/billing/tariffs/6/edit',dict(base_id='6',rate='110',effective_date='2026-12-01',description='New W05 price'))
        self.assertEqual(response.status_code,302,response.text)
        after=self.snapshot()['bil_tariff'];self.assertEqual(after[:13],before)
        self.assertEqual(after[-1]['reduction_factor'],999)
        selected,issues=self.tariff_service.resolve(after,date(2026,10,31));self.assertFalse(issues)
        self.assertEqual(len(selected),13);self.assertEqual(next(r['rate'] for r in selected if r['code']=='W05'),105.39)
    def test_code_previews_ignore_versions_and_preserve_named_fixed_charges(self):
        self.session.execute(text("INSERT INTO bil_tariff(id,utility_type,code,rate,effective_date) VALUES(2,'electricity','ELEC01',3,'2026-01-01'),(3,'electricity','ELEC01',4,'2026-12-01'),(4,'water','W05',1,'2026-06-05'),(5,'sewerage','S04',1,'2026-06-05'),(6,'water_fixed','W_LOSS',19.3,'2026-06-05')"))
        self.session.commit();self.conn.commit();before=self.snapshot()
        expected={'electricity':'ELEC02','water':'W06','sewerage':'S05','water_fixed':'WF01','sewerage_fixed':'SF01'}
        for utility,code in expected.items():self.assertEqual(self.tariff_service.next_code(utility),code)
        response=self.client.get('/admin/billing/tariffs/new')
        doc=html.fromstring(response.data);self.assertTrue(doc.xpath('//input[@name="code"]/@readonly'))
        self.assertEqual(self.snapshot(),before)
    def test_today_and_future_rates_save_and_history_uses_readable_open_end(self):
        original=self.snapshot()['bil_tariff'][0]
        blanks=dict(municipality='',municipal_category='',source_reference='')
        response=self.post('/admin/billing/tariffs/1/edit',dict(base_id='1',rate='4',effective_date='2026-11-02',**blanks))
        self.assertEqual(response.status_code,302,response.text)
        current=self.snapshot()['bil_tariff'][-1]
        response=self.post('/admin/billing/tariffs/1/edit',dict(base_id=str(current['id']),rate='5',effective_date='2026-12-01',**blanks))
        self.assertEqual(response.status_code,302,response.text)
        rows=self.snapshot()['bil_tariff'];self.assertEqual(rows[0],original)
        for when,expected in ((date(2026,10,31),original['rate']),(date(2026,11,2),4),(date(2026,12,1),5)):
            selected,issues=self.tariff_service.resolve(rows,when);self.assertFalse(issues);self.assertEqual(selected[0]['rate'],expected)
        history=html.fromstring(self.client.get('/admin/billing/tariffs/1/history').data)
        self.assertEqual(history.xpath('//tbody/tr/td[4]/text()'),['Previous','Current','Scheduled'])
        self.assertEqual(history.xpath('//tbody/tr[last()]/td[2]')[0].text_content(),chr(8212))
        self.assertEqual(history.xpath('//tbody/tr[2]/td[2]/text()'),['2026-11-30'])
    def test_past_effective_elec01_version_preserves_rows_and_only_writes_tariff_data(self):
        self.session.execute(text("UPDATE bil_tariff SET code='ELEC01' WHERE id=1"));self.session.commit();self.conn.commit()
        original=self.snapshot()['bil_tariff'][0]
        statements=[]
        def capture(conn,cursor,sql,params,context,many):statements.append(sql.strip().lower())
        event.listen(self.conn,'before_cursor_execute',capture)
        try:
            response=self.post('/admin/billing/tariffs/1/edit',dict(base_id='1',rate='3.5747',effective_date='2026-07-01',municipality='',municipal_category='',source_reference=''))
        finally:event.remove(self.conn,'before_cursor_execute',capture)
        self.assertEqual(response.status_code,302,response.text)
        rows=self.snapshot()['bil_tariff'];self.assertEqual(len(rows),2);self.assertEqual(rows[0],original)
        self.assertEqual(rows[1]['code'],'ELEC01')
        for billing,expected in ((date(2026,6,30),3.2795),(date(2026,7,1),3.5747),('2026-10',3.5747)):
            selected,issues=self.tariff_service.resolve(rows,billing);self.assertFalse(issues);self.assertEqual(selected[0]['rate'],expected)
        mutations=[sql for sql in statements if sql.startswith(('insert ','update ','delete '))]
        self.assertEqual(len(mutations),2)
        self.assertTrue(mutations[0].startswith('insert into bil_tariff ('))
        self.assertTrue(mutations[1].startswith('insert into bil_tariff_verification ('))
        self.assertFalse(any(name in sql for sql in statements for name in ('bil_tenant_ledger','bil_statement','bil_metsoa_meter_month','bil_metsoa_breakdown')))
        verification=self.conn.execute(text('SELECT * FROM bil_tariff_verification')).mappings().one()
        for key in ('municipality','municipal_category','source_reference'):self.assertIsNone(verification[key])
        self.assertIsNotNone(verification['recorded_at'])
        history=html.fromstring(self.client.get('/admin/billing/tariffs/1/history').data)
        self.assertEqual(history.xpath('//tbody/tr[1]/td[2]/text()'),['2026-06-30'])
        self.assertEqual(history.xpath('//tbody/tr/td[4]/text()'),['Previous','Current'])
        # Chronological ordering and stale-form rules still apply to past dates.
        before=self.snapshot()
        for day in ('2026-06-01','2026-07-01'):
            response=self.post('/admin/billing/tariffs/1/edit',dict(base_id=str(rows[1]['id']),rate='4',effective_date=day))
            self.assertEqual(response.status_code,400,response.text)
        self.assertEqual(self.post('/admin/billing/tariffs/1/edit',dict(base_id='1',rate='4',effective_date='2026-08-01')).status_code,400)
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
