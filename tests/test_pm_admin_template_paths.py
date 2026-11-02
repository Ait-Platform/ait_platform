"""Moved billing admin templates: tracked-file audit and read-only authenticated rendering."""
import ast,subprocess,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import text,Table,MetaData,event
from sqlalchemy.orm import registry,Session
from flask import request,redirect,url_for,render_template,flash
from flask_wtf.csrf import CSRFProtect,generate_csrf
from jinja2 import Environment,FileSystemLoader,meta
import test_pm_admin_landing as fixture
ROOT=fixture.ROOT
UNRESOLVED={'aadmin_bp/billing/readings_export.html','admin/billing/metsoa_water_config.html','billing/tenant/form.html'}
RENDERERS={'render_template','render_pm_template','_render_billing_template'}

def references():
    for file in (ROOT/'app/admin/programs/billing').rglob('*.py'):
        tree=ast.parse(file.read_text(encoding='utf-8-sig'))
        for node in ast.walk(tree):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id in RENDERERS and node.args and isinstance(node.args[0],ast.Constant) and isinstance(node.args[0].value,str):
                yield node.args[0].value

class TemplateAuditTests(unittest.TestCase):
    def test_all_existing_targets_and_dependencies_are_tracked_and_loadable(self):
        tracked=set(subprocess.check_output(['git','ls-files','--','templates'],cwd=ROOT,text=True).splitlines())
        env=Environment(loader=FileSystemLoader(ROOT/'templates'),extensions=['jinja2.ext.do','jinja2.ext.loopcontrols'])
        missing=set();seen=set()
        def check(name):
            if name in seen:return
            seen.add(name)
            if not (ROOT/'templates'/name).is_file():missing.add(name);return
            self.assertIn('templates/'+name,tracked)
            source=env.loader.get_source(env,name)[0];parsed=env.parse(source)
            env.get_template(name)
            for child in meta.find_referenced_templates(parsed):
                if child is not None:check(child)
        for name in references():check(name)
        self.assertEqual(missing,UNRESOLVED,'Unexpected missing template: do not silently substitute another page')
        self.assertGreaterEqual(len(seen-UNRESOLVED),32)
    def test_old_prefix_remains_only_for_documented_truly_absent_file(self):
        self.assertEqual({name for name in references() if name.startswith('admin/billing/')},{'admin/billing/metsoa_water_config.html'})

class AuthenticatedTemplateTests(fixture.AdminLandingTests):
    def setUp(self):
        super().setUp()
        self.session=Session(self.conn);self.addCleanup(self.session.close)
        self.conn.execute(text('CREATE TEMP TABLE bil_tariff (LIKE public.bil_tariff INCLUDING DEFAULTS INCLUDING CONSTRAINTS)'))
        self.conn.execute(text('CREATE TEMP TABLE bil_platform_settings (LIKE public.bil_platform_settings INCLUDING DEFAULTS INCLUDING CONSTRAINTS)'))
        self.conn.execute(text("INSERT INTO bil_tariff(id,utility_type,code,description,rate,block_start,block_end,reduction_factor,unit,effective_date) VALUES(1,'electricity','TEST-E','Isolated electricity tariff',3.2795,0,0,1,'kWh','2026-01-01')"))
        self.conn.execute(text('INSERT INTO bil_platform_settings(id,base_price_cents,included_meters,extra_meter_price_cents) VALUES(1,10000,2,1500)'));self.conn.commit()
        mapper=registry();models={}
        for name,table in [('BilTariff','bil_tariff'),('BilPlatformSettings','bil_platform_settings')]:
            cls=type(name,(),{});record=Table(table,MetaData(),autoload_with=self.conn)
            mapper.map_imperatively(cls,record,primary_key=[record.c.id]);cls.query=self.session.query(cls);models[name]=cls
        module=types.ModuleType('app.models.billing');module.__dict__.update(models)
        mocked=patch.dict(sys.modules,{'app.models.billing':module});mocked.start();self.addCleanup(mocked.stop)
        CSRFProtect(self.app);self.app.jinja_env.globals['csrf_token']=generate_csrf
        env=dict(render_template=render_template,request=request,redirect=redirect,url_for=url_for,flash=flash,db=types.SimpleNamespace(session=self.session),BilTariff=models['BilTariff'])
        env['_render_billing_template']=fixture.function('app/admin/programs/billing/routes.py','_render_billing_template',{})
        # Load only the scoped tariff module, never application startup/maintenance.
        from flask_login import current_user
        from flask import abort
        resolver=types.ModuleType('isolated_tariff_versions')
        exec(compile((ROOT/'app/utils/tariff_versions.py').read_text(), 'tariff_versions.py','exec'),resolver.__dict__)
        service=types.ModuleType('app.admin.programs.billing.tariff_management')
        service.__dict__.update(db=env['db'],is_admin=lambda:bool(current_user.is_admin),identity=resolver.identity,resolve=resolver.resolve)
        parsed=ast.parse((ROOT/'app/admin/programs/billing/tariff_management.py').read_text())
        parsed.body=[n for n in parsed.body if not (isinstance(n,ast.ImportFrom) and (n.module or '').startswith('app.'))]
        exec(compile(parsed,'tariff_management.py','exec'),service.__dict__)
        self.tariff_service=service
        mocked_service=patch.dict(sys.modules,{service.__name__:service});mocked_service.start();self.addCleanup(mocked_service.stop)
        # Register URLs from actual decorators for unchanged navigation, never execute unrelated workflows.
        tree=ast.parse((ROOT/'app/admin/programs/billing/routes.py').read_text(encoding='utf-8-sig'))
        for node in tree.body:
            if not isinstance(node,ast.FunctionDef):continue
            for d in node.decorator_list:
                if not isinstance(d,ast.Call) or not isinstance(d.func,ast.Attribute) or not isinstance(d.func.value,ast.Name) or d.func.value.id!='admin_bp' or d.func.attr not in ('route','get'):continue
                endpoint='admin_bp.'+next((ast.literal_eval(k.value) for k in d.keywords if k.arg=='endpoint'),node.name)
                if endpoint not in self.app.view_functions:self.app.add_url_rule('/admin'+ast.literal_eval(d.args[0]),endpoint=endpoint,view_func=lambda:('Unrelated route not exercised',501))
        self.pages={}
        for name in ('billing_tariffs','billing_units','billing_meters','billing_invoices','billing_reports','readings_dashboard','billing_data','units_new'):
            path,endpoint=fixture.declared_route('app/admin/programs/billing/routes.py',name,'admin_bp','/admin')
            self.app.view_functions[endpoint]=fixture.function('app/admin/programs/billing/routes.py',name,env)
            self.pages[name]=path
    def snapshot(self):
        return {table:[dict(r) for r in self.conn.execute(text('SELECT * FROM '+table+' ORDER BY id')).mappings()] for table in ('bil_tariff','bil_platform_settings')}
    def test_authenticated_affected_gets_render_without_financial_writes(self):
        self.sign_in();before=self.snapshot();statements=[]
        def record(conn,cursor,sql,parameters,context,many):statements.append(sql)
        event.listen(self.conn,'before_cursor_execute',record)
        try:
            for path in self.pages.values():
                response=self.client.get(path);self.assertEqual(response.status_code,200,path)
            tariffs=self.client.get(self.pages['billing_tariffs']);self.assertIn('TEST-E',tariffs.text);self.assertIn('3.2795',tariffs.text)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertTrue(all(sql.lstrip().upper().startswith('SELECT') for sql in statements))
        self.assertEqual(before,self.snapshot())
    def test_anonymous_affected_routes_still_redirect(self):
        for path in self.pages.values():
            response=self.client.get(path);self.assertEqual(response.status_code,302,path);self.assertEqual(response.location,'/')
    def test_authenticated_browser_navigation_to_affected_pages(self):
        from playwright.sync_api import sync_playwright
        from http.server import BaseHTTPRequestHandler,HTTPServer
        from threading import Thread
        self.sign_in();before=self.snapshot();case=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):
                response=case.client.get(self.path);self.send_response(response.status_code)
                self.send_header('Content-Type',response.content_type)
                if response.location:self.send_header('Location',response.location)
                self.send_header('Content-Length',str(len(response.data)));self.end_headers();self.wfile.write(response.data)
        server=HTTPServer(('127.0.0.1',0),Handler);worker=Thread(target=server.serve_forever,daemon=True);worker.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),worker.join(timeout=2)))
        origin='http://127.0.0.1:'+str(server.server_port)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page()
                expected={'billing_tariffs':'Tariff Management','billing_units':'Units Management','billing_meters':'Meters Management','billing_invoices':'Invoices Management','billing_reports':'Reports Management','readings_dashboard':'Billing Administration'}
                for name,heading in expected.items():
                    response=tab.goto(origin+self.pages[name]);self.assertEqual(response.status,200)
                    self.assertEqual(tab.locator('h1').inner_text(),heading)
                    if name=='billing_tariffs':
                        self.assertEqual(tab.get_by_role('link',name='+ Add New Tariff / Charge',exact=True).count(),1)
                        self.assertIn('TEST-E',tab.locator('table').inner_text())
                    if name!='readings_dashboard':
                        tab.get_by_role('link',name='Back',exact=False).first.click()
                        self.assertEqual(tab.locator('h1').inner_text(),'Billing Administration')
                    else:
                        self.assertEqual(tab.get_by_role('link',name='Back to Bridge',exact=True).get_attribute('href'),self.targets['bridge_bp.bridge'])
            finally:browser.close()
        self.assertEqual(before,self.snapshot())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(cls(name) for cls in (TemplateAuditTests,AuthenticatedTemplateTests) for name in cls.__dict__ if name.startswith('test_'))
