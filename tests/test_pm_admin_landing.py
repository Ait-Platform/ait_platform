"""Original PM admin landing: local PostgreSQL identity, real guard/view/template; no factory."""
import ast,os,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
from dotenv import dotenv_values
from sqlalchemy import create_engine,text,event
from sqlalchemy.engine import make_url
from flask import Flask,Blueprint,request,redirect,url_for,session,current_app,render_template,abort
from flask_login import LoginManager,UserMixin,current_user
from jinja2 import ChoiceLoader,DictLoader,FileSystemLoader
from lxml import html
ROOT=Path(__file__).resolve().parents[1]

def function(file,name,env):
    tree=ast.parse((ROOT/file).read_text(encoding='utf-8-sig'))
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
    node.decorator_list=[]
    exec(compile(ast.Module(body=[node],type_ignores=[]),file,'exec'),env)
    return env[name]

def declared_route(file,name,blueprint,prefix=''):
    tree=ast.parse((ROOT/file).read_text(encoding='utf-8-sig'))
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and (n.name==name or any(isinstance(d,ast.Call) and any(k.arg=='endpoint' and isinstance(k.value,ast.Constant) and k.value.value==name for k in d.keywords) for d in n.decorator_list)))
    for d in node.decorator_list:
        if isinstance(d,ast.Call) and isinstance(d.func,ast.Attribute) and isinstance(d.func.value,ast.Name) and d.func.value.id==blueprint and d.func.attr in ('route','get'):
            endpoint=next((ast.literal_eval(k.value) for k in d.keywords if k.arg=='endpoint'),name)
            return prefix+ast.literal_eval(d.args[0]),blueprint+'.'+endpoint
    raise AssertionError('Missing existing route: '+name)

class AdminLandingTests(unittest.TestCase):
    def setUp(self):
        raw=os.environ.get('PM_LEASE_TEST_DATABASE_URL') or dotenv_values(ROOT.parents[1]/'.env').get('DATABASE_URL','')
        if not raw:self.skipTest('Verified local PostgreSQL required')
        target=make_url(raw.replace('postgres://','postgresql://',1));self.assertEqual(target.database,'ait_local_db');self.assertIn(target.host,('localhost','127.0.0.1','::1'))
        engine=create_engine(target,connect_args={'connect_timeout':5});self.addCleanup(engine.dispose)
        self.conn=engine.connect();self.addCleanup(self.conn.close)
        self.assertEqual(self.conn.execute(text('SELECT current_database()')).scalar(),'ait_local_db')
        self.conn.execute(text('CREATE TEMP TABLE pm_admin_test_identity(id integer PRIMARY KEY,name text,role text)'))
        self.conn.execute(text("INSERT INTO pm_admin_test_identity VALUES(7,'Synthetic Administrator','admin')"));self.conn.commit()
        app=Flask('isolated-pm-admin-landing',root_path=str(ROOT/'app'));app.secret_key='isolated-test-only';app.config['TESTING']=True
        # The root PM helper must find the tracked template even with an app/templates-only loader.
        app.jinja_loader=DictLoader({'layout.html':'{% block content %}{% endblock %}'})
        self.app=app;manager=LoginManager(app);conn=self.conn
        class Identity(UserMixin):
            def __init__(self,row):self.id=row['id'];self.name=row['name'];self.role=row['role'];self.is_admin=row['role']=='admin'
        @manager.user_loader
        def load_user(identifier):
            row=conn.execute(text('SELECT * FROM pm_admin_test_identity WHERE id=:id'),dict(id=int(identifier))).mappings().first()
            return Identity(row) if row else None
        env=dict(current_user=current_user,session=session,request=request,redirect=redirect,url_for=url_for)
        env['is_admin']=function('app/utils/roles.py','is_admin',env)
        guard=function('app/admin/__init__.py','_guard',env)
        helper=function('app/program_billing/lease_management.py','render_pm_template',dict(Path=Path,current_app=current_app,ChoiceLoader=ChoiceLoader,FileSystemLoader=FileSystemLoader,render_template=render_template))
        module=types.ModuleType('app.program_billing.lease_management');module.render_pm_template=helper
        modules=patch.dict(sys.modules,{'app.program_billing.lease_management':module});modules.start();self.addCleanup(modules.stop)
        view=function('app/admin/programs/billing/routes.py','billing_home',dict(render_template=render_template))
        admin=Blueprint('admin_bp',__name__,url_prefix='/admin');admin.before_request(guard)
        admin.add_url_rule('/billing/',endpoint='billing_home',view_func=view)
        admin.add_url_rule('/<subject>/',endpoint='subject_dashboard',view_func=lambda subject:abort(500,description='Generic route must not shadow billing_home'))
        # Register original target URL rules from source, without invoking unrelated financial workflows.
        self.original=('readings_enter','readings_view','readings_consumption','muni_accounts','billing_tenants','billing_tariffs','muni_recon_statement')
        self.targets={}
        for name in self.original:
            path,endpoint=declared_route('app/admin/programs/billing/routes.py',name,'admin_bp','/admin')
            self.targets[endpoint]=path
            admin.add_url_rule(path.removeprefix('/admin'),endpoint=endpoint.split('.',1)[1],view_func=lambda:abort(501))
        app.register_blueprint(admin)
        for file,name,bp in [('app/program_billing/routes.py','utilities_hub','billing_bp'),('app/program_billing/lease_management.py','lease_management','billing_bp'),('app/bridge/routes.py','bridge','bridge_bp')]:
            path,endpoint=declared_route(file,name,bp);self.targets[endpoint]=path;app.add_url_rule(path,endpoint=endpoint,view_func=lambda:abort(501))
        app.add_url_rule('/',endpoint='public_bp.welcome',view_func=lambda:'Welcome')
        self.client=app.test_client()
    def sign_in(self):
        with self.client.session_transaction() as state:state['_user_id']='7';state['_fresh']=True
    def test_authenticated_admin_gets_original_tracked_landing_and_all_navigation(self):
        self.sign_in();statements=[]
        def record(conn,cursor,statement,parameters,context,many):statements.append(statement)
        event.listen(self.conn,'before_cursor_execute',record)
        try:response=self.client.get('/admin/billing/')
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(response.status_code,200,response.text)
        self.assertTrue(all(s.lstrip().upper().startswith('SELECT') for s in statements))
        self.assertFalse(any('bil_' in s.lower() for s in statements))
        doc=html.fromstring(response.data);links=doc.xpath('//a/@href')
        self.assertEqual(len(links),10);self.assertEqual(len(links),len(set(links)))
        for endpoint,path in self.targets.items():
            self.assertIn(path,links);self.assertEqual(self.app.url_map.bind('localhost').match(path,method='GET')[0],endpoint)
        for label in ('Enter Readings','View Readings','Compute Consumption','Export / Print','Tenants & SOA','Tariffs & Rates','Metro Reconciliation','METSOA','Lease Management'):
            self.assertIn(label,doc.text_content())
        self.assertTrue((ROOT/'templates/admin/programs/billing/index.html').is_file())
    def test_anonymous_access_remains_redirected_by_existing_guard(self):
        response=self.client.get('/admin/billing/');self.assertEqual(response.status_code,302);self.assertEqual(response.location,'/')
    def test_browser_original_navigation_has_no_duplicate_tiles(self):
        self.sign_in();response=self.client.get('/admin/billing/');self.assertEqual(response.status_code,200)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda route:route.abort());tab.set_content(response.text,wait_until='domcontentloaded')
                self.assertEqual(tab.locator('h1').inner_text(),'Billing Administration')
                self.assertEqual(tab.locator('h2').count(),9)
                self.assertEqual(tab.get_by_role('link',name='METSOA',exact=False).filter(has=tab.locator('h2',has_text='METSOA')).count(),1)
                self.assertEqual(tab.locator('a[href="'+self.targets['billing_bp.pm_lease_management']+'"]').count(),1)
            finally:browser.close()

if __name__=='__main__':unittest.main()
