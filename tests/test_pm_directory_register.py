"""Browser-local directory assignments and an isolated read-only hub route."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from flask import Flask, Blueprint, request, current_app, render_template, abort
from flask_login import LoginManager, UserMixin, current_user, login_required
from sqlalchemy import text
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader

ROOT=Path(__file__).resolve().parents[1]
catalog_tree=ast.parse((ROOT/'app/program_billing/utilities_output_reports.py').read_text(encoding='utf-8'))
values={n.targets[0].id:ast.literal_eval(n.value) for n in catalog_tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in {'CATALOG','SOURCES'}}
REPORTS=[dict(code=c,title=t,source=values['SOURCES'][source]) for source,items in values['CATALOG'].items() for c,t in items]


def browser_html():
    app=Flask('directory-browser')
    app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':'<html><head></head><body>{% block content %}{% endblock %}</body></html>'}),FileSystemLoader(str(ROOT/'templates'))])
    with app.test_request_context():
        return render_template('program_billing/directory_register.html',properties=[dict(id=26,name='Almond')],selected_property=dict(id=26,name='Almond'),directory_context=dict(user='7',property='26'),directory_reports=REPORTS,url_for=lambda endpoint,**kw:'/static/'+kw['filename'] if endpoint=='static' else '/'+endpoint)


def open_browser(runtime,markup=None,init=''):
    browser=runtime.chromium.launch(headless=True)
    page=browser.new_page()
    def serve(route):
        name=route.request.url.split('/')[-1]
        if name.endswith('.js') and (ROOT/'app/static/js'/name).exists():route.fulfill(body=(ROOT/'app/static/js'/name).read_text(encoding='utf-8'),content_type='text/javascript')
        else:route.fulfill(body=markup or browser_html(),content_type='text/html')
    page.route('**/*',serve)
    page.add_init_script(init or "window.showDirectoryPicker=async()=>{const root=await navigator.storage.getDirectory();return root.getDirectoryHandle('Reports',{create:true});};")
    page.goto('https://ait.test/directory-register')
    return browser,page


class DirectoryRegisterTests(unittest.TestCase):

    def test_select_auto_save_reload_replace_and_property_isolation(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Reports')
                self.assertEqual(page.evaluate("async()=>Array.fromAsync((await AITDirectories.get({user:'7',property:'26'})).handle.keys())"),[])
                self.assertFalse(page.evaluate("AITDirectories.get({user:'8',property:'26'})"))
                # Retain a distinct assignment for another property.
                page.evaluate("""async()=>{const root=await navigator.storage.getDirectory();
                    await AITDirectories.set({user:'7',property:'27'},await root.getDirectoryHandle('Dale',{create:true}));
                    window.showDirectoryPicker=async()=>root.getDirectoryHandle('Replacement',{create:true});}""")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                self.assertEqual(page.locator('#directory-folder').inner_text(),'Replacement')
                page.reload()
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Replacement'")
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'27'})).handle.name"),'Dale')
                # No permission prompts on reopening.
                page.evaluate("window.permissionPrompts=0;FileSystemDirectoryHandle.prototype.requestPermission=async()=>{permissionPrompts++;return 'granted';};")
                page.evaluate("location.reload()")
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Replacement'")
            finally:browser.close()

    def test_picker_options_gesture_and_immediate_display_before_storage(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init="""window.showDirectoryPicker=async options=>{
                window.pickerCall={options,active:navigator.userActivation.isActive};
                return (await navigator.storage.getDirectory()).getDirectoryHandle('Selected now',{create:true});
            };""")
            try:
                page.evaluate("""()=>{window.persist=AITDirectories.set;
                    AITDirectories.set=async(...args)=>{window.finishSave=()=>persist(...args);await new Promise(resolve=>window.releaseSave=resolve);return finishSave();};}""")
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Selected now'")
                self.assertEqual(page.evaluate('pickerCall'),dict(options=dict(mode='readwrite',id='ait-pm-reports'),active=True))
                self.assertFalse(page.evaluate("AITDirectories.get({user:'7',property:'26'})"))
                page.evaluate('releaseSave()')
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                self.assertEqual(page.evaluate("async()=>(await AITDirectories.get({user:'7',property:'26'})).handle.name"),'Selected now')
            finally:browser.close()

    def test_reload_never_requests_permission(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime)
            try:
                page.locator('#directory-select').click()
                page.wait_for_function("document.getElementById('directory-message').textContent==='Folder saved.'")
                page.add_init_script("window.prompts=0;FileSystemDirectoryHandle.prototype.requestPermission=async()=>{prompts++;throw Error('No load prompts');};")
                page.reload()
                page.wait_for_function("document.getElementById('directory-folder').textContent==='Reports'")
                self.assertEqual(page.evaluate('prompts'),0)
            finally:browser.close()

    def test_unsupported_browser_has_short_fallback(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runtime:
            browser,page=open_browser(runtime,init='window.showDirectoryPicker=undefined;')
            try:
                self.assertTrue(page.locator('#directory-select').is_disabled())
                self.assertIn('desktop Chrome or Edge',page.locator('#directory-message').inner_text())
            finally:browser.close()

    def test_real_route_requires_login_and_manager_scope_without_writes(self):
        app=Flask('directory-route');app.secret_key='test-only';app.config['TESTING']=True
        manager=LoginManager(app)
        class User(UserMixin):
            def __init__(self,id):self.id=id
        manager.user_loader(lambda uid:User(int(uid)))
        properties=[dict(id=26,name='Almond',manager_id=7),dict(id=28,name='Almond',manager_id=8)]
        def execute(statement,params):
            sql=str(statement)
            self.assertTrue(sql.startswith('SELECT'))
            rows=[p for p in properties if p['manager_id']==params['u']] if 'name FROM' in sql else [p for p in properties if p['id']==params['id']]
            return SimpleNamespace(mappings=lambda:SimpleNamespace(first=lambda:rows[0] if rows else None,__iter__=lambda self:iter(rows))) if 'name FROM' not in sql else SimpleNamespace(mappings=lambda:iter(rows))
        db=SimpleNamespace(session=SimpleNamespace(execute=execute))
        scope_tree=ast.parse((ROOT/'app/program_billing/metsoa_setup_safety.py').read_text(encoding='utf-8-sig'))
        scope=next(n for n in scope_tree.body if isinstance(n,ast.FunctionDef) and n.name=='property_scope')
        namespace=dict(db=db,text=text,current_user=current_user,abort=abort)
        exec(compile(ast.Module(body=[scope],type_ignores=[]),'<real-property-scope>','exec'),namespace)
        blueprint=Blueprint('billing_bp',__name__)
        tree=ast.parse((ROOT/'app/program_billing/directory_register.py').read_text())
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef))
        namespace.update(billing_bp=blueprint,login_required=login_required,require_pm_user=lambda:None,request=request,current_app=current_app,SOURCES={'all':'Utilities'},CATALOG={'all':[(r['code'],r['title']) for r in REPORTS]},render_pm_template=lambda name,**kw:dict(user=kw['directory_context']['user'],properties=kw['properties']))
        exec(compile(ast.Module(body=[function],type_ignores=[]),'<real-directory-route>','exec'),namespace)
        app.register_blueprint(blueprint);client=app.test_client()
        self.assertEqual(client.get('/billing/directory-register').status_code,401)
        for uid,own,foreign in [(7,26,28),(8,28,26)]:
            with client.session_transaction() as session:session['_user_id']=str(uid);session['_fresh']=True
            response=client.get('/billing/directory-register?property_id='+str(own));self.assertEqual(response.status_code,200)
            self.assertEqual([p['id'] for p in response.json['properties']],[own])
            self.assertEqual(client.get('/billing/directory-register?property_id='+str(foreign)).status_code,403)
        self.assertEqual(client.post('/billing/directory-register').status_code,405)
