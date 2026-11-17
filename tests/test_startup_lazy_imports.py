"""Admin Hub startup stays light; the existing scan handler imports its SDK on demand."""
import ast
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask, Blueprint, current_app, jsonify, render_template, request, redirect, url_for
from flask_login import LoginManager, UserMixin, current_user
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader

ROOT=Path(__file__).resolve().parents[1]


def functions(path,names,namespace):
    tree=ast.parse((ROOT/path).read_text(encoding='utf-8-sig'))
    exec(compile(ast.Module(body=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names],type_ignores=[]),str(path),'exec'),namespace)


class StartupImportsTests(unittest.TestCase):
    def test_real_readonly_factory_registers_routes_without_ai_sdk(self):
        result=subprocess.run([sys.executable,str(ROOT/'scripts/measure_startup_memory.py'),'factory'],cwd=ROOT,
                              capture_output=True,text=True,timeout=90)
        self.assertEqual(result.returncode,0,result.stderr)
        values=json.loads(result.stdout)
        self.assertFalse(values['heavy_imports']['google.genai'])
        self.assertEqual(values['blocked_sql_count'],0)
        self.assertTrue(all(values['endpoints'].values()))

    def test_actual_admin_hub_and_existing_guard(self):
        app=Flask('admin-startup-test');app.secret_key='test-only'
        app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':'{% block content %}{% endblock %}','partials/flash_messages.html':''}),FileSystemLoader(ROOT/'templates')])
        manager=LoginManager(app)
        class User(UserMixin):
            def __init__(self,id):self.id=id;self.role='admin' if id=='1' else 'learner'
        manager.user_loader(lambda uid:User(uid))
        bp=Blueprint('admin_bp',__name__,url_prefix='/admin')
        query=SimpleNamespace(filter=lambda *args:SimpleNamespace(order_by=lambda *args:SimpleNamespace(all=lambda:[])))
        subject=SimpleNamespace(slug=SimpleNamespace(in_=lambda values:None),name=None,query=query)
        namespace=dict(admin_bp=bp,request=request,current_user=current_user,is_admin=lambda:current_user.is_authenticated and current_user.role=='admin',
                       redirect=redirect,url_for=url_for,AuthSubject=subject,render_template=render_template)
        functions(Path('app/admin/__init__.py'),{'_guard'},namespace)
        functions(Path('app/admin/core.py'),{'index'},namespace)
        app.register_blueprint(bp)
        source=(ROOT/'templates/admin/index.html').read_text()
        targets=set(re.findall(r"url_for\(['\"]([^'\"]+)",source))|{'public_bp.welcome'}
        for endpoint in targets:
            if endpoint not in app.view_functions:app.add_url_rule('/test/'+endpoint.replace('.','/'),endpoint,lambda:'Existing target')
        client=app.test_client()
        self.assertEqual(client.get('/admin/').status_code,302)
        with client.session_transaction() as session:session['_user_id']='2'
        self.assertEqual(client.get('/admin/').status_code,302)
        with client.session_transaction() as session:session['_user_id']='1'
        response=client.get('/admin/')
        self.assertEqual(response.status_code,200)
        self.assertIn('Admin Hub',response.text);self.assertIn('Franchisee Register',response.text)

    def test_scan_cached_validation_and_ondemand_paths(self):
        root=(ROOT/'tmp');root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='ait-startup-',dir=root) as directory:
            self.assertTrue(Path(directory).resolve().is_relative_to(root.resolve()))
            app=Flask('scan-lazy-test',root_path=directory)
            bp=Blueprint('admin_bp',__name__)
            import glob
            namespace=dict(admin_bp=bp,request=request,current_app=current_app,jsonify=jsonify,json=json,os=os,glob=glob,base64=__import__('base64'))
            functions(Path('app/admin/programs/grade12_core_math/routes.py'),{'adv_math_ai_scan_paper'},namespace)
            app.register_blueprint(bp);client=app.test_client()
            self.assertEqual(client.post('/adv_math/ai_scan_paper',json={}).status_code,400)
            self.assertEqual(client.post('/adv_math/ai_scan_paper',json={'paper_name':'Missing'}).status_code,404)
            cache=Path(directory)/'data/ai_scans/Example.json';cache.write_text('[]')
            with patch.dict(sys.modules,{'google':ModuleType('google')}):
                response=client.post('/adv_math/ai_scan_paper',json={'paper_name':'Example'})
            self.assertEqual(response.status_code,200);self.assertTrue(response.json['cached'])
            cache.unlink()
            papers=Path(directory)/'data/dbe_papers';papers.mkdir()
            (papers/'Example.pdf').write_bytes(b'%PDF-synthetic')
            configure=Mock();generate=Mock(return_value=SimpleNamespace(text='[]'))
            package=ModuleType('google');package.genai=SimpleNamespace(configure=configure,GenerativeModel=lambda name:SimpleNamespace(generate_content=generate))
            with patch.dict(sys.modules,{'google':package}):
                response=client.post('/adv_math/ai_scan_paper',json={'paper_name':'Example'})
            self.assertEqual(response.status_code,200)
            configure.assert_called_once();generate.assert_called_once()
            self.assertEqual(json.loads(cache.read_text()),[])


if __name__=='__main__':unittest.main()
