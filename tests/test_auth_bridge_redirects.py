"""Actual auth functions on isolated Flask: no factory, env or database."""
import ast
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch
from flask import Blueprint, Flask, current_app, flash, redirect, render_template, request, session, url_for, abort
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from flask_wtf import FlaskForm, CSRFProtect
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired, Length
from jinja2 import DictLoader
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]


class Identity(UserMixin):
    def __init__(self, active=True):
        self.id = 7
        self.email = 'synthetic@example.invalid'
        self.name = 'Synthetic user'
        self.active = active
        self.password_hash = 'synthetic-hash'
    @property
    def is_active(self):
        return self.active
    def check_password(self, attempt):
        return attempt == 'synthetic-password'


class AuthBridgeTests(unittest.TestCase):
    def setUp(self):
        from contextlib import ExitStack
        import socket
        import dotenv
        self.safety = ExitStack()
        self.addCleanup(self.safety.close)
        self.safety.enter_context(patch.object(socket.socket, 'connect', side_effect=AssertionError('Network forbidden')))
        self.safety.enter_context(patch('sqlalchemy.engine.Engine.connect', side_effect=AssertionError('Database forbidden')))
        for name in ('load_dotenv', 'find_dotenv', 'dotenv_values'):
            self.safety.enter_context(patch.object(dotenv, name, side_effect=AssertionError('Environment loading forbidden')))
        self.user = Identity()
        self.admin = False
        self.subjects = []
        self.enrollments = []
        self.app = Flask('isolated-auth-bridge')
        self.app.config.update(TESTING=True, SECRET_KEY='synthetic-auth-secret')
        self.app.jinja_loader = DictLoader({
            'auth/login.html': '{{ form.csrf_token }}{% for message in get_flashed_messages() %}{{ message }}{% endfor %}',
            'auth/bridge_dashboard.html': 'AIT Bridge {{ user.name }}{% for tile in subjects %}<a href="{{ tile.href }}">{{ tile.name }}</a>{% endfor %}',
        })
        CSRFProtect(self.app)
        manager = LoginManager(self.app)
        manager.login_view = 'auth_bp.login'
        manager.user_loader(lambda identifier: self.user if identifier == str(self.user.id) else None)
        auth_bp = Blueprint('auth_bp', __name__)
        bridge_bp = Blueprint('bridge_bp', __name__)
        query = Mock()
        query.filter_by.return_value.first.side_effect = lambda: self.user
        query.get.return_value = self.user
        crm = Mock()
        crm.filter_by.return_value.first.return_value = None
        modules = {name: types.ModuleType(name) for name in (
            'app', 'app.auth', 'app.auth.routes', 'app.program_sace', 'app.program_sace.access',
            'app.sace_activity', 'app.models', 'app.models.practice_crm')}
        modules['app.program_sace.access'].prepare_provisioning_auth = Mock()
        self.activity = Mock(return_value=None)
        modules['app.sace_activity'].resolve_response = self.activity
        modules['app.sace_activity'].subject_slugs = lambda: set()
        modules['app.models.practice_crm'].CrmPracticeUser = types.SimpleNamespace(query=crm)
        module_patch = patch.dict(sys.modules, modules)
        module_patch.start()
        self.addCleanup(module_patch.stop)
        form_tree = ast.parse((ROOT / 'app/auth/forms.py').read_text(encoding='utf-8-sig'))
        form = next(n for n in form_tree.body if isinstance(n, ast.ClassDef) and n.name == 'LoginForm')
        ns = dict(FlaskForm=FlaskForm, StringField=StringField, PasswordField=PasswordField,
            BooleanField=BooleanField, SubmitField=SubmitField, DataRequired=DataRequired, Length=Length)
        exec(compile(ast.Module(body=[form], type_ignores=[]), 'actual-login-form', 'exec'), ns)
        db = types.SimpleNamespace(engine=object(), session=Mock())
        def execute(statement, *args, **kwargs):
            rows = [types.SimpleNamespace(id=1,slug='billing',access_level='enrolled')] if 'legacy_bridge' in str(statement) else []
            return types.SimpleNamespace(fetchall=lambda:rows, mappings=lambda:types.SimpleNamespace(all=lambda:[]), scalar=lambda:None)
        db.session.execute.side_effect = execute
        inspector = patch('sqlalchemy.inspect', return_value=types.SimpleNamespace(get_table_names=lambda:[]))
        inspector.start()
        self.addCleanup(inspector.stop)
        ns.update(auth_bp=auth_bp, bridge_bp=bridge_bp, User=types.SimpleNamespace(query=query), db=db,
            current_app=current_app, current_user=current_user, request=request, session=session,
            flash=flash, redirect=redirect, render_template=render_template, url_for=url_for, abort=abort,
            login_user=login_user, logout_user=logout_user, login_required=login_required,
            check_admin=lambda email:self.admin, text=text, sa_text=text, BRIDGE_QUERY='SELECT legacy_bridge',
            get_all_subjects=lambda:self.subjects, get_user_enrollment=lambda user_id:self.enrollments,
            ensure_free_enrollment=Mock())
        auth_tree = ast.parse((ROOT / 'app/auth/routes.py').read_text(encoding='utf-8-sig'))
        functions = [n for n in auth_tree.body if isinstance(n, ast.FunctionDef) and n.name in ('login','logout','bridge_dashboard')]
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'actual-auth-functions', 'exec'), ns)
        bridge_tree = ast.parse((ROOT / 'app/bridge/routes.py').read_text(encoding='utf-8-sig'))
        function = next(n for n in bridge_tree.body if isinstance(n, ast.FunctionDef) and n.name=='bridge_dashboard')
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'actual-bridge-function', 'exec'), ns)
        self.app.register_blueprint(auth_bp)
        self.app.register_blueprint(bridge_bp)
        self.app.add_url_rule('/welcome', endpoint='public_bp.welcome', view_func=lambda:'Welcome')
        self.app.add_url_rule('/billing/dashboard', endpoint='billing_bp.learner_dashboard', view_func=lambda:'PM')
        self.app.add_url_rule('/mode/<subject_slug>', endpoint='modes_bp.modes_checkpoint_route', view_func=lambda subject_slug:'fixture')
        self.app.add_url_rule('/price/<subject>', endpoint='payment_bp.pricing_base', view_func=lambda subject:'fixture')
        self.app.add_url_rule('/admin/<subject>', endpoint='admin_bp.subject_dashboard', view_func=lambda subject:'fixture')
        self.client = self.app.test_client()

    def login(self, query=''):
        # Obtain the signed token generated by the actual GET form.
        response = self.client.get('/login' + query)
        import re
        token = re.search(r'value="([^"]+)"', response.get_data(as_text=True)).group(1)
        return self.client.post('/login' + query, data=dict(email=self.user.email,
            password='synthetic-password', csrf_token=token))

    def test_ordinary_login_reaches_bridge_with_session_surviving_redirects(self):
        response = self.login()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/dashboard')
        dashboard = self.client.get(response.location)
        self.assertEqual(dashboard.location, '/bridge')
        bridge = self.client.get(dashboard.location)
        self.assertEqual(bridge.status_code, 200)
        self.assertIn('AIT Bridge Synthetic user', bridge.get_data(as_text=True))
        with self.client.session_transaction() as data:
            self.assertEqual(data['_user_id'], '7')
            self.assertFalse(data['is_admin'])

    def test_pm_enrolled_user_still_reaches_bridge(self):
        self.subjects = [types.SimpleNamespace(id=1,slug='billing',name='Property Management',
            program_type='paid',commercial_mode='paid',is_hidden_on_bridge=False)]
        self.enrollments = [types.SimpleNamespace(subject_id=1,user_id=7,id=10)]
        self.login()
        response = self.client.get('/dashboard', follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('AIT Bridge', response.get_data(as_text=True))
        self.assertIn('Property Management', response.get_data(as_text=True))

    def test_anonymous_or_forged_identity_cannot_enter_dashboard_or_bridge(self):
        with self.client.session_transaction() as data:
            data.update(email=self.user.email,is_authenticated=True,is_admin=True)
        for path in ('/dashboard','/bridge'):
            response = self.client.get(path)
            self.assertEqual(response.status_code,302)
            self.assertTrue(response.location.startswith('/login'))

    def test_inactive_account_cannot_receive_authenticated_scaffold(self):
        self.user.active=False
        response=self.login()
        self.assertEqual(response.status_code,200)
        with self.client.session_transaction() as data:
            self.assertNotIn('_user_id',data)
            self.assertNotIn('is_authenticated',data)

    def test_safe_next_preserved_external_next_rejected(self):
        response=self.login('?next=/billing/dashboard')
        self.assertEqual(response.location,'/billing/dashboard')
        self.client.get('/logout')
        response=self.login('?next=https://example.invalid/')
        self.assertEqual(response.location,'/dashboard')

    def test_logout_clears_login_and_remember_identity(self):
        self.login()
        response=self.client.get('/logout')
        self.assertEqual(response.location,'/welcome')
        with self.client.session_transaction() as data:
            self.assertNotIn('_user_id',data)
            self.assertNotIn('email',data)
        self.assertTrue(self.client.get('/bridge').location.startswith('/login'))
        self.assertIn('remember_token=', ';'.join(response.headers.getlist('Set-Cookie')))

    def test_bridge_retains_activity_specific_resolution(self):
        self.login()
        self.activity.return_value=redirect('/activity-fixture')
        response=self.client.get('/bridge')
        self.assertEqual(response.location,'/activity-fixture')


    def test_platform_admin_also_uses_authenticated_bridge(self):
        self.admin=True
        self.login()
        response=self.client.get('/dashboard',follow_redirects=True)
        self.assertEqual(response.status_code,200)
        self.assertIn('AIT Bridge',response.get_data(as_text=True))
        with self.client.session_transaction() as data:
            self.assertEqual(data['_user_id'],'7')

    def test_invalid_password_and_missing_csrf_cannot_authenticate(self):
        response=self.client.get('/login')
        import re
        token=re.search(r'value="([^"]+)"',response.get_data(as_text=True)).group(1)
        response=self.client.post('/login',data=dict(email=self.user.email,password='incorrect-password',csrf_token=token))
        self.assertEqual(response.status_code,200)
        with self.client.session_transaction() as data:
            self.assertNotIn('_user_id',data)
        response=self.client.post('/login',data=dict(email=self.user.email,password='synthetic-password'))
        self.assertEqual(response.status_code,400)

    def test_dashboard_requires_login_identity_not_legacy_email(self):
        with self.client.session_transaction() as data:
            data['email']=self.user.email
        response=self.client.get('/dashboard',follow_redirects=True)
        self.assertEqual(response.status_code,200)
        self.assertIn('csrf_token',response.get_data(as_text=True))
        self.assertNotIn('AIT Bridge',response.get_data(as_text=True))


if __name__=='__main__':
    unittest.main(verbosity=2)
