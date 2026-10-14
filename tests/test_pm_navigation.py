"""Focused navigation tests without the mutating application factory."""
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock
from flask import Flask, redirect, url_for
from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]

class PropertyNavigationTests(unittest.TestCase):
    def test_legacy_program_entry_redirects_before_catalog_metadata(self):
        tree = ast.parse((ROOT / 'app/auth/routes.py').read_text(encoding='utf-8'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'learner_subject_dashboard')
        node.decorator_list = []
        database = Mock()
        scope = dict(redirect=redirect, url_for=url_for, db=database)
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'entry', 'exec'), scope)
        app = Flask(__name__)
        app.add_url_rule('/billing/dashboard', endpoint='billing_bp.learner_dashboard')
        with app.test_request_context():
            for slug in ('billing', ' BILLING '):
                self.assertEqual(scope['learner_subject_dashboard'](slug).location, '/billing/dashboard')
        database.session.execute.assert_not_called()

    def test_bridge_opens_existing_dashboard_without_old_messaging(self):
        env = Environment(loader=FileSystemLoader(ROOT / 'templates'))
        env.globals['url_for'] = lambda endpoint, **kw: '/billing/dashboard' if endpoint == 'billing_bp.learner_dashboard' else '/fixture'
        env.globals['get_flashed_messages'] = lambda **kw: []
        env.globals['current_user'] = Mock(has_role=Mock(return_value=False))
        template = env.from_string((ROOT / 'templates/auth/bridge_dashboard.html').read_text(encoding='utf-8').replace('{% extends "layout.html" %}', ''))
        html = template.render(user=Mock(name='Manager'), subjects=[dict(slug='billing', name='Billing', access_level='enrolled', message='Your trial/subscription program is active.')], platform_admin_bridge=False)
        self.assertIn('href="/billing/dashboard"', html)
        self.assertNotIn('Quick Start Guide', html)
        self.assertNotIn('subscription', html)
        self.assertIn('Property Management', html)

    def test_property_list_is_scoped_to_current_manager(self):
        import types
        tree = ast.parse((ROOT / 'app/program_billing/helpers.py').read_text(encoding='utf-8'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_dashboard_data')
        prop = types.SimpleNamespace(id=11, name='Authorized property', address='', tenants=[], meters=[], is_archived=False)
        query = Mock()
        query.filter_by.return_value.all.return_value = [prop]
        scope = dict(current_user=types.SimpleNamespace(id=7), BilProperty=types.SimpleNamespace(query=query))
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'property-list', 'exec'), scope)
        rows = scope['get_dashboard_data']()
        query.filter_by.assert_called_once_with(manager_id=7, is_archived=False)
        self.assertEqual([row['property_id'] for row in rows], [11])
        self.assertNotIn('admin', scope)

    def test_locked_entry_preserves_unlock_control(self):
        env = Environment(loader=FileSystemLoader(ROOT / 'templates'))
        env.globals['url_for'] = lambda endpoint, **kw: '/unlock' if endpoint == 'auth_bp.dashboard_info' else '/fixture'
        env.globals['get_flashed_messages'] = lambda **kw: []
        env.globals['current_user'] = Mock(has_role=Mock(return_value=False))
        template = env.from_string((ROOT / 'templates/auth/bridge_dashboard.html').read_text(encoding='utf-8').replace('{% extends "layout.html" %}', ''))
        html = template.render(user=Mock(name='Manager'), subjects=[dict(slug='billing', name='Billing', access_level='locked')], platform_admin_bridge=False)
        self.assertIn('href="/unlock"', html)
        self.assertIn('Unlock / Enroll', html)
        self.assertIn('disabled>Locked', html)

if __name__ == '__main__':
    unittest.main()
