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

    def dashboard_data(self, properties, show_hidden=False):
        import types
        tree = ast.parse((ROOT / 'app/program_billing/helpers.py').read_text(encoding='utf-8'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_dashboard_data')
        query = Mock()
        def filtered(**criteria):
            rows = [p for p in properties if all(getattr(p, key) == value for key, value in criteria.items())]
            return types.SimpleNamespace(all=lambda: rows)
        query.filter_by.side_effect = filtered
        scope = dict(current_user=types.SimpleNamespace(id=7), BilProperty=types.SimpleNamespace(query=query))
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'property-list', 'exec'), scope)
        return scope['get_dashboard_data'](show_hidden=show_hidden), query

    def test_properties_without_tenants_units_or_leases(self):
        import types
        prop = types.SimpleNamespace(id=11, manager_id=7, name='Empty property', address='', meters=[], is_archived=False)
        rows, query = self.dashboard_data([prop])
        query.filter_by.assert_called_once_with(manager_id=7, is_archived=False)
        self.assertEqual([row['property_id'] for row in rows], [11])
        self.assertNotIn('tenant_id', rows[0])
        self.assertNotIn('tenant_name', rows[0])

    def test_existing_properties_never_load_tenant_relationship(self):
        class ExistingProperty:
            id = 12
            manager_id = 7
            name = 'Existing property'
            address = 'Existing address'
            is_archived = False
            meters = [Mock(utility_type='water')]
            @property
            def tenants(self):
                raise AssertionError('Tenant relationship must not be loaded')
            @property
            def leases(self):
                raise AssertionError('Lease relationship must not be loaded')
            @property
            def sectional_units(self):
                raise AssertionError('Unit relationship must not be loaded')
        rows, _ = self.dashboard_data([ExistingProperty()])
        self.assertEqual(rows[0]['property_name'], 'Existing property')
        self.assertNotIn('meter_number', rows[0])
        self.assertNotIn('utility_type', rows[0])

    def test_properties_with_or_without_meters_never_load_relationships(self):
        class PropertyRecord:
            id = 12
            manager_id = 7
            name = 'Property'
            address = ''
            is_archived = False
            def __init__(self, has_meters):
                self.has_meters = has_meters
            @property
            def meters(self):
                raise AssertionError('ORM meter query must not run, regardless of meter existence')
            @property
            def tenants(self):
                raise AssertionError('Tenant relationship must not load')
            @property
            def leases(self):
                raise AssertionError('Lease relationship must not load')
            @property
            def muni_accounts(self):
                raise AssertionError('Municipal accounts must not load')
        for has_meters in (False, True):
            with self.subTest(has_meters=has_meters):
                rows, query = self.dashboard_data([PropertyRecord(has_meters)])
                self.assertEqual(rows, [dict(property_id=12, property_name='Property', address='', is_archived=False)])
                query.filter_by.assert_called_once_with(manager_id=7, is_archived=False)

    def test_dashboard_and_portfolio_render_property_only_rows(self):
        from jinja2 import ChoiceLoader, DictLoader
        env = Environment(loader=ChoiceLoader([DictLoader({
            'layout.html': '{% block content %}{% endblock %}',
            'partials/flash_messages.html': '',
        }), FileSystemLoader(ROOT / 'templates')]))
        env.globals['url_for'] = lambda endpoint, **kw: '/fixture/' + endpoint
        env.globals['csrf_token'] = lambda: 'test-token'
        data = [dict(property_id=12, property_name='Existing property', address='', is_archived=False)]
        html = env.get_template('program_billing/manager_dashboard.html').render(data=data, lease_workspace=True)
        for label in ('METSOA', 'Tenant SOA', 'Metro Reconciliation', 'Lease Management', 'Property Portfolio'):
            self.assertIn(label, html)
        self.assertIn('/fixture/billing_bp.property_portfolio', html)
        self.assertIn('Existing property', html)
        portfolio = env.get_template('program_billing/property_portfolio.html').render(data=data)
        for endpoint in ('architecture_summary', 'setup_wizard', 'delete_property'):
            self.assertIn('/fixture/billing_bp.' + endpoint, portfolio)
        self.assertIn('Existing property', portfolio)

    def test_other_managers_excluded_even_when_showing_archived_properties(self):
        import types
        props = [types.SimpleNamespace(id=i, manager_id=manager, name=str(i), address='', meters=[], is_archived=archived)
                 for i, manager, archived in [(11, 7, False), (12, 7, True), (22, 8, False)]]
        rows, _ = self.dashboard_data(props)
        self.assertEqual([r['property_id'] for r in rows], [11])
        rows, query = self.dashboard_data(props, show_hidden=True)
        self.assertEqual([r['property_id'] for r in rows], [11, 12])
        query.filter_by.assert_called_once_with(manager_id=7)

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
