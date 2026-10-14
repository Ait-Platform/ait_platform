"""Focused property operations on session-temporary local PostgreSQL fixtures."""
import ast
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
from flask import Flask, Blueprint, render_template
from flask_login import LoginManager, UserMixin, login_required
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
import sqlalchemy as sa
from sqlalchemy.orm import registry, Session
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]

class Identity(UserMixin):
    def __init__(self, identifier): self.id = identifier

class PropertyManagementTests(unittest.TestCase):
    def setUp(self):
        source_root = ROOT if (ROOT / '.env').exists() else ROOT.parent.parent
        url = dotenv_values(source_root / '.env').get('DATABASE_URL')
        parsed = sa.engine.make_url(url.replace('postgres://', 'postgresql://', 1))
        self.assertEqual(parsed.database, 'ait_local_db')
        self.assertIn(parsed.host, ('localhost','127.0.0.1','::1'))
        self.engine = sa.create_engine(parsed)
        self.addCleanup(self.engine.dispose)
        self.conn = self.engine.connect()
        self.addCleanup(self.conn.close)
        self.assertEqual(self.conn.exec_driver_sql('SELECT current_database()').scalar(), 'ait_local_db')
        self.conn.exec_driver_sql('CREATE TEMP TABLE bil_property (id serial PRIMARY KEY, name varchar(100) NOT NULL, address varchar(200), manager_id integer, is_archived boolean, onboarding_status varchar(50))')
        self.conn.exec_driver_sql('SET search_path TO pg_temp')
        self.conn.exec_driver_sql("INSERT INTO bil_property(id,name,address,manager_id,is_archived) VALUES (26,'Almond','Original address',7,false),(27,'Other manager','Private address',8,false)")
        self.conn.exec_driver_sql("SELECT setval(pg_get_serial_sequence('bil_property','id'),27)")
        self.conn.commit()
        mapper = registry()
        table = sa.Table('bil_property', mapper.metadata, autoload_with=self.conn)
        class Property: pass
        mapper.map_imperatively(Property, table)
        self.conn.commit()
        self.session = Session(self.conn)
        self.addCleanup(self.session.close)
        blueprint = Blueprint('billing_bp', __name__)
        self.app = Flask('property-management-test')
        self.app.config.update(TESTING=True, SECRET_KEY='isolated-test')
        self.app.jinja_loader = ChoiceLoader([DictLoader({'layout.html':'{% block content %}{% endblock %}', 'partials/flash_messages.html':''}), FileSystemLoader(ROOT/'templates')])
        self.app.jinja_env.globals['csrf_token'] = lambda: 'synthetic'
        login = LoginManager(self.app)
        login.user_loader(lambda identifier: Identity(int(identifier)))
        modules = {}
        for name in ('app','app.models','app.program_billing','app.extensions','app.models.billing','app.program_billing.routes','app.program_billing.lease_management'):
            modules[name] = types.ModuleType(name)
            modules[name].__path__ = []
        modules['app.extensions'].db = types.SimpleNamespace(session=self.session)
        modules['app.models.billing'].BilProperty = Property
        modules['app.program_billing.routes'].billing_bp = blueprint
        modules['app.program_billing.lease_management'].render_pm_template = render_template
        module_patch = patch.dict(sys.modules, modules);module_patch.start();self.addCleanup(module_patch.stop)
        spec = importlib.util.spec_from_file_location('app.program_billing.property_management',ROOT/'app/program_billing/property_management.py')
        mod = importlib.util.module_from_spec(spec);sys.modules[spec.name]=mod;spec.loader.exec_module(mod)
        self.module = mod
        # Include the real main route, without starting the AIT application factory.
        tree = ast.parse((ROOT/'app/program_billing/routes.py').read_text(encoding='utf-8'))
        for function in ('property_owner', 'learner_dashboard'):
            node = next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==function)
            exec(compile(ast.Module(body=[node],type_ignores=[]),'property-owner-route','exec'),dict(billing_bp=blueprint,login_required=login_required))
        for endpoint in ('property_portfolio','pm_lease_management','utilities_hub','tenant_accounts'):
            blueprint.add_url_rule('/fixture/'+endpoint,endpoint=endpoint,view_func=lambda:'fixture')
        self.app.register_blueprint(blueprint)
        self.client = self.app.test_client()
        self.sign_in(7)

    def sign_in(self, identifier):
        with self.client.session_transaction() as session:
            session['_user_id']=str(identifier);session['_fresh']=True

    def count(self):
        return self.session.execute(sa.text('SELECT count(*) FROM bil_property')).scalar()

    def test_default_dashboard_reuses_property_owner_working_area(self):
        response=self.client.get('/billing/dashboard')
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        for content in ('Save Property','My Properties','Almond','Original address','<dialog','Assign Owner'):
            self.assertIn(content,html)
        self.assertNotIn('Other manager',html)
        self.assertNotIn('Property Management modules',html)
        self.assertIn('aria-current="page"',html)
        for endpoint in ('pm_lease_management','utilities_hub','tenant_accounts'):
            self.assertIn('/fixture/'+endpoint,html)
        self.assertEqual(self.count(),2)

    def test_dashboard_and_home_require_authentication(self):
        with self.client.session_transaction() as session:session.clear()
        self.assertEqual(self.client.get('/billing/dashboard').status_code,401)
        self.assertEqual(self.client.get('/billing/home').status_code,401)

    def test_existing_properties_appear_without_reregistration(self):
        html=self.client.get('/billing/property-owner').get_data(as_text=True)
        self.assertIn('Almond',html);self.assertIn('Original address',html)
        self.assertNotIn('Other manager',html);self.assertNotIn('Private address',html)
        self.assertEqual(self.count(),2)

    def test_registration_only_creates_a_property_and_reuses_retries(self):
        self.assertEqual(self.client.post('/billing/property-owner',data={'property_name':'New property'}).status_code,302)
        self.assertEqual(self.count(),3)
        self.client.post('/billing/property-owner',data={'property_name':' NEW PROPERTY '})
        self.assertEqual(self.count(),3)
        self.client.post('/billing/property-owner',data={'property_name':'Almond'})
        self.assertEqual(self.count(),3)
        self.assertEqual(set(sa.inspect(self.conn).get_temp_table_names()),{'bil_property'})

    def test_address_update_preserves_property_id_and_other_records(self):
        response=self.client.post('/billing/property-owner/26/address',data={'address':'Unit 2\n10 Main Road\nJohannesburg 2000'})
        self.assertEqual(response.status_code,302)
        self.assertEqual(self.session.execute(sa.text('SELECT name,address FROM bil_property WHERE id=26')).one(),('Almond','Unit 2\n10 Main Road\nJohannesburg 2000'))
        self.assertEqual(self.count(),2)
        self.assertIn('10 Main Road',self.client.get('/billing/property-owner').get_data(as_text=True))

    def test_cross_manager_address_update_is_denied(self):
        self.assertEqual(self.client.post('/billing/property-owner/27/address',data={'address':'Forged'}).status_code,404)
        self.assertEqual(self.session.execute(sa.text('SELECT address FROM bil_property WHERE id=27')).scalar(),'Private address')

    def test_validation_never_inserts_or_truncates(self):
        for name in ('', ' '*3, 'X'*101):
            self.assertEqual(self.client.post('/billing/property-owner',data={'property_name':name}).status_code,400)
        self.assertEqual(self.client.post('/billing/property-owner/26/address',data={'address':'X'*201}).status_code,400)
        self.assertEqual(self.count(),2)

    def test_anonymous_cannot_read_create_or_change_address(self):
        with self.client.session_transaction() as session:session.clear()
        self.assertEqual(self.client.get('/billing/property-owner').status_code,401)
        self.assertEqual(self.client.post('/billing/property-owner',data={'property_name':'Forbidden'}).status_code,401)
        self.assertEqual(self.client.post('/billing/property-owner/26/address',data={'address':'Forbidden'}).status_code,401)
        self.assertEqual(self.count(),2)

    def test_same_name_is_allowed_for_another_manager(self):
        self.sign_in(8)
        self.client.post('/billing/property-owner',data={'property_name':'Almond'})
        self.assertEqual(self.count(),3)
        self.assertEqual(self.session.execute(sa.text("SELECT count(*) FROM bil_property WHERE name='Almond'")).scalar(),2)

    def test_owner_controls_are_disabled_without_loading_owner_tables(self):
        html=self.client.get('/billing/property-owner').get_data(as_text=True)
        self.assertIn('data-open-owner',html)
        self.assertIn('/billing/property-owner/26/edit',html)
        self.assertIn('disabled id="owner-save-disabled"',html)
        self.assertIn('<dialog',html)

    def test_edit_prepopulates_and_persists_available_property_fields(self):
        response=self.client.get('/billing/property-owner/26/edit')
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        self.assertIn('value="Almond"',html);self.assertIn('Original address',html)
        response=self.client.post('/billing/property-owner/26/edit',data={'property_name':'Almond House','address':'New complete address'})
        self.assertEqual(response.status_code,302)
        self.assertEqual(self.session.execute(sa.text('SELECT name,address FROM bil_property WHERE id=26')).one(),('Almond House','New complete address'))
        self.assertEqual(self.count(),2)

    def test_edit_cross_manager_and_anonymous_are_denied(self):
        self.assertEqual(self.client.get('/billing/property-owner/27/edit').status_code,404)
        self.assertEqual(self.client.post('/billing/property-owner/27/edit',data={'property_name':'Forged','address':'Forged'}).status_code,404)
        with self.client.session_transaction() as session:session.clear()
        self.assertEqual(self.client.get('/billing/property-owner/26/edit').status_code,401)

    def test_edit_validation_and_duplicate_name_do_not_modify_records(self):
        for payload in ({'property_name':'','address':''},{'property_name':'X'*101},{'property_name':'Almond','address':'X'*201}):
            self.assertEqual(self.client.post('/billing/property-owner/26/edit',data=payload).status_code,400)
        self.client.post('/billing/property-owner',data={'property_name':'Second'})
        self.assertEqual(self.client.post('/billing/property-owner/26/edit',data={'property_name':'Second','address':'Different'}).status_code,409)
        self.assertEqual(self.session.execute(sa.text('SELECT address FROM bil_property WHERE id=26')).scalar(),'Original address')

    def test_edit_failed_commit_rolls_back_and_never_reports_success(self):
        from sqlalchemy.exc import SQLAlchemyError
        with patch.object(self.session,'commit',side_effect=SQLAlchemyError('synthetic failure')):
            response=self.client.post('/billing/property-owner/26/edit',data={'property_name':'Changed','address':'Changed'})
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.session.execute(sa.text('SELECT name,address FROM bil_property WHERE id=26')).one(),('Almond','Original address'))
        with self.client.session_transaction() as session:self.assertFalse(any(category=='success' for category,message in session.get('_flashes',[])))

    def test_owner_preview_fields_and_save_gate_without_owner_tables(self):
        for path in ('/billing/property-owner','/billing/property-owner/26/edit'):
            html=self.client.get(path).get_data(as_text=True)
            for label in ('Register New Owner','Select Existing Owner','ID / registration number','Residential / registered address','Postal address','Ownership percentage','Company','Trust'):
                self.assertIn(label,html)
            self.assertIn('disabled id="owner-save-disabled"',html)
            self.assertIn('>Save Owner</button>',html)
            self.assertIn('Owner registration is not yet available.',html)
            self.assertIn('Postal address same as residential / registered address',html)
            self.assertNotIn('Save Owner &amp; Assignment',html)
            self.assertIn('Nothing has been saved or assigned.',html)
            self.assertNotIn('action="/billing/owner',html)
        self.assertEqual(self.count(),2)
        self.assertEqual(set(sa.inspect(self.conn).get_temp_table_names()),{'bil_property'})

if __name__=='__main__': unittest.main()
