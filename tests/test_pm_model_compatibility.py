"""PM model/tenant compatibility tests without a factory or database."""
import ast
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
from builtins import property as python_property

from flask import Flask
from flask_login import LoginManager, UserMixin, login_user
from sqlalchemy import Boolean, Column, Date, Float, ForeignKey, Integer, String, Text, Table
from sqlalchemy.orm import DeclarativeBase
from werkzeug.exceptions import BadRequest, Forbidden

ROOT = Path(__file__).resolve().parents[1]


class CompatibilityTests(unittest.TestCase):
    def setUp(self):
        class Base(DeclarativeBase):
            pass
        db = types.SimpleNamespace(Model=Base, Column=Column, Integer=Integer, String=String,
            Text=Text, Float=Float, Boolean=Boolean, Date=Date, ForeignKey=ForeignKey,
            relationship=lambda *args, **kwargs: None)
        for name in ('user', 'user_enrollment', 'bil_bank_detail', 'bil_meter', 'ref_muni_owner'):
            Table(name, Base.metadata, Column('id', Integer, primary_key=True))
        tree = ast.parse((ROOT / 'app/models/billing.py').read_text(encoding='utf-8-sig'))
        names = {'BilProperty', 'BilSectionalUnit', 'BilTenant', 'BilMuniAccount'}
        nodes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name in names]
        self.models = dict(db=db, relationship=db.relationship, python_property=python_property)
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'actual-pm-models', 'exec'), self.models)
        modules = {name: types.ModuleType(name) for name in ('app', 'app.models', 'app.models.billing')}
        modules['app.models.billing'].BilTenant = self.models['BilTenant']
        self.module_patch = patch.dict(sys.modules, modules)
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)
        spec = importlib.util.spec_from_file_location('pm_tenant_creation_isolated', ROOT / 'app/program_billing/tenant_creation.py')
        self.helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.helper)
        self.app = Flask('pm-model-compatibility')
        self.app.secret_key = 'synthetic-only'
        LoginManager(self.app)
        self.context = self.app.test_request_context()
        self.context.push()
        self.addCleanup(self.context.pop)
        class Identity(UserMixin):
            id = 644
        login_user(Identity())

    def test_model_matches_preserved_relationships_and_unknown_activity(self):
        Tenant = self.models['BilTenant']
        self.assertTrue(Tenant.__table__.c.is_active.nullable)
        self.assertIsNone(Tenant.__table__.c.is_active.default)
        self.assertTrue(Tenant.__table__.c.property_id.nullable)
        self.assertFalse(Tenant.__table__.c.sectional_unit_id.nullable)
        self.assertEqual(next(iter(Tenant.__table__.c.sectional_unit_id.foreign_keys)).target_fullname, 'bil_sectional_unit.id')
        self.assertEqual(next(iter(self.models['BilMuniAccount'].__table__.c.property_id.foreign_keys)).ondelete, 'RESTRICT')
        for value, label, active in ((None, 'Unknown', False), (False, 'Inactive', False), (True, 'Active', True)):
            tenant = Tenant(name='Synthetic tenant', sectional_unit_id=70, is_active=value)
            self.assertEqual(tenant.activity_status, label)
            self.assertEqual(tenant.is_confirmed_active, active)
            self.assertIsNone(tenant.property_id)
            self.assertIs(tenant.is_active, value)

    def test_new_tenant_requires_explicit_matching_unit_and_manager(self):
        prop = types.SimpleNamespace(id=26, manager_id=644)
        unit = types.SimpleNamespace(id=70, property_id=26)
        tenant = self.helper.tenant_for_unit(prop, unit, name='Synthetic new tenant')
        self.assertEqual((tenant.property_id, tenant.sectional_unit_id), (26, 70))
        self.assertIsNone(tenant.is_active)
        with self.assertRaises(Forbidden):
            self.helper.tenant_for_unit(types.SimpleNamespace(id=26, manager_id=999), unit, name='Denied')
        for invalid in (types.SimpleNamespace(id=None, property_id=26), types.SimpleNamespace(id=71, property_id=99)):
            with self.assertRaises(BadRequest):
                self.helper.tenant_for_unit(prop, invalid, name='Denied')
        for key in ('property_id', 'sectional_unit_id', 'is_active'):
            with self.assertRaises(BadRequest):
                self.helper.tenant_for_unit(prop, unit, name='Denied', **{key: 1})

    def test_actual_onboarding_creation_block_sets_units_and_root_property(self):
        tree = ast.parse((ROOT / 'app/program_billing/routes.py').read_text(encoding='utf-8'))
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'ai_onboarding_process')
        body = next(n for n in function.body if isinstance(n, ast.Try)).body
        start = next(i for i, n in enumerate(body) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'unit_map' for t in n.targets))
        end = next(i for i, n in enumerate(body) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'month' for t in n.targets))
        records = []
        class Session:
            def add(self, record):
                records.append(record)
            def flush(self):
                for index, record in enumerate(records, 100):
                    if getattr(record, 'id', None) is None:
                        record.id = index
        namespace = dict(self.models, db=types.SimpleNamespace(session=Session()),
            prop=types.SimpleNamespace(id=26, manager_id=644, name='Synthetic property'),
            data={'tenants': [{'id': 't1', 'name': 'Synthetic tenant', 'rent': 100}]},
            tenant_for_unit=self.helper.tenant_for_unit,
            BilLease=lambda **kwargs: types.SimpleNamespace(**kwargs))
        exec(compile(ast.Module(body=body[start:end], type_ignores=[]), 'actual-onboarding-creation', 'exec'), namespace)
        tenants = [record for record in records if isinstance(record, self.models['BilTenant'])]
        units = [record for record in records if isinstance(record, self.models['BilSectionalUnit'])]
        self.assertEqual(len(tenants), 2)
        self.assertEqual(len(units), 2)
        for tenant, unit in zip(tenants, units):
            self.assertEqual(tenant.sectional_unit_id, unit.id)
            self.assertEqual(tenant.property_id, 26)
            self.assertEqual(unit.property_id, 26)
            self.assertIsNone(tenant.is_active)
        self.assertEqual(namespace['unit_map'], {'owner': 26, 't1': 26})

    def test_setup_uses_legacy_unit_and_checked_tenant_constructor(self):
        tree = ast.parse((ROOT / 'app/program_billing/routes.py').read_text(encoding='utf-8'))
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'setup_submit')
        body = next(n for n in function.body if isinstance(n, ast.Try)).body
        unit = next(n for n in body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'unit' for t in n.targets))
        self.assertEqual(unit.value.func.id, 'BilSectionalUnit')
        creation = next(n for n in ast.walk(function) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'tenant_for_unit')
        self.assertEqual([arg.id for arg in creation.args], ['prop', 'unit'])
        self.assertNotIn('property_id', [keyword.arg for keyword in creation.keywords])


if __name__ == '__main__':
    unittest.main(verbosity=2)
