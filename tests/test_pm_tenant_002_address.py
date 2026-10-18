"""Migration correction only: exact reported production columns, no live access."""
import unittest, importlib.util
from pathlib import Path
from alembic.operations import Operations
from alembic.migration import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
import test_pm_parties_tenant_register as fixture

class TenantAddressMigrationTests(unittest.TestCase):
    load_lease_workspace=True
    setUp=fixture.TenantRegisterTests.setUp
    sign_in=fixture.TenantRegisterTests.sign_in
    sql=fixture.TenantRegisterTests.sql
    payload=fixture.TenantRegisterTests.payload
    assign=fixture.TenantRegisterTests.assign
    create=fixture.TenantRegisterTests.create

    def foundation(self,address=None):
        self.sql('CREATE TEMP TABLE bil_sectional_unit(id integer PRIMARY KEY,name varchar(120),property_id integer REFERENCES bil_property(id))')
        self.sql('CREATE TEMP TABLE bil_bank_detail(id integer PRIMARY KEY)')
        self.sql("CREATE TEMP TABLE bil_tenant (id serial PRIMARY KEY,name varchar(200) NOT NULL,sectional_unit_id integer NOT NULL REFERENCES bil_sectional_unit(id),metro_account_no varchar(64),rent_includes_metro integer NOT NULL,email varchar(255),phone varchar(50),notes text,bank_detail_id integer REFERENCES bil_bank_detail(id),email_statements boolean NOT NULL)")
        if address:self.sql('ALTER TABLE bil_tenant ADD COLUMN address '+address)
        self.session.commit()
        for name in ('pm_tenant_001','pm_tenant_002'):
            spec=importlib.util.spec_from_file_location(name,Path('migrations/versions')/(name+'.py'))
            migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
            migration.op=Operations(MigrationContext.configure(self.session.connection()))
            if name=='pm_tenant_001':
                migration.upgrade();self.sql("INSERT INTO alembic_version VALUES ('pm_tenant_001')");self.session.commit()
            else:self.migration=migration
        self.before_columns={c['name']:c for c in inspect(self.session.connection()).get_columns('bil_tenant')}
        self.before_fks=inspect(self.session.connection()).get_foreign_keys('bil_tenant')

    def test_exact_production_columns_add_address_keep_unit_and_billing_fks(self):
        self.foundation();self.assertEqual(len(self.before_columns),10)
        self.migration.upgrade();self.session.commit()
        after={c['name']:c for c in inspect(self.session.connection()).get_columns('bil_tenant')}
        self.assertEqual(len(after),15)
        self.assertEqual(after['address']['type'].length,500)
        self.assertTrue(after['sectional_unit_id']['nullable']);self.assertTrue(after['property_id']['nullable'])
        for name,old in self.before_columns.items():
            self.assertEqual(str(after[name]['type']),str(old['type']))
            if name!='sectional_unit_id':self.assertEqual(after[name]['nullable'],old['nullable'])
            self.assertEqual(after[name]['default'],old['default'])
        for fk in self.before_fks:self.assertIn(fk,inspect(self.session.connection()).get_foreign_keys('bil_tenant'))
        self.sql("INSERT INTO bil_tenant(name,manager_id,party_type,identity_number,address,rent_includes_metro,email_statements) VALUES ('Unassigned',7,'natural_person','STANDALONE','Synthetic address',0,false)")
        self.session.commit()
        self.assertEqual(tuple(self.sql('SELECT property_id,sectional_unit_id FROM bil_tenant').one()),(None,None))

    def test_compatible_existing_address_preserved(self):
        self.foundation('varchar(500)');self.migration.upgrade();self.session.commit()
        columns=inspect(self.session.connection()).get_columns('bil_tenant')
        self.assertEqual(sum(c['name']=='address' for c in columns),1)
        self.assertEqual(next(c for c in columns if c['name']=='address')['type'].length,500)

    def test_incompatible_existing_address_aborts_without_partial_schema(self):
        self.foundation('varchar(100)')
        with self.assertRaises(SQLAlchemyError):self.migration.upgrade()
        self.session.rollback()
        columns=inspect(self.session.connection()).get_columns('bil_tenant')
        self.assertNotIn('manager_id',{c['name'] for c in columns})
        self.assertEqual(next(c for c in columns if c['name']=='address')['type'].length,100)
