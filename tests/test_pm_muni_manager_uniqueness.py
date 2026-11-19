"""Focused manager uniqueness and legacy-preservation tests on temporary PostgreSQL."""
import importlib.util
import os
from pathlib import Path
from unittest.mock import patch

from alembic.config import Config
from alembic.operations import Operations
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from dotenv import dotenv_values

import test_pm_municipal_accounts as fixture

ROOT = Path(__file__).resolve().parents[1]


class ManagerUniquenessTests(fixture.MunicipalAccountsTests):
    def setUp(self):
        # The inherited fixture's historical fallback points outside this checkout.
        # Supply the current local configuration; its localhost/database guards remain.
        raw = os.environ.get('PM_LEASE_TEST_DATABASE_URL') or dotenv_values(ROOT/'.env').get('DATABASE_URL', '')
        with patch.dict(os.environ, {'PM_LEASE_TEST_DATABASE_URL': raw}):
            super().setUp()
        spec = importlib.util.spec_from_file_location('manager_unique_migration', ROOT/'migrations/versions/pm_muni_manager_unique_001.py')
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)

    def legacy_constraint(self):
        self.session.execute(text('ALTER TABLE bil_muni_account ADD CONSTRAINT bil_muni_account_account_number_key UNIQUE(account_number)'))
        self.session.commit()
        self.conn.commit()

    def unassigned_account(self):
        self.session.execute(text("INSERT INTO ref_muni_owner(id,name) VALUES(3,'Legacy owner') ON CONFLICT DO NOTHING"))
        self.session.execute(text("UPDATE bil_muni_account SET property_id=NULL,account_number='70134280012',owner_id=3,water_meter_id=NULL,elec_meter_id=NULL WHERE id=1"))
        self.session.execute(text('CREATE TEMP TABLE retained_account_history(id integer PRIMARY KEY,account_id integer,description text)'))
        self.session.execute(text("INSERT INTO retained_account_history VALUES(1,1,'Retain original account history')"))
        self.session.commit()
        self.conn.commit()

    def migrate(self, approved=True, fail_drop=False):
        self.session.commit()
        self.conn.commit()
        config = Config()
        config.attributes['pm_muni_manager_unique_approved'] = approved
        with self.conn.begin(), EnvironmentContext(config, ScriptDirectory(str(ROOT/'migrations'))) as environment:
            environment.configure(connection=self.conn)
            operations = Operations(environment.get_context())
            with patch.object(self.migration, 'op', operations):
                if fail_drop:
                    with patch.object(operations, 'drop_constraint', side_effect=RuntimeError('Synthetic DDL failure')):
                        self.migration.upgrade()
                else:
                    self.migration.upgrade()

    def test_manager_scope_legacy_collision_is_409_and_keeps_values(self):
        self.unassigned_account()
        self.legacy_constraint()
        before = self.snapshot()
        response = self.post(dict(account_number='70134280012',owner_name='New property owner',water_number='AXE916'))
        self.assertEqual(response.status_code,409,response.text)
        self.assertIn('legacy global database rule',response.text)
        self.assertIn('70134280012',response.text)
        self.assertIn('AXE916',response.text)
        self.assertNotIn('UniqueViolation',response.text)
        self.assertEqual(before,self.snapshot())

    def test_manager_scope_migration_preserves_unassigned_history(self):
        self.unassigned_account()
        self.legacy_constraint()
        before = self.snapshot()
        columns = [c['name'] for c in inspect(self.conn).get_columns('bil_muni_account')]
        self.migrate()
        self.assertEqual(before,self.snapshot())
        self.assertEqual(columns,[c['name'] for c in inspect(self.conn).get_columns('bil_muni_account')])
        response = self.post(dict(account_number='70134280012',owner_name='Independent owner',water_number='AXE916'))
        self.assertEqual(response.status_code,302,response.text)
        rows = self.session.execute(text("SELECT id,property_id FROM bil_muni_account WHERE account_number='70134280012' ORDER BY id")).all()
        self.assertEqual(rows[0],(1,None))
        self.assertEqual(rows[1][1],26)
        self.assertEqual(dict(self.session.execute(text('SELECT * FROM bil_muni_account WHERE id=1')).mappings().one()),before['bil_muni_account'][0])
        self.assertEqual(self.session.execute(text('SELECT * FROM retained_account_history')).all(),[(1,1,'Retain original account history')])
        self.assertEqual(before['bil_meter_reading'],self.snapshot()['bil_meter_reading'])
        self.assertEqual(before['bil_consumption'],self.snapshot()['bil_consumption'])

    def test_manager_scope_lookup_duplicates_edits_and_separate_managers(self):
        self.legacy_constraint()
        self.migrate()
        created = self.post(dict(account_number='SHARED-ACCOUNT',owner_name='Owner',water_number='OWN-WATER'))
        self.assertEqual(created.status_code,302,created.text)
        lookup = self.lookup('SHARED-ACCOUNT').json
        self.assertTrue(lookup['found'])
        self.assertEqual(lookup['association'],'selected_property')
        before = self.snapshot()
        duplicate = self.post(dict(account_number=' shared-account ',owner_name='Owner'))
        self.assertEqual(duplicate.status_code,409)
        self.assertIn('already exists',duplicate.text)
        self.assertEqual(before,self.snapshot())
        other_property = self.client.get('/billing/utilities/27/accounts/lookup?account_number=SHARED-ACCOUNT',headers=self.headers)
        self.assertEqual(other_property.status_code,200)
        self.assertEqual(other_property.json['association'],'other_property')
        self.assertEqual(self.post(dict(account_number='SHARED-ACCOUNT',owner_name='Owner'),url='/billing/utilities/27/accounts').status_code,409)
        self.session.execute(text("INSERT INTO bil_muni_account(property_id,account_number) VALUES(28,'SHARED-ACCOUNT')"))
        self.session.commit()
        self.conn.commit()
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_muni_account WHERE account_number='SHARED-ACCOUNT'")).scalar(),2)
        edit = dict(lookup['values'],account_id=lookup['account_id'],version=lookup['version'])
        edit['owner_name']='Corrected registered owner'
        self.assertEqual(self.post(edit).status_code,302)
        self.assertEqual(self.client.get(self.accounts_url+'?edit='+str(lookup['account_id']),headers=self.headers).status_code,200)
        foreign = self.session.execute(text("SELECT id FROM bil_muni_account WHERE property_id=28 AND account_number='SHARED-ACCOUNT'")).scalar()
        self.assertEqual(self.client.get(self.accounts_url+'?edit='+str(foreign),headers=self.headers).status_code,404)
        self.assertEqual(self.post(dict(account_number='SHARED-ACCOUNT',account_id=str(foreign),owner_name='Owner')).status_code,404)

    def test_manager_scope_database_rejects_duplicates_and_ownership_collision(self):
        self.legacy_constraint()
        self.migrate()
        self.session.execute(text("INSERT INTO bil_muni_account(property_id,account_number) VALUES(26,'DB-NUMBER'),(28,'DB-NUMBER')"))
        self.session.commit()
        self.conn.commit()
        with self.assertRaises(IntegrityError) as rejected, self.conn.begin_nested():
            self.conn.execute(text("INSERT INTO bil_muni_account(property_id,account_number) VALUES(27,' db-number ')"))
        self.assertEqual(rejected.exception.orig.diag.constraint_name,'uq_pm_muni_manager_account')
        with self.assertRaises(IntegrityError), self.conn.begin_nested():
            self.conn.execute(text('UPDATE bil_property SET manager_id=8 WHERE id=26'))
        self.assertEqual(self.conn.execute(text('SELECT manager_id FROM bil_property WHERE id=26')).scalar(),7)

    def test_manager_scope_legacy_meters_follow_verified_owner(self):
        self.legacy_constraint()
        self.migrate()
        self.session.execute(text("INSERT INTO bil_muni_account(property_id,account_number,water_meter_id) VALUES(NULL,'LINKED-LEGACY',1),(28,'LINKED-LEGACY',NULL)"))
        self.session.commit()
        self.conn.commit()
        with self.assertRaises(IntegrityError), self.conn.begin_nested():
            self.conn.execute(text("INSERT INTO bil_muni_account(property_id,account_number) VALUES(27,'LINKED-LEGACY')"))
        with self.assertRaises(IntegrityError), self.conn.begin_nested():
            self.conn.execute(text('UPDATE bil_meter SET property_id=28 WHERE id=1'))
        self.assertEqual(self.conn.execute(text('SELECT property_id FROM bil_meter WHERE id=1')).scalar(),26)

    def test_manager_scope_unverified_legacy_cannot_be_claimed(self):
        self.unassigned_account()
        self.legacy_constraint()
        self.migrate()
        self.assertFalse(self.lookup('70134280012').json['found'])
        before = self.snapshot()
        self.assertEqual(self.post(dict(account_id='1',account_number='70134280012',owner_name='Owner')).status_code,404)
        self.assertEqual(before,self.snapshot())

    def test_manager_scope_conflicts_abort_without_dropping_global_rule(self):
        self.legacy_constraint()
        self.session.execute(text("INSERT INTO bil_muni_account(property_id,account_number) VALUES(27,' met-26 ')"))
        self.session.commit()
        self.conn.commit()
        before = self.snapshot()
        with self.assertRaisesRegex(RuntimeError,'Conflicting municipal'):
            self.migrate()
        self.assertEqual(before,self.snapshot())
        names = {item['name'] for item in inspect(self.conn).get_unique_constraints('bil_muni_account')}
        self.assertIn('bil_muni_account_account_number_key',names)

    def test_manager_scope_approval_and_ddl_failure_preserve_constraint(self):
        self.legacy_constraint()
        with self.assertRaisesRegex(RuntimeError,'explicitly approved'):
            self.migrate(approved=False)
        with self.assertRaisesRegex(RuntimeError,'Synthetic DDL failure'):
            self.migrate(fail_drop=True)
        self.assertIn('bil_muni_account_account_number_key',{item['name'] for item in inspect(self.conn).get_unique_constraints('bil_muni_account')})
        self.assertIsNone(self.conn.execute(text("SELECT to_regprocedure(current_schema()||'.pm_muni_check_account_unique()')")).scalar())

    def test_manager_scope_repeatable_read_cannot_bypass_uniqueness(self):
        self.legacy_constraint()
        self.migrate()
        self.session.commit()
        self.conn.commit()
        with self.assertRaises(DBAPIError) as error, self.conn.begin():
            self.conn.exec_driver_sql('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            self.conn.execute(text("INSERT INTO bil_muni_account(property_id,account_number) VALUES(26,'RR-ACCOUNT')"))
        self.assertEqual(error.exception.orig.pgcode,'25001')
