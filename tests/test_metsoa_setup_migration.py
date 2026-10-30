"""Reported production drift reproduced only in session-local PostgreSQL tables."""
import importlib.util
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import text, inspect
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import ProgrammingError, IntegrityError
import test_pm_metsoa_setup_safety as safety

class MetsoaMigrationTests(safety.MetsoaSetupProtectionTests):
    def setUp(self):
        super().setUp()
        # LIKE INCLUDING CONSTRAINTS omits source primary-key indexes.
        self.session.execute(text('ALTER TABLE bil_meter ADD PRIMARY KEY(id)'))
        self.session.execute(text('ALTER TABLE bil_muni_account ADD PRIMARY KEY(id)'))
        self.session.commit()

    def migration(self):
        spec=importlib.util.spec_from_file_location('metsoa_migration',Path('migrations/versions/metsoa_setup_001.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with Operations.context(MigrationContext.configure(self.conn)):
            module.upgrade()
        self.session.commit()
        return module

    def test_reported_missing_property_column_reconciles_without_assigning_accounts(self):
        before=self.snapshot()
        self.session.execute(text('ALTER TABLE bil_muni_account DROP COLUMN property_id'))
        self.session.commit()
        with self.assertRaises(ProgrammingError):
            with self.session.begin_nested():
                self.client.get('/billing/setup?property_id=26',headers=self.headers)
        self.migration()
        self.assertEqual(self.session.execute(text('SELECT property_id FROM bil_muni_account WHERE id=1')).scalar(),None)
        for table,rows in before.items():
            after=self.snapshot()[table]
            if table=='bil_muni_account':
                self.assertEqual([{k:v for k,v in r.items() if k!='property_id'} for r in rows], [{k:v for k,v in r.items() if k!='property_id'} for r in after])
            else:self.assertEqual(rows,after)
        page=self.client.get('/billing/setup?property_id=26',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        snapshot=self.snapshot();self.migration();self.assertEqual(snapshot,self.snapshot())
        # The new FK rejects invented associations, without changing the existing NULL.
        with self.assertRaises(IntegrityError):
            with self.session.begin_nested():
                self.session.execute(text('UPDATE bil_muni_account SET property_id=99999 WHERE id=1'))

    def test_other_missing_optional_account_and_meter_fields_are_additive(self):
        self.session.execute(text('ALTER TABLE bil_muni_account DROP COLUMN ca_contract_number'))
        self.session.execute(text('ALTER TABLE bil_meter DROP COLUMN replacement_for_meter_id'))
        self.session.commit()
        before=self.snapshot();module=self.migration()
        for table,columns in module.COLUMNS.items():
            self.assertTrue({name for name,kind in columns}<={c['name'] for c in inspect(self.conn).get_columns(table)})
        after=self.snapshot()
        for table,rows in before.items():
            self.assertEqual([dict(r) for r in rows],[{k:r[k] for k in rows[0]} for r in after[table]] if rows else [])
        page=self.client.get('/billing/setup?property_id=26',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text)
        self.assertIn('MET-26',page.text)
        self.assertIn('view-only',page.text)

    def test_missing_prerequisite_aborts_without_partial_additions(self):
        self.session.execute(text('ALTER TABLE bil_muni_account DROP COLUMN property_id'))
        self.session.execute(text('ALTER TABLE bil_architecture_draft DROP COLUMN updated_at'))
        self.session.commit()
        with self.assertRaisesRegex(RuntimeError,'bil_architecture_draft'):
            self.migration()
        self.assertNotIn('property_id',{c['name'] for c in inspect(self.conn).get_columns('bil_muni_account')})
