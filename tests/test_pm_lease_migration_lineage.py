"""Offline Alembic graph and PostgreSQL DDL compilation; no database connections."""
import hashlib
import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch
from alembic.script import ScriptDirectory
from alembic.migration import MigrationContext
from alembic.operations import Operations

ROOT=Path(__file__).resolve().parents[1]
PRODUCTION_HEADS=('home_sace_003','pm_owner_002','reading_sace_002','reading_sace_audit_001','uip_p57')

class LeaseMigrationLineageTests(unittest.TestCase):
    def test_actual_graph_resolves_historical_and_lease_revisions(self):
        scripts=ScriptDirectory(str(ROOT/'migrations'))
        actual={r.revision for r in scripts.walk_revisions()}
        self.assertTrue({'pm_owner_001','pm_owner_002','pm_lease_001','45a4f34e07dc','d3f70f6a0794'} <= actual)
        self.assertEqual(len(actual),40)

    def test_production_revision_set_resolves_and_upgrade_is_lease_only(self):
        scripts=ScriptDirectory(str(ROOT/'migrations'))
        for revision in PRODUCTION_HEADS:self.assertEqual(scripts.get_revision(revision).revision,revision)
        steps=scripts._upgrade_revs('pm_lease_001',PRODUCTION_HEADS)
        self.assertEqual([step.revision.revision for step in steps],['pm_lease_001'])
        lease=scripts.get_revision('pm_lease_001')
        self.assertIsNone(lease.down_revision)
        self.assertIsNone(lease.dependencies)
        self.assertEqual(set(scripts.get_heads()),set(PRODUCTION_HEADS)|{'pm_lease_001'})

    def test_restored_ownership_revisions_are_exact_original_bytes(self):
        original_hashes = {
            'pm_owner_001':'d980172ac79b00ecac1af2e6aeb29e6c317cd5a7d55ebae97355ae6bfbe4b259',
            'pm_owner_002':'1833e1f853a991f7844b0beaf7c6165ccb90163f2a691ca71904c248b3e379eb',
        }
        for revision, digest in original_hashes.items():
            self.assertEqual(hashlib.sha256((ROOT/f'migrations/versions/{revision}.py').read_bytes().replace(b'\r\n',b'\n')).hexdigest(),digest)

    def test_no_preexisting_migration_changed(self):
        tracked=subprocess.run(['git','ls-tree','-r','--name-only','HEAD','migrations/versions'],cwd=ROOT,check=True,capture_output=True,text=True).stdout.splitlines()
        for relative in tracked:
            original=subprocess.run(['git','show',f'HEAD:{relative}'],cwd=ROOT,check=True,capture_output=True).stdout
            self.assertEqual((ROOT/relative).read_bytes().replace(b'\r\n',b'\n'),original.replace(b'\r\n',b'\n'),relative)

    def test_postgres_ddl_contains_only_two_new_tables_and_indexes(self):
        path=ROOT/'migrations/versions/pm_lease_001.py'
        self.assertEqual(path.read_bytes(),(ROOT/'docs/proposed_migrations/pm_lease_001.py').read_bytes())
        spec=importlib.util.spec_from_file_location('offline_lease_revision',path)
        migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
        output=io.StringIO()
        context=MigrationContext.configure(dialect_name='postgresql',opts={'as_sql':True,'output_buffer':output})
        inspector=MagicMock()
        inspector.has_table.side_effect=lambda table:table in ('user','bil_property')
        inspector.get_columns.side_effect=lambda table:[{'name':n} for n in (['id'] if table=='user' else ['id','manager_id'])]
        migration.op=Operations(context)
        with patch.object(migration.sa,'inspect',return_value=inspector):migration.upgrade()
        sql=output.getvalue()
        import re
        self.assertEqual(re.findall(r'CREATE TABLE (\w+)',sql),['bil_lease_draft','bil_lease_document'])
        self.assertEqual(re.findall(r'CREATE INDEX (\w+)',sql),['ix_pm_lease_draft_scope','ix_pm_lease_document_draft'])
        for forbidden in ['ALTER TABLE','DROP TABLE','UPDATE ','INSERT INTO','DELETE FROM','CREATE OR REPLACE','CREATE TRIGGER']:
            self.assertNotIn(forbidden,sql)
        self.assertIn('items JSONB',sql)
        self.assertIn('uq_pm_lease_registration',sql)
        self.assertIn('uq_pm_lease_draft_scope',sql)
        self.assertIn('REFERENCES bil_property (id) ON DELETE RESTRICT',sql)
        self.assertIn('REFERENCES "user" (id) ON DELETE RESTRICT',sql)
        with self.assertRaises(RuntimeError):migration.downgrade()

    def test_version_transition_preserves_owner_and_other_program_heads(self):
        from alembic.runtime.migration import HeadMaintainer
        scripts=ScriptDirectory(str(ROOT/'migrations'))
        steps=scripts._upgrade_revs('pm_lease_001',PRODUCTION_HEADS)
        output=io.StringIO()
        context=MigrationContext.configure(dialect_name='postgresql',opts={'as_sql':True,'output_buffer':output})
        maintainer=HeadMaintainer(context,set(PRODUCTION_HEADS))
        for step in steps:maintainer.update_to_step(step)
        self.assertEqual(maintainer.heads,set(PRODUCTION_HEADS)|{'pm_lease_001'})
        self.assertIn('INSERT INTO alembic_version',output.getvalue())
        self.assertNotIn('UPDATE alembic_version',output.getvalue())
        self.assertNotIn('DELETE FROM alembic_version',output.getvalue())
