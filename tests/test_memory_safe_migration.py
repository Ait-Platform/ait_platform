"""Real Alembic multi-head upgrades on disposable session-temporary PostgreSQL objects."""
import ast
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys
import unittest
import importlib.util
import runpy
import os
import io
from contextlib import redirect_stdout
from types import ModuleType, SimpleNamespace
from unittest.mock import patch
from contextlib import nullcontext

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from dotenv import dotenv_values
import sqlalchemy as sa
from sqlalchemy.engine import make_url

ROOT=Path(__file__).resolve().parents[1]
PARENT='protrade_trade_codes_019'
TARGET='ait_franchisee_001'


class MemorySafeMigrationTests(unittest.TestCase):
    def setUp(self):
        url=make_url(dotenv_values(ROOT/'.env')['DATABASE_URL'].replace('postgres://','postgresql://',1))
        self.assertEqual(url.database,'ait_local_db')
        self.assertIn(url.host,('localhost','127.0.0.1','::1'))
        self.assertFalse(url.query)
        self.engine=sa.create_engine(url,connect_args={'connect_timeout':5})
        self.addCleanup(self.engine.dispose)
        self.conn=self.engine.connect();self.addCleanup(self.conn.close)
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY)')
        self.conn.exec_driver_sql('INSERT INTO "user" VALUES (1),(2)')
        self.conn.exec_driver_sql('CREATE TEMP TABLE preserved_business(id integer PRIMARY KEY,balance integer)')
        self.conn.exec_driver_sql('INSERT INTO preserved_business VALUES (1,12500)')
        self.conn.exec_driver_sql('SET search_path TO pg_temp')
        self.config=Config(str(ROOT/'migrations/alembic.ini'))
        self.config.attributes.update(connection=self.conn,skip_app_metadata=True)
        self.script=ScriptDirectory.from_config(self.config)
        self.initial=(set(self.script.get_heads())-{TARGET})|{PARENT}
        # Fixture baseline only: framework-managed stamp in disposable temp schema.
        # Never used as a production migration procedure.
        command.stamp(self.config,list(self.initial))
        self.conn.commit()
        self.assertEqual(self.persistence('alembic_version'),'t')
        self.assertEqual(self.persistence('user'),'t')

    def persistence(self,name):
        return self.conn.execute(sa.text('SELECT relpersistence FROM pg_class WHERE oid=to_regclass(:name)'),{'name':name}).scalar()

    def heads(self):
        return set(self.conn.execute(sa.text('SELECT version_num FROM alembic_version')).scalars())

    @contextmanager
    def transaction(self):
        # End any observation-only transaction before the actual migration.
        self.conn.rollback()
        with self.conn.begin():
            self.conn.exec_driver_sql("SET LOCAL lock_timeout='5s'")
            self.conn.exec_driver_sql("SET LOCAL statement_timeout='60s'")
            yield

    def test_ini_and_parent_resolve_without_path_override(self):
        self.assertEqual(Path(self.config.get_main_option('script_location')).resolve(),ROOT/'migrations')
        self.assertEqual(self.script.get_revision(TARGET).down_revision,PARENT)
        self.assertGreater(len(self.initial),1)

    def test_target_upgrade_preserves_other_branches_and_existing_rows(self):
        self.config.attributes['franchisee_registration_approved']=True
        with self.transaction():command.upgrade(self.config,TARGET)
        self.assertEqual(self.heads(),(self.initial-{PARENT})|{TARGET})
        for name in ['ait_franchisee_registration','ait_franchisee_number_seq']:
            self.assertEqual(self.persistence(name),'t')
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM "user" ORDER BY id').all(),[(1,),(2,)])
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM preserved_business').all(),[(1,12500)])
        self.assertEqual(self.conn.exec_driver_sql('SELECT count(*) FROM ait_franchisee_registration').scalar(),0)

    def test_approval_failure_leaves_schema_and_all_revision_rows_unchanged(self):
        with self.assertRaisesRegex(RuntimeError,'explicitly approved'),self.transaction():
            command.upgrade(self.config,TARGET)
        self.assertEqual(self.heads(),self.initial)
        self.assertIsNone(self.persistence('ait_franchisee_registration'))
        self.assertIsNone(self.persistence('ait_franchisee_number_seq'))

    def test_failure_at_version_update_rolls_back_both_ddl_and_version(self):
        self.config.attributes['franchisee_registration_approved']=True
        reached=[]
        def fail(conn,cursor,statement,parameters,context,many):
            if 'alembic_version' in statement and statement.lstrip().upper().startswith('UPDATE') and TARGET in statement:
                reached.append(True)
                raise RuntimeError('Synthetic failure before Alembic version update')
        sa.event.listen(self.conn,'before_cursor_execute',fail)
        try:
            with self.assertRaisesRegex(RuntimeError,'Synthetic failure'),self.transaction():
                command.upgrade(self.config,TARGET)
        finally:sa.event.remove(self.conn,'before_cursor_execute',fail)
        self.assertTrue(reached)
        self.assertEqual(self.heads(),self.initial)
        self.assertIsNone(self.persistence('ait_franchisee_registration'))
        self.assertIsNone(self.persistence('ait_franchisee_number_seq'))
        self.assertEqual(self.conn.exec_driver_sql('SELECT * FROM preserved_business').all(),[(1,12500)])

    def test_repeat_target_is_a_framework_noop(self):
        self.config.attributes['franchisee_registration_approved']=True
        with self.transaction():command.upgrade(self.config,TARGET)
        expected=self.heads()
        self.config.attributes.pop('franchisee_registration_approved')
        with self.transaction():command.upgrade(self.config,TARGET)
        self.assertEqual(self.heads(),expected)
        self.assertEqual(self.conn.exec_driver_sql('SELECT last_value,is_called FROM ait_franchisee_number_seq').one(),(1,False))

    def test_lightweight_current_does_not_import_application_in_fresh_process(self):
        result=subprocess.run([sys.executable,str(ROOT/'scripts/measure_startup_memory.py'),'alembic','--lightweight'],
                              cwd=ROOT,capture_output=True,text=True,timeout=60)
        self.assertEqual(result.returncode,0,result.stderr)
        data=json.loads(result.stdout)
        self.assertFalse(data['application_imported'])
        self.assertEqual(data['blocked_sql_count'],0)
        self.assertFalse(any(data['heavy_imports'].values()))

    def test_web_entrypoints_use_existing_operator_factory(self):
        for path in [ROOT/'wsgi.py',ROOT/'app/wsgi.py']:
            tree=ast.parse(path.read_text(encoding='utf-8-sig'))
            calls=[node for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='create_app']
            self.assertEqual(len(calls),1)
            self.assertTrue(any(item.arg=='operator_mode' and isinstance(item.value,ast.Constant) and item.value.value is True for item in calls[0].keywords))

    def test_default_metadata_mode_remains_available(self):
        from alembic import context
        metadata=sa.MetaData();sa.Table('metadata_sentinel',metadata,sa.Column('id',sa.Integer,primary_key=True))
        package=ModuleType('app');package.__path__=[]
        extensions=ModuleType('app.extensions');extensions.db=SimpleNamespace(metadata=metadata)
        models=ModuleType('app.models')
        calls=[];config=Config();config.attributes['connection']=self.conn
        with patch.dict(sys.modules,{'app':package,'app.extensions':extensions,'app.models':models}), \
             patch.object(context,'config',config,create=True),patch.object(context,'is_offline_mode',lambda:False), \
             patch.object(context,'configure',lambda **kwargs:calls.append(kwargs)), \
             patch.object(context,'begin_transaction',nullcontext),patch.object(context,'run_migrations',lambda:None):
            runpy.run_path(str(ROOT/'migrations/env.py'))
        self.assertIs(calls[0]['target_metadata'],metadata)

    def test_franchisee_setting_check_does_not_print_secret_values(self):
        from cryptography.fernet import Fernet
        spec=importlib.util.spec_from_file_location('safe_franchisee_settings',ROOT/'scripts/verify_franchisee_environment.py')
        helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
        settings={'FRANCHISEE_PII_KEY':Fernet.generate_key().decode(),'FRANCHISEE_PUBLIC_ORIGIN':'https://ait.example.org'}
        self.assertTrue(all(helper.verify(settings).values()))
        for origin in ['', 'http://ait.example.org','https://ait.example.org/path','https://user:password@ait.example.org','https://ait.example.org:99999']:
            self.assertFalse(helper.verify(dict(settings,FRANCHISEE_PUBLIC_ORIGIN=origin))['FRANCHISEE_PUBLIC_ORIGIN_valid'])
        self.assertFalse(helper.verify(dict(settings,FRANCHISEE_PII_KEY='invalid'))['FRANCHISEE_PII_KEY_valid'])
        self.assertNotIn(settings['FRANCHISEE_PII_KEY'],json.dumps(helper.verify(settings)))

    def run_predeploy_script(self):
        document=(ROOT/'docs/render_memory_franchisee_migration.md').read_text(encoding='utf-8')
        block=document.split('<!-- MIGRATION_COMMAND -->',1)[1].split('<!-- END_MIGRATION_COMMAND -->',1)[0]
        self.assertEqual(block.strip(), "```sh\npython scripts/verify_franchisee_environment.py && python scripts/apply_franchisee_migration.py\n```")
        self.conn.rollback()
        @contextmanager
        def begin():
            with self.conn.begin():yield self.conn
        engine=SimpleNamespace(begin=begin,dispose=lambda:None)
        output=io.StringIO()
        with patch.dict(os.environ,{'DATABASE_URL':dotenv_values(ROOT/'.env')['DATABASE_URL']}), \
             patch('sqlalchemy.create_engine',return_value=engine),redirect_stdout(output):
            runpy.run_path(str(ROOT/'scripts/apply_franchisee_migration.py'),run_name='__main__')
        return output.getvalue()

    def test_documented_predeploy_command_on_temporary_fixture(self):
        self.assertIn('COMMITTED',self.run_predeploy_script())
        self.assertEqual(self.heads(),(self.initial-{PARENT})|{TARGET})
        self.assertEqual(self.persistence('ait_franchisee_registration'),'t')
        self.assertIn('COMMITTED',self.run_predeploy_script())
        self.assertEqual(self.heads(),(self.initial-{PARENT})|{TARGET})

    def test_predeploy_script_failure_rolls_back_without_exposing_error_details(self):
        from contextlib import redirect_stderr
        error=io.StringIO()
        with patch('alembic.command.upgrade',side_effect=RuntimeError('sensitive-details')), redirect_stderr(error), self.assertRaises(SystemExit) as result:
            self.run_predeploy_script()
        self.assertEqual(result.exception.code,1)
        self.assertIn('FAILED during Alembic upgrade: RuntimeError',error.getvalue())
        self.assertNotIn('sensitive-details',error.getvalue())
        self.assertEqual(self.heads(),self.initial)
        self.assertIsNone(self.persistence('ait_franchisee_registration'))


if __name__=='__main__':unittest.main()
