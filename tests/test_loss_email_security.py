"""Real PostgreSQL guards and actual route functions; PDF/SMTP mocked only."""
import ast
import importlib.util
import io
import re
from pathlib import Path
import sys
from types import SimpleNamespace, ModuleType
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from alembic.migration import MigrationContext
from alembic.operations import Operations
from dotenv import dotenv_values
from flask import Flask, Blueprint, request, current_app, send_file, url_for
from flask_login import LoginManager, UserMixin, login_required
from flask_mail import Message
from flask_wtf import CSRFProtect
from flask_wtf.csrf import generate_csrf

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('loss_email_security_under_test', ROOT/'app/subject_loss/email_security.py')
security = importlib.util.module_from_spec(spec)
spec.loader.exec_module(security)
PDF = b'%PDF-1.4\nexisting report PDF fixture\n%%EOF'


class Actor(UserMixin):
    def __init__(self, identifier):
        self.id = identifier


class LossEmailSecurityTests(unittest.TestCase):
    def setUp(self):
        url = dotenv_values(ROOT/'.env')['DATABASE_URL'].replace('postgres://','postgresql://',1)
        parsed = sa.engine.make_url(url)
        self.assertEqual(parsed.database, 'ait_local_db')
        self.assertIn(parsed.host, ('localhost','127.0.0.1','::1'))
        self.prefix = 'le_' + uuid4().hex[:16] + '_'
        self.names = ['user','auth_approved_admin','auth_subject','auth_subject_admin',
                      'lca_run','lca_result','lca_response','loss_report_email_send',
                      'ix_loss_email_owner_attempt']
        self.engine = sa.create_engine(url)
        self.addCleanup(self.cleanup_tables)
        @sa.event.listens_for(self.engine, 'before_cursor_execute', retval=True)
        def isolate_tables(conn, cursor, statement, parameters, context, executemany):
            for name in self.names:
                statement = re.sub(r'(?<![\w])'+name+r'(?![\w])', self.prefix+name, statement)
            return statement, parameters
        with self.engine.connect() as c:
            self.assertEqual(c.execute(sa.text('SELECT current_database()')).scalar(), 'ait_local_db')
        self.sessions = scoped_session(sessionmaker(bind=self.engine))
        self.addCleanup(self.sessions.remove)
        self.patch_db = patch.object(security,'db',SimpleNamespace(session=self.sessions))
        self.patch_db.start(); self.addCleanup(self.patch_db.stop)
        with self.engine.begin() as c:
            c.exec_driver_sql('CREATE TABLE "user" (id integer PRIMARY KEY, name text,email text,is_active integer NOT NULL)')
            c.exec_driver_sql('CREATE TABLE auth_approved_admin (id integer PRIMARY KEY,email text,active integer)')
            c.exec_driver_sql('CREATE TABLE auth_subject (id integer PRIMARY KEY,slug text,is_active integer)')
            c.exec_driver_sql('CREATE TABLE auth_subject_admin (id integer PRIMARY KEY,email text,subject_id integer)')
            c.exec_driver_sql('CREATE TABLE lca_run (id integer PRIMARY KEY,user_id integer,status text)')
            c.exec_driver_sql('CREATE TABLE lca_result (id integer PRIMARY KEY,run_id integer,user_id integer,total integer)')
            c.exec_driver_sql('CREATE TABLE lca_response (id integer PRIMARY KEY,run_id integer,user_id integer,answer text)')
            c.exec_driver_sql("INSERT INTO \"user\" VALUES (1,'Owner','owner@example.com',1),(2,'Other','other@example.com',1),(3,'Admin','admin@example.com',1),(4,'Subject Admin','lossadmin@example.com',1)")
            c.exec_driver_sql("INSERT INTO auth_approved_admin VALUES (1,'admin@example.com',1)")
            c.exec_driver_sql("INSERT INTO auth_subject VALUES (1,'loss',1),(2,'reading',1)")
            c.exec_driver_sql("INSERT INTO auth_subject_admin VALUES (1,'lossadmin@example.com',1)")
            c.exec_driver_sql("INSERT INTO lca_run VALUES (10,1,'completed'),(11,2,'completed'),(12,1,'completed')")
            c.exec_driver_sql("INSERT INTO lca_result VALUES (1,10,1,75),(2,11,2,60),(3,12,1,80)")
            c.exec_driver_sql("INSERT INTO lca_response VALUES (1,10,1,'yes'),(2,11,2,'no')")
            spec = importlib.util.spec_from_file_location('loss_email_migration',ROOT/'migrations/versions/loss_email_security_001.py')
            self.migration = importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
            self.migration.op = Operations(MigrationContext.configure(c));self.migration.upgrade()
        self.app = Flask(__name__)
        self.app.config.update(SECRET_KEY='local-tests-only',TESTING=True,EMAIL_ENABLED=True)
        self.app.extensions['mail']=SimpleNamespace(default_sender='ait@example.com',suppress=False)
        manager = LoginManager(self.app)
        manager.user_loader(lambda identifier: Actor(int(identifier)))
        self.messages=[]
        self.sender = Mock(side_effect=lambda **kw: self.messages.append(kw) or True)
        self.mail = SimpleNamespace(send=Mock(side_effect=lambda message: self.messages.append(message)))
        self.render = Mock(return_value=PDF)
        self.html = Mock(return_value='<html>Existing report</html>')
        self.context = Mock(side_effect=lambda rid,uid: ({'learner_name':'Owner','run_id':rid,'user_id':uid},None,None))
        fake = ModuleType('app.admin.programs.loss.routes');fake._build_context=self.context
        self.patch_context = patch.dict(sys.modules,{'app.admin.programs.loss.routes':fake})
        self.patch_context.start();self.addCleanup(self.patch_context.stop)
        bp = Blueprint('loss_bp',__name__,url_prefix='/loss')
        scope = dict(loss_bp=bp, login_required=login_required,
            controlled_email_errors=security.controlled_email_errors,
            authorised_report=security.authorised_report,guarded_send=security.guarded_send,
            EmailActionError=security.EmailActionError,request=request,current_app=current_app,
            send_file=send_file,url_for=url_for,io=io,BytesIO=io.BytesIO,Message=Message,
            os=SimpleNamespace(getenv=lambda *args:'mock-renderer'),
            pdfkit=SimpleNamespace(configuration=Mock(),from_string=self.render),
            _generate_learner_pdf_html=self.html,_render_loss_pdf_bytes=self.render,
            _send_mail=self.sender,mail=self.mail)
        tree=ast.parse((ROOT/'app/subject_loss/routes.py').read_text(encoding='utf-8'))
        functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'report_email_and_download','report_send_and_download'}]
        exec(compile(ast.Module(body=functions,type_ignores=[]),'actual_loss_email_routes','exec'),scope)
        bp.add_url_rule('/report.pdf',endpoint='report_pdf',view_func=lambda:PDF)
        self.app.register_blueprint(bp)
        @self.app.teardown_appcontext
        def remove_session(error):
            self.sessions.remove()
        self.client=self.app.test_client()
        self.login(1)

    def cleanup_tables(self):
        self.sessions.remove()
        assert re.fullmatch(r'le_[0-9a-f]{16}_', self.prefix)
        # Only this test's generated table names are resolved by the engine listener.
        with self.engine.begin() as c:
            for name in reversed(self.names[:-1]):
                c.exec_driver_sql('DROP TABLE IF EXISTS "'+name+'" CASCADE')
        self.engine.dispose()

    def login(self,identifier,client=None):
        with (client or self.client).session_transaction() as s:
            s['_user_id']=str(identifier);s['_fresh']=True

    def post(self,route=0,run=10,client=None,**data):
        path=f'/loss/report.email/{run}' if route else f'/loss/report/send?run_id={run}&user_id=999'
        return (client or self.client).post(path,data=data)

    def scalar(self,sql):
        with self.engine.connect() as c:return c.execute(sa.text(sql)).scalar()

    def sql(self,sql):
        with self.engine.begin() as c:c.execute(sa.text(sql))

    def test_anonymous_both_routes_never_render_or_send(self):
        anonymous=self.app.test_client()
        for route in (0,1):self.assertEqual(self.post(route,client=anonymous).status_code,401)
        self.render.assert_not_called();self.sender.assert_not_called();self.mail.send.assert_not_called()

    def test_foreign_unknown_and_forged_session_permissions_are_denied(self):
        with self.client.session_transaction() as s:s['is_admin']=True;s['role']='admin'
        for route in (0,1):
            for run in (11,999):
                response=self.post(route,run,email='attacker@example.com',user_id='1')
                self.assertEqual(response.status_code,404)
                self.assertEqual(response.json,{'error':'Report unavailable.'})
        self.context.assert_not_called();self.html.assert_not_called();self.render.assert_not_called()
        self.assertEqual(self.scalar('SELECT count(*) FROM loss_report_email_send'),0)

    def test_recipient_and_user_id_manipulation_ignored_and_pdf_preserved(self):
        for route in (0,1):
            if route:self.sql('DELETE FROM loss_report_email_send')
            response=self.post(route,email='attacker@example.com',to='attacker@example.com',user_id='2')
            self.assertEqual(response.status_code,200,response.data)
            self.assertEqual(response.mimetype,'application/pdf');self.assertEqual(response.data,PDF)
            self.assertIn('attachment;',response.headers['Content-Disposition'])
            message=self.messages[-1]
            self.assertEqual(message.recipients if isinstance(message,Message) else [message['to_email']],['owner@example.com'])
        self.context.assert_called_with(10,1);self.html.assert_called_with(10,1)

    def test_explicit_global_and_loss_admins_send_only_to_owner(self):
        for actor,route in ((3,0),(4,1)):
            if route:self.sql('DELETE FROM loss_report_email_send')
            self.login(actor)
            self.assertEqual(self.post(route,11,email='admin@example.com').status_code,200)
            self.assertEqual(self.scalar('SELECT owner_id FROM loss_report_email_send'),2)
            self.assertEqual(self.scalar('SELECT actor_id FROM loss_report_email_send'),actor)
            self.assertEqual(self.scalar('SELECT recipient FROM loss_report_email_send'),'other@example.com')

    def test_inactive_or_unrelated_admin_grants_are_denied(self):
        self.sql('UPDATE auth_approved_admin SET active=0');self.login(3)
        self.assertEqual(self.post(0,11).status_code,404)
        self.sql('UPDATE auth_subject_admin SET subject_id=2');self.login(4)
        self.assertEqual(self.post(1,11).status_code,404)

    def test_invalid_or_inactive_owner_email_and_inconsistent_records_fail_closed(self):
        for sql,code in [("UPDATE \"user\" SET email='invalid' WHERE id=1",400),
                         ("UPDATE \"user\" SET email='owner@example.com',is_active=0 WHERE id=1",404),
                         ("UPDATE \"user\" SET is_active=1 WHERE id=1; UPDATE lca_result SET user_id=2 WHERE run_id=10",404)]:
            self.sql(sql)
            for route in (0,1):self.assertEqual(self.post(route).status_code,code)
        self.render.assert_not_called();self.sender.assert_not_called()

    def test_duplicate_cross_route_and_owner_wide_cooldown(self):
        self.assertEqual(self.post(0).status_code,200)
        for route,run in ((0,10),(1,10),(0,12)):
            response=self.post(route,run)
            self.assertEqual(response.status_code,429)
            self.assertEqual(response.headers['Retry-After'],'600')
        self.assertEqual(len(self.messages),1)
        self.sql("UPDATE loss_report_email_send SET attempted_at=clock_timestamp()-INTERVAL '11 minutes'")
        self.assertEqual(self.post(1).status_code,200)
        self.assertEqual(len(self.messages),2)

    def test_different_owner_does_not_share_cooldown(self):
        self.assertEqual(self.post().status_code,200);self.login(2)
        self.assertEqual(self.post(1,11).status_code,200)

    def test_smtp_failure_keeps_durable_uncertain_state(self):
        self.sender.side_effect=RuntimeError('sensitive SMTP detail')
        response=self.post()
        self.assertEqual(response.status_code,503);self.assertNotIn(b'sensitive',response.data)
        self.assertEqual(self.scalar('SELECT status FROM loss_report_email_send'),'uncertain')
        self.sql("UPDATE loss_report_email_send SET attempted_at=clock_timestamp()-INTERVAL '1 day'")
        self.assertEqual(self.post(1).status_code,409)
        self.sender.assert_called_once();self.mail.send.assert_not_called()

    def test_pdf_failure_returns_controlled_error_before_reservation(self):
        self.render.side_effect=RuntimeError('sensitive PDF path')
        for route in (0,1):
            response=self.post(route);self.assertEqual(response.status_code,503)
            self.assertNotIn(b'sensitive',response.data)
        self.assertEqual(self.scalar('SELECT count(*) FROM loss_report_email_send'),0)
        self.sender.assert_not_called();self.mail.send.assert_not_called()

    def test_missing_tracking_table_prevents_sending(self):
        self.sql('DROP TABLE loss_report_email_send')
        response=self.post();self.assertEqual(response.status_code,503)
        self.sender.assert_not_called();self.assertNotIn(b'loss_report_email_send',response.data)

    def test_tracking_commit_failure_prevents_sending(self):
        session=self.sessions()
        with patch.object(session,'commit',side_effect=RuntimeError('sensitive storage failure')):
            self.assertEqual(self.post().status_code,503)
        self.sender.assert_not_called()
        self.assertEqual(self.scalar('SELECT count(*) FROM loss_report_email_send'),0)

    def test_smtp_success_then_outcome_commit_failure_blocks_retry(self):
        session=self.sessions();real_commit=session.commit;calls=[]
        def commit():
            calls.append(1)
            if len(calls)==2:raise RuntimeError('sensitive storage failure')
            return real_commit()
        with patch.object(session,'commit',side_effect=commit):
            self.assertEqual(self.post().status_code,503)
        self.assertEqual(self.scalar('SELECT status FROM loss_report_email_send'),'attempted')
        self.assertEqual(self.post(1).status_code,409);self.assertEqual(len(self.messages),1)

    def test_concurrent_cross_route_requests_send_once(self):
        entered=Event();release=Event()
        def send(**kwargs):
            entered.set();self.assertTrue(release.wait(10));self.messages.append(kwargs);return True
        self.sender.side_effect=send
        c1=self.app.test_client();c2=self.app.test_client();self.login(1,c1);self.login(1,c2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(self.post,0,10,c1)
            self.assertTrue(entered.wait(10))
            second=pool.submit(self.post,1,10,c2)
            response=second.result(timeout=10)
            self.assertEqual(response.status_code,409)
            release.set();self.assertEqual(first.result(timeout=10).status_code,200)
        self.assertEqual(len(self.messages),1)

    def test_disabled_mail_flag_preserves_authorised_pdf_download(self):
        self.app.config['EMAIL_ENABLED']=False
        self.assertEqual(self.post(1).data,PDF)
        self.mail.send.assert_not_called()
        self.assertEqual(self.scalar('SELECT count(*) FROM loss_report_email_send'),0)
        self.assertEqual(self.post(1,11).status_code,404)

    def test_csrf_rejects_missing_token_and_allows_authenticated_owner(self):
        CSRFProtect(self.app)
        @self.app.get('/test-token')
        def token():return generate_csrf()
        self.assertEqual(self.post().status_code,400)
        token=self.client.get('/test-token').get_data(as_text=True)
        self.assertEqual(self.post(csrf_token=token).status_code,200)

    def test_database_lock_contention_rejects_concurrent_reservation(self):
        with self.engine.begin() as other:
            other.execute(sa.text('SELECT pg_advisory_xact_lock(:namespace,1)'), {'namespace': security.LOCK_NAMESPACE})
            self.assertEqual(self.post().status_code,409)
            self.sender.assert_not_called()
        self.assertEqual(self.post().status_code,200)

    def test_account_change_or_admin_revocation_during_render_blocks_send(self):
        def render(*args):
            self.sql("UPDATE \"user\" SET email='changed@example.com' WHERE id=1")
            return PDF
        self.render.side_effect=render
        self.assertEqual(self.post().status_code,409)
        self.sender.assert_not_called()
        self.login(3)
        self.render.side_effect=lambda *args: self.sql('UPDATE auth_approved_admin SET active=0') or PDF
        self.assertEqual(self.post(0,11).status_code,404)
        self.sender.assert_not_called()

    def test_suppressed_transport_is_not_recorded_as_success(self):
        self.app.config['MAIL_SUPPRESS_SEND']=True
        self.assertEqual(self.post().status_code,503)
        self.assertEqual(self.scalar('SELECT status FROM loss_report_email_send'),'uncertain')
        self.sender.assert_not_called()

    def test_invalid_pdf_is_never_sent(self):
        self.render.return_value=b'<html>renderer failure</html>'
        for route in (0,1):self.assertEqual(self.post(route).status_code,503)
        self.sender.assert_not_called();self.mail.send.assert_not_called()
        self.assertEqual(self.scalar('SELECT count(*) FROM loss_report_email_send'),0)

    def test_boolean_global_admin_flag_is_supported(self):
        self.sql('ALTER TABLE auth_approved_admin ALTER COLUMN active TYPE boolean USING active=1')
        self.login(3);self.assertEqual(self.post(0,11).status_code,200)

    def test_source_records_unchanged_and_migration_is_additive(self):
        def snapshot():
            with self.engine.connect() as c:
                return {t:list(c.execute(sa.text('SELECT * FROM '+t+' ORDER BY id'))) for t in ['lca_run','lca_result','lca_response']}
        before=snapshot();self.assertEqual(self.post().status_code,200);self.assertEqual(snapshot(),before)
        with self.assertRaisesRegex(RuntimeError,'Preserve'):self.migration.downgrade()


if __name__=='__main__':unittest.main()
