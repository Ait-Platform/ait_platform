"""Actual /forgot and delivery guards on isolated PostgreSQL; SMTP mocked."""
import ast
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import os
from pathlib import Path
import re
import sys
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

from alembic.migration import MigrationContext
from alembic.operations import Operations
from dotenv import dotenv_values
from flask import Blueprint, Flask, current_app, flash, redirect, render_template, request, url_for
from flask_wtf import CSRFProtect
from flask_wtf.csrf import generate_csrf
from jinja2 import ChoiceLoader, DictLoader
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('app.auth.password_reset_security',ROOT/'app/auth/password_reset_security.py')
security=importlib.util.module_from_spec(spec);spec.loader.exec_module(security)


class PasswordResetEmailSecurityTests(unittest.TestCase):
    def setUp(self):
        url=dotenv_values(ROOT/'.env')['DATABASE_URL'].replace('postgres://','postgresql://',1)
        parsed=sa.engine.make_url(url)
        self.assertEqual(parsed.database,'ait_local_db');self.assertIn(parsed.host,('localhost','127.0.0.1','::1'))
        self.prefix='pr_'+uuid4().hex[:16]+'_'
        self.names=['user','password_reset_request','password_reset_send','ix_reset_request_ip_time']
        self.engine=sa.create_engine(url)
        self.addCleanup(self.cleanup)
        @sa.event.listens_for(self.engine,'before_cursor_execute',retval=True)
        def isolate(conn,cursor,statement,parameters,context,executemany):
            for name in self.names:
                statement=re.sub(r'(?<![\w])'+name+r'(?![\w])',self.prefix+name,statement)
            return statement,parameters
        self.sessions=scoped_session(sessionmaker(bind=self.engine))
        self.addCleanup(self.sessions.remove)
        for patcher in [patch.object(security,'db',SimpleNamespace(session=self.sessions)),
                        patch.dict(sys.modules,{'app.auth.password_reset_security':security}),
                        patch.dict(os.environ,{'RENDER':'false'})]:
            patcher.start();self.addCleanup(patcher.stop)
        with self.engine.begin() as c:
            self.assertEqual(c.execute(sa.text('SELECT current_database()')).scalar(),'ait_local_db')
            c.exec_driver_sql('CREATE TABLE "user" (id integer PRIMARY KEY,name text,email text,password_hash text)')
            c.exec_driver_sql("INSERT INTO \"user\" VALUES (1,'Registered Owner','owner@example.com','existing-hash'),(2,'Second Owner','second@example.com','other-hash')")
            spec=importlib.util.spec_from_file_location('reset_migration',ROOT/'migrations/versions/reset_email_security_001.py')
            self.migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
            self.migration.op=Operations(MigrationContext.configure(c));self.migration.upgrade()
        self.messages=[]
        self.sender=Mock(side_effect=lambda *args,**kw:self.messages.append((args,kw)) or True)
        patcher=patch.object(security,'send_email',self.sender);patcher.start();self.addCleanup(patcher.stop)
        self.tasks=[]
        self.real_start=security.start_delivery
        patcher=patch.object(security,'start_delivery',side_effect=lambda email,url:self.tasks.append((email,url)))
        patcher.start();self.addCleanup(patcher.stop)
        self.app=self.make_app();self.client=self.app.test_client()

    def cleanup(self):
        self.sessions.remove()
        assert re.fullmatch(r'pr_[0-9a-f]{16}_',self.prefix)
        with self.engine.begin() as c:
            for name in reversed(self.names[:-1]):c.exec_driver_sql('DROP TABLE IF EXISTS "'+name+'" CASCADE')
        self.engine.dispose()

    def make_app(self):
        app=Flask(__name__,template_folder=str(ROOT/'templates'))
        app.config.update(SECRET_KEY='reset-test-only',TESTING=True)
        app.extensions['mail']=SimpleNamespace(suppress=False)
        app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':'{% block content %}{% endblock %}'}),app.jinja_loader])
        app.jinja_env.globals['csrf_token']=lambda:''
        bp=Blueprint('auth_bp',__name__)
        tree=ast.parse((ROOT/'app/auth/routes.py').read_text(encoding='utf-8-sig'))
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='forgot')
        exec(compile(ast.Module(body=[function],type_ignores=[]),'actual_forgot_route','exec'),
             dict(auth_bp=bp,request=request,current_app=current_app,flash=flash,redirect=redirect,
                  url_for=url_for,render_template=render_template))
        bp.add_url_rule('/login',endpoint='login',view_func=lambda:'Login')
        bp.add_url_rule('/reset/<token>',endpoint='reset',view_func=lambda token:'Reset')
        app.register_blueprint(bp)
        @app.teardown_appcontext
        def remove_session(error):self.sessions.remove()
        return app

    def post(self,email='owner@example.com',ip='203.0.113.8',client=None,headers=None,**extra):
        return (client or self.client).post('/forgot',data=dict(email=email,**extra),headers=headers or {},environ_overrides={'REMOTE_ADDR':ip})

    def deliver_tasks(self):
        tasks=self.tasks[:];self.tasks.clear()
        for email,url in tasks:
            with self.app.app_context():security.deliver_once(email,url)

    def count(self,table='password_reset_request'):
        with self.engine.connect() as c:return c.execute(sa.text('SELECT count(*) FROM '+table)).scalar()

    def sql(self,sql):
        with self.engine.begin() as c:return c.execute(sa.text(sql))

    def response_signature(self,response,client):
        with client.session_transaction() as s:flashes=s.get('_flashes',[])
        return response.status_code,response.location,response.data,flashes,response.headers.get('Cache-Control')

    def test_registered_unknown_malformed_and_cooldown_have_identical_responses(self):
        signatures=[]
        for i,email in enumerate(['owner@example.com','missing@example.com','not-email','owner@example.com']):
            client=self.app.test_client();response=self.post(email,f'203.0.113.{10+i}',client)
            signatures.append(self.response_signature(response,client));self.deliver_tasks()
        self.assertTrue(all(value==signatures[0] for value in signatures))
        self.assertEqual(len(self.messages),1)

    def test_three_requests_per_ip_including_unknown_and_malformed(self):
        for email in ['missing@example.com','','owner@example.com']:
            self.assertEqual(self.post(email).status_code,302)
        for email in ['owner@example.com','missing@example.com']:
            response=self.post(email);self.assertEqual(response.status_code,429)
            self.assertEqual(response.get_data(as_text=True),security.GENERIC_MESSAGE)
            self.assertGreater(int(response.headers['Retry-After']),0)
        self.assertEqual(self.count(),3);self.assertEqual(len(self.tasks),3)
        self.deliver_tasks();self.assertEqual(len(self.messages),1)

    def test_cooldown_shared_across_ips_and_workers(self):
        self.post();self.deliver_tasks()
        for i in range(3):
            worker=self.make_app().test_client()
            self.assertEqual(self.post(ip=f'203.0.113.{20+i}',client=worker).status_code,302)
            self.deliver_tasks()
        self.assertEqual(len(self.messages),1);self.assertEqual(self.count('password_reset_send'),1)
        self.assertEqual(self.count(),4)

    def test_cooldown_expiry_allows_new_explicit_request(self):
        self.post();self.deliver_tasks()
        self.sql("UPDATE password_reset_send SET attempted_at=clock_timestamp()-INTERVAL '11 minutes'")
        self.post(ip='203.0.113.9');self.deliver_tasks()
        self.assertEqual(len(self.messages),2)

    def test_different_accounts_have_separate_cooldowns(self):
        self.post();self.deliver_tasks()
        self.post('second@example.com');self.deliver_tasks()
        self.assertEqual(len(self.messages),2);self.assertEqual(self.count('password_reset_send'),2)

    def test_rolling_hour_and_worker_restart_do_not_reset_ip_limit(self):
        for _ in range(3):self.post()
        self.assertEqual(self.post(client=self.make_app().test_client()).status_code,429)
        self.sql("UPDATE password_reset_request SET attempted_at=clock_timestamp()-INTERVAL '59 minutes'")
        self.assertEqual(self.post().status_code,429)
        self.sql("UPDATE password_reset_request SET attempted_at=clock_timestamp()-INTERVAL '61 minutes'")
        self.assertEqual(self.post().status_code,302)

    def test_concurrent_ip_requests_allow_exactly_three(self):
        def submit(_):return self.post(client=self.make_app().test_client()).status_code
        with ThreadPoolExecutor(max_workers=8) as pool:codes=list(pool.map(submit,range(8)))
        self.assertEqual(codes.count(302),3,codes);self.assertEqual(codes.count(429),5,codes)
        self.assertEqual(self.count(),3)
        self.deliver_tasks();self.assertEqual(len(self.messages),1)

    def test_concurrent_account_requests_send_once(self):
        def deliver(i):
            with self.app.app_context():security.deliver_once('owner@example.com','https://ait.example/reset/'+security.URL_TOKEN_MARKER)
        with ThreadPoolExecutor(max_workers=6) as pool:list(pool.map(deliver,range(6)))
        self.assertEqual(len(self.messages),1);self.assertEqual(self.count('password_reset_send'),1)

    def test_actual_background_response_does_not_wait_for_smtp(self):
        entered=Event();release=Event();finished=Event()
        def send(*args,**kwargs):
            entered.set()
            try:self.assertTrue(release.wait(10));self.messages.append((args,kwargs));return True
            finally:finished.set()
        self.sender.side_effect=send
        with patch.object(security,'start_delivery',side_effect=self.real_start):
            response=self.post()
        self.assertEqual(response.status_code,302)
        self.assertTrue(entered.wait(10));self.assertFalse(finished.is_set())
        release.set();self.assertTrue(finished.wait(10))
        # Wait for the task's outcome commit before fixture cleanup.
        for _ in range(100):
            with self.engine.connect() as c:
                state=c.execute(sa.text('SELECT status FROM password_reset_send')).scalar()
            if state=='accepted':break
            Event().wait(0.01)
        self.assertEqual(state,'accepted')

    def test_existing_token_format_template_and_expiry_preserved(self):
        self.post();self.deliver_tasks()
        args,kwargs=self.messages[0]
        self.assertEqual(args[:2],('Reset your password',['owner@example.com']))
        url=kwargs['body'].removeprefix('Reset link: ')
        token=url.rsplit('/',1)[-1]
        from app.utils.password_reset import load_reset_token,make_reset_token
        with self.app.app_context():
            self.assertEqual(load_reset_token(token),1)
            with patch('itsdangerous.timed.time.time',return_value=1700000000):old=make_reset_token(1)
            with patch('itsdangerous.timed.time.time',return_value=1700003601):self.assertIsNone(load_reset_token(old))
        self.assertIn(url,kwargs['html']);self.assertIn('valid for 60 minutes',kwargs['html'])
        self.assertIn('Hi Registered Owner',kwargs['html'])

    def test_recipient_always_saved_account_email(self):
        self.sql("UPDATE \"user\" SET email='Owner@Example.com' WHERE id=1")
        self.post(' OWNER@EXAMPLE.COM ',to='attacker@example.com',recipient='attacker@example.com')
        self.deliver_tasks()
        self.assertEqual(self.messages[0][0][1],['Owner@Example.com'])

    def test_smtp_failure_generic_response_and_durable_cooldown(self):
        self.sender.side_effect=RuntimeError('535 private password detail')
        response=self.post();self.deliver_tasks()
        self.assertEqual(response.status_code,302);self.assertNotIn(b'private',response.data)
        self.assertEqual(self.count('password_reset_send'),1)
        self.post(ip='203.0.113.9');self.deliver_tasks();self.sender.assert_called_once()
        with self.engine.connect() as c:self.assertEqual(c.execute(sa.text('SELECT status FROM password_reset_send')).scalar(),'uncertain')

    def test_sender_false_suppression_and_template_failures_do_not_retry(self):
        self.sender.return_value=False;self.sender.side_effect=None
        self.post();self.deliver_tasks();self.sender.assert_called_once()
        self.post(ip='203.0.113.9');self.deliver_tasks();self.sender.assert_called_once()
        self.app.config['MAIL_SUPPRESS_SEND']=True
        self.post('second@example.com',ip='203.0.113.10');self.deliver_tasks();self.sender.assert_called_once()
        self.sql("UPDATE password_reset_send SET attempted_at=clock_timestamp()-INTERVAL '11 minutes'")
        with patch.object(security,'render_template',side_effect=RuntimeError('private template path')):
            self.post(ip='203.0.113.11');self.deliver_tasks()
        self.sender.assert_called_once()

    def test_missing_ip_table_fails_closed_with_generic_response(self):
        self.sql('DROP TABLE password_reset_request')
        response=self.post();self.assertEqual(response.status_code,302)
        self.assertEqual(self.tasks,[]);self.sender.assert_not_called()
        self.assertNotIn(b'password_reset_request',response.data)

    def test_missing_account_table_never_sends(self):
        self.sql('DROP TABLE password_reset_send')
        self.assertEqual(self.post().status_code,302);self.deliver_tasks()
        self.sender.assert_not_called();self.assertEqual(self.count(),1)

    def test_ip_commit_failure_never_schedules_or_sends(self):
        session=self.sessions()
        with patch.object(session,'commit',side_effect=RuntimeError('private database error')):
            self.assertEqual(self.post().status_code,302)
        self.assertEqual(self.tasks,[]);self.assertEqual(self.count(),0);self.sender.assert_not_called()

    def test_account_reservation_commit_failure_never_sends(self):
        self.post()
        with self.app.app_context():
            session=self.sessions()
            with patch.object(session,'commit',side_effect=RuntimeError('private database error')):
                security.deliver_once('owner@example.com','https://ait.example/reset/'+security.URL_TOKEN_MARKER)
        self.sender.assert_not_called();self.assertEqual(self.count('password_reset_send'),0)
        self.tasks.clear()

    def test_outcome_commit_failure_cannot_trigger_duplicate(self):
        self.post()
        with self.app.app_context():
            session=self.sessions();real=session.commit;calls=[]
            def commit():
                calls.append(1)
                if len(calls)>1:raise RuntimeError('private commit detail')
                return real()
            with patch.object(session,'commit',side_effect=commit):
                security.deliver_once('owner@example.com','https://ait.example/reset/'+security.URL_TOKEN_MARKER)
        self.tasks.clear();self.post(ip='203.0.113.9');self.deliver_tasks()
        self.assertEqual(len(self.messages),1)
        with self.engine.connect() as c:self.assertEqual(c.execute(sa.text('SELECT status FROM password_reset_send')).scalar(),'attempted')

    def test_send_occurs_only_after_account_reservation_committed(self):
        observed=[]
        def send(*args,**kw):observed.append(self.count('password_reset_send'));return True
        self.sender.side_effect=send;self.post();self.deliver_tasks();self.assertEqual(observed,[1])

    def test_unknown_and_invalid_accounts_never_create_cooldown_rows(self):
        for email in ['unknown@example.com','invalid','']:
            self.post(email);self.deliver_tasks()
        self.assertEqual(self.count(),3);self.assertEqual(self.count('password_reset_send'),0)
        self.sender.assert_not_called()

    def test_direct_headers_cannot_bypass_ip_limit(self):
        for i in range(3):self.post(headers={'X-Forwarded-For':f'192.0.2.{i}','CF-Connecting-IP':f'192.0.2.{i}'})
        self.assertEqual(self.post(headers={'X-Forwarded-For':'192.0.2.100'}).status_code,429)

    def test_render_client_ip_uses_edge_header_not_xff(self):
        with patch.dict(os.environ,{'RENDER':'true'}):
            for i in range(3):self.post(ip=f'10.0.0.{i}',headers={'CF-Connecting-IP':'203.0.113.8','X-Forwarded-For':f'192.0.2.{i}'})
            self.assertEqual(self.post(headers={'CF-Connecting-IP':'203.0.113.8'}).status_code,429)
            self.assertEqual(self.post(headers={'CF-Connecting-IP':'203.0.113.9'}).status_code,302)

    def test_missing_or_invalid_ip_returns_generic_without_sending(self):
        with patch.dict(os.environ,{'RENDER':'true'}):
            for value in ['', 'bad', '192.0.2.1,192.0.2.2','fe80::1%eth0']:
                self.assertEqual(self.post(headers={'CF-Connecting-IP':value}).status_code,302)
        self.assertEqual(self.count(),0);self.assertEqual(self.tasks,[])

    def test_ipv6_canonicalization_and_ipv4_mapping_share_quota(self):
        for _ in range(3):self.post(ip='2001:db8::1')
        self.assertEqual(self.post(ip='2001:0db8:0:0:0:0:0:1').status_code,429)
        for _ in range(3):self.post(ip='203.0.113.8')
        self.assertEqual(self.post(ip='::ffff:203.0.113.8').status_code,429)

    def test_get_and_csrf_behavior_preserved(self):
        CSRFProtect(self.app)
        @self.app.get('/test-token')
        def token():return generate_csrf()
        self.assertEqual(self.client.get('/forgot').status_code,200)
        self.assertEqual(self.post().status_code,400);self.assertEqual(self.count(),0)
        token=self.client.get('/test-token').get_data(as_text=True)
        self.assertEqual(self.post(csrf_token=token).status_code,302)

    def test_task_start_failure_is_generic_and_does_not_refund_ip_slot(self):
        with patch.object(security,'start_delivery',side_effect=RuntimeError('private task detail')):
            response=self.post()
        self.assertEqual(response.status_code,302);self.assertNotIn(b'private',response.data)
        self.assertEqual(self.count(),1);self.sender.assert_not_called()

    def test_case_ambiguous_legacy_accounts_fail_closed(self):
        self.sql("INSERT INTO \"user\" VALUES (3,'Ambiguous','OWNER@example.com','same-case-variant')")
        self.assertEqual(self.post().status_code,302);self.deliver_tasks()
        self.sender.assert_not_called();self.assertEqual(self.count('password_reset_send'),0)

    def test_suppressed_transport_extension_prevents_delivery(self):
        self.app.extensions['mail'].suppress=True
        self.post();self.deliver_tasks();self.sender.assert_not_called()
        self.app.extensions['mail'].suppress=False
        self.post(ip='203.0.113.9');self.deliver_tasks();self.sender.assert_not_called()
        self.assertEqual(self.count('password_reset_send'),1)

    def test_source_user_records_unchanged_and_quota_has_no_plain_ip(self):
        with self.engine.connect() as c:before=list(c.execute(sa.text('SELECT * FROM "user" ORDER BY id')))
        self.post();self.deliver_tasks()
        with self.engine.connect() as c:
            self.assertEqual(list(c.execute(sa.text('SELECT * FROM "user" ORDER BY id'))),before)
            row=c.execute(sa.text('SELECT * FROM password_reset_request')).mappings().one()
        self.assertEqual(set(row),{'id','ip_hash','attempted_at'});self.assertRegex(row['ip_hash'],r'^[0-9a-f]{64}$')
        self.assertNotIn('203.0.113.8',str(row))
        with self.assertRaisesRegex(RuntimeError,'Preserve'):self.migration.downgrade()


if __name__=='__main__':unittest.main()
