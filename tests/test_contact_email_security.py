"""Actual contact route and quota on isolated PostgreSQL tables; SMTP mocked."""
import ast
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import os
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

from alembic.migration import MigrationContext
from alembic.operations import Operations
from dotenv import dotenv_values
from flask import Blueprint, Flask, current_app, flash, redirect, render_template, request, url_for
from flask_mail import Message
from flask_wtf import CSRFProtect
from flask_wtf.csrf import generate_csrf
from jinja2 import DictLoader
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('app.public.contact_security',ROOT/'app/public/contact_security.py')
security=importlib.util.module_from_spec(spec);spec.loader.exec_module(security)


class ContactEmailSecurityTests(unittest.TestCase):
    def setUp(self):
        url=dotenv_values(ROOT/'.env')['DATABASE_URL'].replace('postgres://','postgresql://',1)
        parsed=sa.engine.make_url(url)
        self.assertEqual(parsed.database,'ait_local_db')
        self.assertIn(parsed.host,('localhost','127.0.0.1','::1'))
        self.table='ce_'+uuid4().hex[:16]+'_attempt'
        self.engine=sa.create_engine(url)
        self.addCleanup(self.cleanup)
        @sa.event.listens_for(self.engine,'before_cursor_execute',retval=True)
        def isolated(conn,cursor,statement,parameters,context,executemany):
            for name,new in [('contact_email_attempt',self.table),('ix_contact_email_ip_time',self.table+'_idx')]:
                statement=re.sub(r'(?<![\w])'+name+r'(?![\w])',new,statement)
            return statement,parameters
        self.sessions=scoped_session(sessionmaker(bind=self.engine))
        self.addCleanup(self.sessions.remove)
        self.patch_db=patch.object(security,'db',SimpleNamespace(session=self.sessions))
        self.patch_db.start();self.addCleanup(self.patch_db.stop)
        self.patch_module=patch.dict(sys.modules,{'app.public.contact_security':security})
        self.patch_module.start();self.addCleanup(self.patch_module.stop)
        self.patch_env=patch.dict(os.environ,{'RENDER':'false'})
        self.patch_env.start();self.addCleanup(self.patch_env.stop)
        with self.engine.begin() as c:
            self.assertEqual(c.execute(sa.text('SELECT current_database()')).scalar(),'ait_local_db')
            spec=importlib.util.spec_from_file_location('contact_migration',ROOT/'migrations/versions/contact_email_security_001.py')
            self.migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.migration)
            self.migration.op=Operations(MigrationContext.configure(c));self.migration.upgrade()
        self.messages=[]
        self.mail=SimpleNamespace(send=Mock(side_effect=self.messages.append))
        self.app=self.make_app();self.client=self.app.test_client()

    def cleanup(self):
        self.sessions.remove()
        assert re.fullmatch(r'ce_[0-9a-f]{16}_attempt',self.table)
        with self.engine.begin() as c:c.exec_driver_sql('DROP TABLE IF EXISTS "'+self.table+'"')
        self.engine.dispose()

    def make_app(self):
        app=Flask(__name__,template_folder=str(ROOT/'templates'))
        app.config.update(SECRET_KEY='contact-test-only',TESTING=True,MAIL_USERNAME='ait@example.com',
                          MAIL_PASSWORD='dummy-not-live',CONTACT_TO_EMAIL='support@example.com')
        app.extensions['mail']=SimpleNamespace(default_sender='ait@example.com',suppress=False)
        app.jinja_loader=DictLoader({'public/contact.html':'Existing contact form'})
        bp=Blueprint('public_bp',__name__)
        tree=ast.parse((ROOT/'app/public/routes.py').read_text(encoding='utf-8'))
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='contact')
        scope=dict(public_bp=bp,request=request,current_app=current_app,flash=flash,
                   redirect=redirect,url_for=url_for,render_template=render_template,Message=Message,mail=self.mail)
        exec(compile(ast.Module(body=[function],type_ignores=[]),'actual_contact_route','exec'),scope)
        app.register_blueprint(bp)
        @app.teardown_appcontext
        def remove_session(error):self.sessions.remove()
        return app

    def post(self,ip='203.0.113.8',client=None,headers=None,**changes):
        data=dict(name='Visitor Name',email='visitor@example.com',subject='Help please',message='A legitimate question.')
        data.update(changes)
        return (client or self.client).post('/contact',data=data,headers=headers or {},environ_overrides={'REMOTE_ADDR':ip})

    def count(self):
        with self.engine.connect() as c:return c.execute(sa.text('SELECT count(*) FROM contact_email_attempt')).scalar()

    def sql(self,sql):
        with self.engine.begin() as c:return c.execute(sa.text(sql))

    def test_normal_submission_preserves_recipient_reply_to_body_and_redirect(self):
        response=self.post(to='attacker@example.com',recipient='attacker@example.com')
        self.assertEqual(response.status_code,302);self.assertEqual(response.location,'/contact')
        message=self.messages[0]
        self.assertEqual(message.recipients,['support@example.com'])
        self.assertEqual(message.reply_to,'visitor@example.com')
        self.assertEqual(message.subject,'[AIT Contact] Help please')
        self.assertIn('A legitimate question.',message.body)
        self.assertEqual(self.count(),1)

    def test_exactly_three_per_ip_per_hour_and_retry_after(self):
        for _ in range(3):self.assertEqual(self.post().status_code,302)
        response=self.post()
        self.assertEqual(response.status_code,429)
        self.assertGreater(int(response.headers['Retry-After']),0)
        self.assertLessEqual(int(response.headers['Retry-After']),3600)
        self.assertEqual(response.mimetype,'text/plain')
        self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.assertEqual(self.count(),3);self.assertEqual(len(self.messages),3)

    def test_different_ip_has_separate_quota(self):
        for _ in range(3):self.post()
        self.assertEqual(self.post('203.0.113.9').status_code,302)
        self.assertEqual(self.count(),4)

    def test_reopening_session_or_worker_does_not_reset_quota(self):
        for _ in range(3):self.post()
        worker=self.make_app().test_client()
        self.assertEqual(self.post(client=worker).status_code,429)
        self.assertEqual(self.post(client=self.app.test_client()).status_code,429)

    def test_rolling_hour_releases_only_expired_slots(self):
        for _ in range(3):self.post()
        self.sql("UPDATE contact_email_attempt SET attempted_at=clock_timestamp()-INTERVAL '61 minutes' WHERE id=(SELECT id FROM contact_email_attempt ORDER BY attempted_at LIMIT 1)")
        self.assertEqual(self.post().status_code,302)
        self.assertEqual(self.post().status_code,429)
        self.assertEqual(len(self.messages),4)

    def test_three_before_hour_boundary_do_not_allow_three_after(self):
        for _ in range(3):self.post()
        self.sql("UPDATE contact_email_attempt SET attempted_at=clock_timestamp()-INTERVAL '59 minutes'")
        self.assertEqual(self.post().status_code,429)
        self.sql("UPDATE contact_email_attempt SET attempted_at=clock_timestamp()-INTERVAL '61 minutes'")
        self.assertEqual(self.post().status_code,302)

    def test_concurrent_workers_send_at_most_three(self):
        def submit(_):return self.post(client=self.make_app().test_client()).status_code
        with ThreadPoolExecutor(max_workers=8) as pool:codes=list(pool.map(submit,range(8)))
        self.assertEqual(codes.count(302),3,codes);self.assertEqual(codes.count(429),5,codes)
        self.assertEqual(self.count(),3);self.assertEqual(len(self.messages),3)

    def test_smtp_failures_consume_slots_without_retry_or_error_leak(self):
        self.mail.send.side_effect=RuntimeError('SMTP 535 secret-password internals')
        for _ in range(3):
            response=self.post();self.assertEqual(response.status_code,503)
            for secret in (b'535',b'secret-password',b'RuntimeError'):self.assertNotIn(secret,response.data)
        self.assertEqual(self.post().status_code,429)
        self.assertEqual(self.mail.send.call_count,3);self.assertEqual(self.count(),3)

    def test_uncertain_smtp_does_not_retry_or_refund_attempt(self):
        def accepted_then_failed(message):
            self.messages.append(message)
            raise RuntimeError('transport outcome lost')
        self.mail.send.side_effect=accepted_then_failed
        self.assertEqual(self.post().status_code,503)
        self.assertEqual(len(self.messages),1);self.assertEqual(self.count(),1)
        self.mail.send.assert_called_once()

    def test_missing_mail_config_and_suppression_consume_slots(self):
        self.app.config['MAIL_PASSWORD']=''
        self.assertEqual(self.post().status_code,503)
        self.app.config['MAIL_PASSWORD']='dummy';self.app.config['MAIL_SUPPRESS_SEND']=True
        self.assertEqual(self.post().status_code,503)
        self.app.config['MAIL_SUPPRESS_SEND']=False;self.app.extensions['mail'].suppress=True
        self.assertEqual(self.post().status_code,503)
        self.assertEqual(self.post().status_code,429)
        self.mail.send.assert_not_called();self.assertEqual(self.count(),3)

    def test_missing_quota_table_fails_closed_before_smtp(self):
        self.sql('DROP TABLE contact_email_attempt')
        response=self.post();self.assertEqual(response.status_code,503)
        self.assertNotIn(b'contact_email_attempt',response.data);self.mail.send.assert_not_called()

    def test_quota_commit_failure_never_calls_smtp(self):
        session=self.sessions()
        with patch.object(session,'commit',side_effect=RuntimeError('private database detail')):
            response=self.post();self.assertEqual(response.status_code,503)
        self.assertNotIn(b'private',response.data);self.mail.send.assert_not_called();self.assertEqual(self.count(),0)

    def test_smtp_runs_only_after_durable_quota_commit(self):
        observed=[]
        self.mail.send.side_effect=lambda message: observed.append(self.count())
        self.assertEqual(self.post().status_code,302);self.assertEqual(observed,[1])

    def test_get_and_head_do_not_consume_quota_or_require_tracking_table(self):
        self.sql('DROP TABLE contact_email_attempt')
        for method in ('GET','HEAD'):
            self.assertEqual(self.client.open('/contact',method=method).status_code,200)
        self.mail.send.assert_not_called()

    def test_invalid_form_and_header_injection_are_controlled(self):
        for changes in [dict(email='not-email'),dict(name=''),dict(message=''),dict(subject='Header\r\nBcc: evil@example.com'),dict(name='N'*201),dict(subject='S'*201),dict(message='M'*10001)]:
            response=self.post(**changes);self.assertEqual(response.status_code,400)
        self.assertEqual(self.count(),0);self.mail.send.assert_not_called()

    def test_optional_subject_still_works(self):
        self.assertEqual(self.post(subject='').status_code,302)
        self.assertEqual(self.messages[0].subject,'[AIT Contact] Contact form')

    def test_spoofed_forwarded_headers_ignored_on_direct_requests(self):
        for i in range(3):
            self.assertEqual(self.post(headers={'X-Forwarded-For':f'198.51.100.{i}', 'CF-Connecting-IP':f'198.51.100.{i}'}).status_code,302)
        self.assertEqual(self.post(headers={'X-Forwarded-For':'192.0.2.2','CF-Connecting-IP':'192.0.2.2'}).status_code,429)

    def test_render_uses_edge_ip_and_ignores_spoofed_xff_prefix(self):
        with patch.dict(os.environ,{'RENDER':'true'}):
            for i in range(3):
                self.assertEqual(self.post('10.0.0.5',headers={'CF-Connecting-IP':'203.0.113.8','X-Forwarded-For':f'192.0.2.{i}, 203.0.113.8'}).status_code,302)
            self.assertEqual(self.post('10.0.0.6',headers={'CF-Connecting-IP':'203.0.113.8'}).status_code,429)
            self.assertEqual(self.post('10.0.0.6',headers={'CF-Connecting-IP':'203.0.113.9'}).status_code,302)

    def test_render_missing_or_invalid_edge_ip_fails_closed(self):
        with patch.dict(os.environ,{'RENDER':'true'}):
            for value in ('','bad','203.0.113.8, 203.0.113.9','fe80::1%eth0'):
                self.assertEqual(self.post(headers={'CF-Connecting-IP':value,'X-Forwarded-For':'203.0.113.8'}).status_code,503)
        self.assertEqual(self.count(),0);self.mail.send.assert_not_called()

    def test_canonical_ipv6_and_mapped_ipv4_cannot_reset_quota(self):
        for _ in range(3):self.post('2001:db8::1')
        self.assertEqual(self.post('2001:0db8:0000:0000:0000:0000:0000:0001').status_code,429)
        for _ in range(3):self.post('203.0.113.8')
        self.assertEqual(self.post('::ffff:203.0.113.8').status_code,429)

    def test_quota_storage_contains_no_plain_ip_or_message(self):
        self.post()
        with self.engine.connect() as c:
            row=c.execute(sa.text('SELECT * FROM contact_email_attempt')).mappings().one()
        self.assertEqual(set(row),{'id','ip_hash','attempted_at'})
        self.assertRegex(row['ip_hash'],r'^[0-9a-f]{64}$')
        self.assertNotIn('203.0.113.8',str(row));self.assertNotIn('legitimate',str(row))

    def test_csrf_protection_remains_in_effect(self):
        CSRFProtect(self.app)
        @self.app.get('/test-token')
        def token():return generate_csrf()
        self.assertEqual(self.post().status_code,400)
        self.assertEqual(self.count(),0)
        token=self.client.get('/test-token').get_data(as_text=True)
        self.assertEqual(self.post(csrf_token=token).status_code,302)

    def test_missing_source_ip_fails_closed(self):
        self.assertEqual(self.post('').status_code,503)
        self.mail.send.assert_not_called();self.assertEqual(self.count(),0)

    def test_migration_preserves_reservations_on_downgrade(self):
        self.post()
        with self.assertRaisesRegex(RuntimeError,'Preserve'):self.migration.downgrade()
        self.assertEqual(self.count(),1)


if __name__=='__main__':unittest.main()
