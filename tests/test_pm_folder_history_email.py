"""Original historical bytes through the authenticated, CSRF-protected email route."""
import ast
import hashlib
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4
from flask import Flask, Blueprint, request, current_app, abort
from flask_login import LoginManager, UserMixin, current_user, login_required
from flask_wtf.csrf import CSRFProtect, generate_csrf
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from dotenv import dotenv_values
from test_pm_directory_register import ROOT, REPORTS


class HistoricalEmailTests(unittest.TestCase):
    def setUp(self):
        url=dotenv_values(ROOT/'.env')['DATABASE_URL'];parsed=make_url(url)
        if parsed.host not in {'localhost','127.0.0.1'} or parsed.database!='ait_local_db':self.skipTest('Verified local PostgreSQL required')
        self.engine=create_engine(url);self.conn=self.engine.connect();self.addCleanup(self.engine.dispose);self.addCleanup(self.conn.close)
        self.conn.execute(text('CREATE TEMP TABLE bil_property(id integer PRIMARY KEY,manager_id integer)'))
        self.conn.execute(text('INSERT INTO bil_property VALUES(26,7),(27,7),(28,8)'))
        self.conn.execute(text('CREATE TEMP TABLE bil_utilities_capture_audit(id serial PRIMARY KEY,property_id integer,kind varchar(20),record_id integer,actor_id integer,after_data json)'))
        self.conn.commit();self.session=Session(bind=self.conn);self.addCleanup(self.session.close)
        app=self.app=Flask('history-email');app.secret_key='isolated-test-only';app.config['TESTING']=True
        CSRFProtect(app);manager=LoginManager(app)
        class User(UserMixin):
            def __init__(self,id):self.id=id
        manager.user_loader(lambda uid:User(int(uid)))
        bp=Blueprint('billing_bp',__name__);namespace=dict(db=SimpleNamespace(session=self.session),text=text,abort=abort,current_user=current_user,billing_bp=bp,request=request,current_app=current_app,login_required=login_required,require_pm_user=lambda:None,CATALOG={'all':[(r['code'],r['title']) for r in REPORTS]})
        tree=ast.parse((ROOT/'app/program_billing/metsoa_setup_safety.py').read_text(encoding='utf-8-sig'))
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='property_scope')
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'<real-scope>','exec'),namespace)
        tree=ast.parse((ROOT/'app/program_billing/directory_register.py').read_text(encoding='utf-8-sig'))
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='directory_email')
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'<real-email>','exec'),namespace)
        app.register_blueprint(bp);app.add_url_rule('/csrf',view_func=lambda:generate_csrf())
        self.client=app.test_client()
        with self.client.session_transaction() as s:s['_user_id']='7';s['_fresh']=True
        self.csrf=self.client.get('/csrf').text
        self.pdf=b'%PDF-1.4\nOriginal September figures: 123.45\n%%EOF'
        self.request_id=str(uuid4())
        mail=patch('app.utils.mailer.send_pdf_email',return_value=True);self.mail=mail.start();self.addCleanup(mail.stop)
    def post(self,**overrides):
        data=dict(property_id='27',report='metsoa_statement',email_to='recipient@example.com',request_id=self.request_id,csrf_token=self.csrf,document=(BytesIO(self.pdf),'September original.pdf'))
        data.update(overrides);return self.client.post('/billing/directory-register/email',data=data)
    def test_exact_original_attachment_and_durable_duplicate_prevention(self):
        def accepted(*args):
            self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_utilities_capture_audit WHERE after_data->>'status'='attempted'")).scalar(),1)
            self.assertEqual(args[3:],(self.pdf,'September original.pdf'));return True
        self.mail.side_effect=accepted
        response=self.post();self.assertEqual(response.status_code,200,response.text)
        self.assertIn('accepted',response.json['message']);self.assertEqual(self.post().status_code,409);self.mail.assert_called_once()
        rows=self.session.execute(text('SELECT property_id,actor_id,after_data FROM bil_utilities_capture_audit ORDER BY id')).all()
        self.assertEqual([(r[0],r[1],r[2]['status']) for r in rows],[(27,7,'attempted'),(27,7,'accepted')])
        self.assertTrue(all(r[2]['sha256']==hashlib.sha256(self.pdf).hexdigest() for r in rows))
    def test_login_csrf_and_cross_manager_scope(self):
        self.assertEqual(self.post(csrf_token='').status_code,400)
        self.assertEqual(self.post(property_id='28').status_code,403)
        with self.client.session_transaction() as s:s.pop('_user_id')
        self.assertEqual(self.post().status_code,401);self.mail.assert_not_called()
    def test_invalid_recipient_pdf_and_filename(self):
        for fields in [dict(email_to='invalid'),dict(document=(BytesIO(b'not a PDF'),'report.pdf')),dict(document=(BytesIO(self.pdf),'../report.pdf')),dict(report='foreign')]:
            with self.subTest(fields=fields):self.assertEqual(self.post(**fields).status_code,400)
        self.mail.assert_not_called()
    def test_smtp_failure_is_not_success_and_is_not_retried(self):
        self.mail.side_effect=RuntimeError('SMTP unavailable')
        response=self.post();self.assertEqual(response.status_code,503);self.assertIn('not confirmed',response.json['message'])
        self.assertEqual(self.post().status_code,409);self.mail.assert_called_once()
    def test_suppressed_mail_and_oversized_attachment_do_not_send(self):
        self.app.config['MAIL_SUPPRESS_SEND']=True
        self.assertEqual(self.post().status_code,503)
        self.app.config['MAIL_SUPPRESS_SEND']=False
        self.assertEqual(self.post(document=(BytesIO(b'%PDF-'+b'x'*(10*1024*1024)),'large.pdf')).status_code,413)
        self.mail.assert_not_called()
