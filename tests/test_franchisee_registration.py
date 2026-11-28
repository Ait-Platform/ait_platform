"""Actual routes/migration on session-temporary PostgreSQL; no app factory or real mail."""
import ast
import importlib.util
import json
import logging
import re
import sys
import types
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, scoped_session, sessionmaker
from sqlalchemy.engine import make_url
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.environment import EnvironmentContext
from cryptography.fernet import Fernet
from dotenv import dotenv_values
from flask import Flask, render_template
from flask_login import LoginManager, UserMixin
from flask_mail import Mail
from flask_sqlalchemy.query import Query
from flask_wtf import CSRFProtect
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
from lxml import html

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class FranchiseeTests(unittest.TestCase):
    def setUp(self):
        raw = dotenv_values(ROOT / '.env')['DATABASE_URL']
        target = make_url(raw.replace('postgres://','postgresql://',1))
        self.assertEqual(target.database,'ait_local_db')
        self.assertIn(target.host,('localhost','127.0.0.1','::1'))
        self.engine = sa.create_engine(target,connect_args={'connect_timeout':5})
        self.addCleanup(self.engine.dispose)
        self.conn = self.engine.connect();self.addCleanup(self.conn.close)
        self.conn.exec_driver_sql('CREATE TEMP TABLE "user"(id integer PRIMARY KEY,email text UNIQUE,name text,role text)')
        self.conn.exec_driver_sql('CREATE TEMP TABLE mech_shops(id integer PRIMARY KEY,user_id integer,business_name text,trade text)')
        self.conn.exec_driver_sql('CREATE TEMP TABLE protrade_document(id integer PRIMARY KEY,user_id integer,shop_id integer,kind text,document_key text,snapshot json,created_at timestamp)')
        self.conn.exec_driver_sql('CREATE TEMP TABLE auth_subject(id integer PRIMARY KEY,slug text,name text,is_active integer,is_hidden_on_bridge boolean)')
        self.conn.exec_driver_sql('CREATE TEMP TABLE user_enrollment(id integer PRIMARY KEY,user_id integer,subject_id integer,status text)')
        self.conn.exec_driver_sql('CREATE TEMP TABLE franchise_license(id integer PRIMARY KEY,user_id integer,subject_id integer,total_seats integer,used_seats integer)')
        self.conn.exec_driver_sql('SET search_path TO pg_temp')
        migration = module('franchisee_migration_test','migrations/versions/ait_franchisee_001.py')
        config=Config();config.attributes['franchisee_registration_approved']=True
        with EnvironmentContext(config,ScriptDirectory(str(ROOT/'migrations'))) as environment:
            environment.configure(connection=self.conn)
            migration.op=Operations(environment.get_context());migration.upgrade()
            config.attributes['franchise_registration_rename_approved']=True
            rename=module('franchise_rename_setup','migrations/versions/franchise_registration_002.py')
            rename.op=Operations(environment.get_context());rename.upgrade()
            config.attributes['franchise_registration_simplify_approved']=True
            simplify=module('franchise_simplify_setup','migrations/versions/franchise_registration_003.py')
            simplify.op=Operations(environment.get_context());simplify.upgrade()
        for name in ['franchise_registration','ait_franchisee_number_seq']:
            self.assertEqual(self.conn.execute(sa.text('SELECT relpersistence FROM pg_class WHERE oid=to_regclass(:name)'),{'name':name}).scalar(),'t')
        self.conn.commit()
        base = declarative_base()
        sessions = scoped_session(sessionmaker(bind=self.conn, query_cls=Query))
        self.addCleanup(sessions.remove)
        base.query = sessions.query_property()
        class User(base,UserMixin):
            __tablename__='user'
            id=sa.Column(sa.Integer,primary_key=True)
            email=sa.Column(sa.Text)
            name=sa.Column(sa.Text)
            role=sa.Column(sa.Text)
        class Shop(base):
            __tablename__='mech_shops'
            id=sa.Column(sa.Integer,primary_key=True)
            user_id=sa.Column(sa.Integer)
            business_name=sa.Column(sa.Text)
            trade=sa.Column(sa.Text)
        class Document(base):
            __tablename__='protrade_document'
            id=sa.Column(sa.Integer,primary_key=True)
            user_id=sa.Column(sa.Integer)
            shop_id=sa.Column(sa.Integer)
            kind=sa.Column(sa.Text)
            document_key=sa.Column(sa.Text)
            snapshot=sa.Column(sa.JSON)
            created_at=sa.Column(sa.DateTime)
        class Subject(base):
            __tablename__='auth_subject'
            id=sa.Column(sa.Integer,primary_key=True)
            slug=sa.Column(sa.Text)
            name=sa.Column(sa.Text)
            is_active=sa.Column(sa.Integer)
            is_hidden_on_bridge=sa.Column(sa.Boolean)
        class Enrollment(base):
            __tablename__='user_enrollment'
            id=sa.Column(sa.Integer,primary_key=True)
            user_id=sa.Column(sa.Integer)
            subject_id=sa.Column(sa.Integer)
            status=sa.Column(sa.Text)
        class License(base):
            __tablename__='franchise_license'
            id=sa.Column(sa.Integer,primary_key=True)
            user_id=sa.Column(sa.Integer)
            subject_id=sa.Column(sa.Integer)
            total_seats=sa.Column(sa.Integer)
            used_seats=sa.Column(sa.Integer)
        self.Subject,self.Enrollment,self.License=Subject,Enrollment,License
        self.Shop,self.Document=Shop,Document
        fake_db = types.SimpleNamespace(Model=base,session=sessions)
        for name in ('Column','Integer','String','Text','ForeignKey','DateTime','JSON','CheckConstraint','UniqueConstraint'):
            setattr(fake_db,name,getattr(sa,name))
        tree=ast.parse((ROOT/'app/models/franchisee.py').read_text())
        namespace={'db':fake_db}
        exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.ClassDef)],type_ignores=[]),'<actual-franchisee-model>','exec'),namespace)
        self.Registration=namespace['FranchiseeRegistration'];self.User=User;self.session=sessions
        sessions.add_all([User(id=1,email='admin@example.org',name='AIT Admin',role='admin'),User(id=2,email='happiness@example.org',name='Existing Happiness',role='mechanic_user'),User(id=3,email='other@example.org',name='Other User',role='learner')]);sessions.commit()
        self.mail = types.SimpleNamespace(send=lambda msg:self.messages.append(msg))
        self.messages=[]
        replacements={}
        for name,path in [('app',ROOT/'app'),('app.models',ROOT/'app/models'),('app.utils',ROOT/'app/utils')]:
            value=types.ModuleType(name);value.__path__=[str(path)];replacements[name]=value
        for name in ['app.extensions','app.models.auth','app.models.franchisee','app.models.mechanic','app.utils.roles']:
            replacements[name]=types.ModuleType(name)
        replacements['app.extensions'].db=fake_db;replacements['app.extensions'].mail=self.mail
        replacements['app.models.mechanic'].MechShop=Shop
        replacements['app.models.mechanic'].ProTradeDocument=Document
        replacements['app.models.auth'].User=User
        replacements['app.models.auth'].AuthSubject=Subject
        replacements['app.models.auth'].UserEnrollment=Enrollment
        replacements['app.models.auth'].FranchiseLicense=License
        replacements['app.models.franchisee'].FranchiseeRegistration=self.Registration
        replacements['app.utils.roles'].is_admin=lambda user:user.role=='admin'
        self.modules=patch.dict(sys.modules,replacements);self.modules.start();self.addCleanup(self.modules.stop)
        self.flow=module('franchisee_routes_test','app/franchisees.py')
        app=Flask('franchisee-tests');app.secret_key='test-session-key-not-production'
        app.config.update(TESTING=True,WTF_CSRF_ENABLED=False,MAIL_SUPPRESS_SEND=False,MAIL_DEFAULT_SENDER='ait-test@example.org',FRANCHISEE_PII_KEY=Fernet.generate_key().decode(),FRANCHISEE_PUBLIC_ORIGIN='https://ait.test')
        Mail(app)  # Message defaults only; flow.mail.send is the capture stub above.
        CSRFProtect(app)
        manager=LoginManager(app);manager.user_loader(lambda uid:sessions.get(User,int(uid)))
        app.jinja_loader=ChoiceLoader([DictLoader({'layout.html':'<html><body>{% block content %}{% endblock %}</body></html>'}),FileSystemLoader(ROOT/'templates')])
        app.add_url_rule('/admin/',endpoint='admin_bp.index',view_func=lambda:'Admin Hub')
        app.add_url_rule('/bridge',endpoint='bridge_bp.bridge',view_func=lambda:'AIT Bridge')
        app.add_url_rule('/mechanic/',endpoint='mechanic_bp.mechanic_dashboard',view_func=lambda:'Existing ProTrade entry')
        app.add_url_rule('/contact',endpoint='public_bp.contact',view_func=lambda:'Contact AIT Admin')
        app.add_url_rule('/modes/<subject_slug>',endpoint='modes_bp.modes_checkpoint_route',view_func=lambda subject_slug:'Existing programme access gate: '+subject_slug)
        app.register_blueprint(self.flow.franchisee_bp)
        @app.teardown_request
        def end(error):
            sessions.rollback()
        self.app=app;self.client=app.test_client();self.login(1)

    def login(self, identifier):
        with self.client.session_transaction() as values:
            values.clear()
            if identifier:values['_user_id']=str(identifier);values['_fresh']=True

    def invite(self,email='happiness@example.org'):
        self.login(1)
        response=self.client.post('/admin/franchisees',data={'email':email})
        self.assertEqual(response.status_code,302,response.text)
        token=re.search(r'/franchisees/register/([A-Za-z0-9_-]+)',self.messages[-1].body).group(1)
        return token

    def fields(self,**changes):
        values=dict(full_name='Happiness Example',cellphone='0821234567',address='12 Synthetic Test Street',occupation='Contractor',accept_terms='yes')
        values.update(changes)
        return values

    def register_self(self,uid=2,**changes):
        self.login(uid)
        result=self.client.post('/franchisees/opportunities?area=register',data=self.fields(**changes))
        self.assertEqual(result.status_code,302,result.text)
        return self.Registration.query.filter_by(user_id=uid).one()

    def legacy(self,uid=2,number='FR-0001'):
        user=self.session.get(self.User,uid)
        key=self.app.config['FRANCHISEE_PII_KEY'].encode()
        old=Fernet(key)
        row=self.Registration(email=user.email,user_id=uid,status='Approved',full_name='Historical Profile',cellphone='0821234567',
            identity_ciphertext=old.encrypt(b'HISTORICAL-ID-DATA').decode(),identity_digest='unchanged-historical-digest',
            address_ciphertext=old.encrypt(b'12 Original Test Street').decode(),created_at=self.flow.now(),submitted_at=self.flow.now(),
            created_by=uid,franchisee_number=number,history=[{'action':'approved','actor_id':1,'at':self.flow.now().isoformat()}])
        self.session.add(row);self.session.commit()
        self.session.execute(sa.text("SELECT setval('ait_franchisee_number_seq',1,true)"));self.session.commit()
        return row

    def test_form_has_no_identity_or_email_input_and_uses_account_email(self):
        self.login(2)
        for area in ('register',):
            response=self.client.get('/franchisees/opportunities?area='+area)
            self.assertEqual(response.status_code,200)
            tree=html.fromstring(response.text)
            self.assertFalse(tree.xpath('//*[@name="sa_id" or @name="email"]'))
            self.assertNotIn('SA ID',response.text)
            self.assertIn('signed-in AIT account',response.text)
        row=self.register_self(email='attacker@example.org',sa_id='invalid',identity_digest='forged')
        self.assertEqual(row.email,'happiness@example.org')
        self.assertIsNone(row.identity_ciphertext)
        self.assertIsNone(row.identity_digest)
        self.assertTrue(row.is_active)
        self.assertEqual(row.franchisee_number,'FR-0001')
        fresh=self.app.test_client()
        with fresh.session_transaction() as values:values['_user_id']='2';values['_fresh']=True
        page=html.fromstring(fresh.get('/franchisees/opportunities').text)
        table=page.get_element_by_id('franchise-register-table')
        self.assertIn('FR-0001',table.text_content())
        self.assertIn('happiness@example.org',table.text_content())
        self.assertFalse(page.xpath('//input[@name="full_name"]'))

    def test_new_users_register_without_any_franchise_specific_settings(self):
        self.app.config.pop('FRANCHISEE_PII_KEY',None)
        self.app.config.pop('FRANCHISEE_PUBLIC_ORIGIN',None)
        first=self.register_self()
        second=self.register_self(3,full_name='Other profile')
        self.assertEqual((first.franchisee_number,second.franchisee_number),('FR-0001','FR-0002'))
        self.assertEqual(second.email,'other@example.org')
        self.assertIsNone(second.identity_ciphertext)
        self.assertIsNone(second.identity_digest)
        self.assertEqual(self.Registration.query.count(),2)
        self.assertEqual(self.messages,[])

    def test_historical_644_preserved_and_645_can_save_without_identity(self):
        self.session.add_all([self.User(id=644,email='bill-fixture@example.org',name='Bill fixture',role='learner'),
                              self.User(id=645,email='pro-fixture@example.org',name='PRO fixture',role='learner')])
        self.session.commit()
        original=self.legacy(644)
        before=self.session.execute(sa.text('SELECT to_jsonb(r)::text FROM franchise_registration r WHERE user_id=644')).scalar()
        registered=self.register_self(645,full_name='PRO fixture')
        self.assertEqual(registered.franchisee_number,'FR-0002')
        self.assertIsNone(registered.identity_digest)
        self.assertIsNone(registered.identity_ciphertext)
        self.assertEqual(before,self.session.execute(sa.text('SELECT to_jsonb(r)::text FROM franchise_registration r WHERE user_id=644')).scalar())
        self.assertNotIn('Historical Profile',self.client.get('/franchisees/opportunities').text)
        self.login(644)
        self.assertIn('FR-0001',self.client.get('/franchisees/opportunities').text)
        self.assertNotIn('PRO fixture',self.client.get('/franchisees/opportunities').text)
        self.login(1)
        detail=self.client.get(f'/admin/franchisees/{original.id}')
        self.assertIn('HISTORICAL-ID-DATA',detail.text)
        self.assertIn('12 Original Test Street',detail.text)

    def test_edit_preserves_historical_identity_number_dates_and_history(self):
        row=self.legacy()
        original=(row.id,row.franchisee_number,row.created_at,row.submitted_at,row.reviewed_at,row.identity_ciphertext,row.identity_digest)
        history=list(row.history)
        self.login(2)
        edit=html.fromstring(self.client.get('/franchisees/opportunities?area=edit').text)
        self.assertFalse(edit.xpath('//*[@name="sa_id" or @name="email"]'))
        self.assertNotIn('HISTORICAL-ID-DATA',edit.text_content())
        self.assertEqual(edit.xpath('//input[@name="full_name"]/@value'),['Historical Profile'])
        self.assertEqual(edit.xpath('//textarea[@name="address"]/text()'),['12 Original Test Street'])
        self.session.get(self.User,2).email='updated-login@example.org';self.session.commit()
        response=self.client.post('/franchisees/opportunities?area=edit',data=self.fields(action='save_profile',full_name='Updated',address='34 Updated Test Street',email='forged@example.org',sa_id='not-an-id'))
        self.assertEqual(response.status_code,302,response.text)
        row=self.Registration.query.one()
        self.assertEqual((row.id,row.franchisee_number,row.created_at,row.submitted_at,row.reviewed_at,row.identity_ciphertext,row.identity_digest),original)
        self.assertEqual(row.history[:-1],history)
        self.assertEqual(row.history[-1]['action'],'profile_updated')
        self.assertEqual(row.email,'updated-login@example.org')
        with self.app.app_context():self.assertEqual(self.flow.decrypt_private(row.address_ciphertext),'34 Updated Test Street')

    def test_saved_submitted_profile_activation_uses_current_account_email(self):
        with self.app.app_context():address=self.flow.encrypt_private('12 Previously Saved Street')
        date=self.flow.now()
        row=self.Registration(email='happiness@example.org',user_id=2,status='Submitted',full_name='Saved Profile',cellphone='0821234567',
            address_ciphertext=address,created_at=date,submitted_at=date,created_by=2,history=[])
        self.session.add(row);self.session.commit()
        self.session.get(self.User,2).email='updated-login@example.org';self.session.commit()
        self.login(2)
        response=self.client.post('/franchisees/opportunities',data={'action':'activate','accept_terms':'yes','email':'forged@example.org'})
        self.assertEqual(response.status_code,302,response.text)
        row=self.Registration.query.one()
        self.assertTrue(row.is_active)
        self.assertEqual(row.email,'updated-login@example.org')
        self.assertEqual(row.submitted_at,date)
        self.assertEqual(row.address_ciphertext,address)
        self.assertEqual(row.franchisee_number,'FR-0001')

    def test_new_profile_without_identity_can_be_reopened_and_edited(self):
        row=self.register_self()
        stable=(row.id,row.franchisee_number,row.created_at,row.submitted_at)
        response=self.client.get('/franchisees/opportunities?area=edit')
        self.assertEqual(response.status_code,200)
        self.assertIn('12 Synthetic Test Street',response.text)
        saved=self.client.post('/franchisees/opportunities?area=edit',data=self.fields(action='save_profile',full_name='Edited'))
        self.assertEqual(saved.status_code,302)
        row=self.Registration.query.one()
        self.assertEqual((row.id,row.franchisee_number,row.created_at,row.submitted_at),stable)
        self.assertIsNone(row.identity_ciphertext)
        self.assertIsNone(row.identity_digest)

    def test_historical_identity_cannot_block_edit_if_its_ciphertext_is_unreadable(self):
        row=self.register_self()
        row.identity_ciphertext='unreadable-historical-ciphertext';row.identity_digest='historical-digest';self.session.commit()
        self.assertEqual(self.client.get('/franchisees/opportunities?area=edit').status_code,200)
        response=self.client.post('/franchisees/opportunities?area=edit',data=self.fields(action='save_profile',full_name='Saved Without Reading ID'))
        self.assertEqual(response.status_code,302)
        row=self.Registration.query.one()
        self.assertEqual((row.identity_ciphertext,row.identity_digest),('unreadable-historical-ciphertext','historical-digest'))

    def test_remaining_field_and_terms_validation_is_visible_without_partial_save(self):
        self.login(2)
        for field in ('full_name','cellphone','address','accept_terms'):
            response=self.client.post('/franchisees/opportunities',data=self.fields(**{field:''}))
            self.assertEqual(response.status_code,400,(field,response.text))
            self.assertIn('Registration was not saved.',response.text)
            self.assertEqual(self.Registration.query.count(),0)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields(occupation='x'*161)).status_code,400)
        row=self.register_self(sa_id='anything',email='ignored')
        self.assertTrue(row.is_active)

    def test_duplicate_registration_and_database_user_uniqueness_are_retained(self):
        row=self.register_self()
        number,history=row.franchisee_number,list(row.history)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,409)
        self.assertEqual(self.Registration.query.count(),1)
        self.assertEqual((row.franchisee_number,row.history),(number,history))
        with self.assertRaises(sa.exc.IntegrityError):
            self.session.execute(sa.text("""INSERT INTO franchise_registration
              (id,email,user_id,status,full_name,cellphone,address_ciphertext,created_at,submitted_at,created_by,franchisee_number,history)
              SELECT 999,'different@example.org',user_id,status,full_name,cellphone,address_ciphertext,created_at,submitted_at,created_by,'FR-9999',history
              FROM franchise_registration LIMIT 1"""))
        self.session.rollback()
        self.assertEqual(self.Registration.query.count(),1)

    def test_csrf_and_account_isolation_protect_save_and_edit(self):
        self.register_self()
        self.app.config['WTF_CSRF_ENABLED']=True
        self.assertEqual(self.client.post('/franchisees/opportunities?area=edit',data=self.fields(action='save_profile')).status_code,400)
        self.app.config['WTF_CSRF_ENABLED']=False
        self.login(3)
        self.assertEqual(self.client.get('/franchisees/opportunities?area=edit').status_code,409)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields(action='save_profile',user_id='2')).status_code,409)
        self.assertNotIn('Happiness Example',self.client.get('/franchisees/opportunities').text)
        self.login(None)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,401)
        self.assertEqual(self.Registration.query.count(),1)

    def test_admin_suspension_and_reinstatement_keep_identity_and_number(self):
        row=self.register_self();number=row.franchisee_number
        self.login(1)
        self.assertEqual(self.client.post(f'/admin/franchisees/{row.id}',data={'action':'suspend'}).status_code,302)
        self.login(2)
        self.assertEqual(self.client.post('/franchisees/opportunities?area=edit',data=self.fields(action='save_profile',full_name='Suspended Profile')).status_code,302)
        self.assertTrue(self.Registration.query.one().is_suspended)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,409)
        self.login(1)
        self.assertEqual(self.client.post(f'/admin/franchisees/{row.id}',data={'action':'reinstate'}).status_code,302)
        self.assertTrue(self.Registration.query.one().is_active)
        self.assertEqual(self.Registration.query.one().franchisee_number,number)

    def test_commit_failure_rolls_back_without_announcing_success(self):
        self.login(2)
        failure=sa.exc.OperationalError('private statement',{},types.SimpleNamespace(pgcode='42501'))
        with patch.object(self.session,'commit',side_effect=failure):
            response=self.client.post('/franchisees/opportunities',data=self.fields())
        self.assertEqual(response.status_code,503)
        self.assertIn('lacks a required registration permission',response.text)
        self.assertNotIn('private statement',response.text)
        self.assertEqual(self.Registration.query.count(),0)

    def test_existing_clients_earnings_and_programme_records_are_readonly(self):
        self.register_self()
        self.session.add_all([self.Shop(id=11,user_id=2,business_name='Own business',trade='boq'),self.Shop(id=12,user_id=3,business_name='Private business',trade='boq'),
            self.Subject(id=1,slug='reading',name='Reading',is_active=1,is_hidden_on_bridge=False),self.Enrollment(id=1,user_id=2,subject_id=1,status='active'),
            self.License(id=1,user_id=2,subject_id=1,total_seats=10,used_seats=3),
            self.Document(id=1,user_id=2,shop_id=11,kind='boq_section',document_key='own-section',snapshot={'commercial_terms':{'operator_retains_cents':3700}},created_at=self.flow.now()),
            self.Document(id=2,user_id=3,shop_id=12,kind='boq_section',document_key='foreign-section',snapshot={'commercial_terms':{'operator_retains_cents':9900}},created_at=self.flow.now())])
        self.session.commit()
        before=self.session.execute(sa.text('SELECT to_jsonb(d)::text FROM protrade_document d ORDER BY id')).all()
        for area in ('clients','protrade'):
            page=self.client.get('/franchisees/opportunities?area='+area)
            self.assertIn('Own business',page.text);self.assertNotIn('Private business',page.text)
        page=self.client.get('/franchisees/opportunities?area=earnings')
        self.assertIn('R37.00',page.text);self.assertNotIn('R99.00',page.text)
        self.assertIn('Reading',self.client.get('/franchisees/opportunities?area=programmes').text)
        self.assertEqual(before,self.session.execute(sa.text('SELECT to_jsonb(d)::text FROM protrade_document d ORDER BY id')).all())
        self.assertEqual(self.Shop.query.count(),2);self.assertEqual(self.License.query.count(),1)

    def test_legacy_invitation_requires_ait_login_and_uses_same_simplified_form(self):
        token=self.invite()
        row=self.Registration.query.one();identifier=row.id
        self.login(None)
        self.assertEqual(self.client.get('/franchisees/register/'+token).status_code,401)
        self.login(2)
        self.assertEqual(self.client.get('/franchisees/register/'+token).status_code,302)
        self.register_self()
        self.assertEqual(self.Registration.query.one().id,identifier)
        self.assertIsNone(self.Registration.query.one().identity_ciphertext)

    def test_schema_pending_prevents_failed_insert_and_never_fabricates_identity(self):
        self.session.execute(sa.text("ALTER TABLE franchise_registration DROP CONSTRAINT ck_franchisee_submitted"))
        self.session.execute(sa.text("ALTER TABLE franchise_registration ADD CONSTRAINT ck_franchisee_submitted CHECK (status='Invited' OR (full_name IS NOT NULL AND cellphone IS NOT NULL AND identity_ciphertext IS NOT NULL AND address_ciphertext IS NOT NULL AND identity_digest IS NOT NULL AND submitted_at IS NOT NULL AND invitation_digest IS NULL))"));self.session.commit()
        self.login(2)
        page=self.client.get('/franchisees/opportunities?area=register')
        self.assertIn('franchise_registration_003',page.text)
        self.assertTrue(html.fromstring(page.text).xpath('//button[@type="submit"][@disabled]'))
        response=self.client.post('/franchisees/opportunities',data=self.fields())
        self.assertEqual(response.status_code,503)
        self.assertEqual(self.Registration.query.count(),0)
        self.assertFalse(self.session.execute(sa.text('SELECT is_called FROM ait_franchisee_number_seq')).scalar())

    def test_migration_003_preserves_history_and_other_constraints_and_rolls_back(self):
        row=self.legacy();before=self.session.execute(sa.text('SELECT to_jsonb(r)::text FROM franchise_registration r')).scalar()
        self.session.execute(sa.text('ALTER TABLE franchise_registration DROP CONSTRAINT ck_franchisee_submitted'))
        self.session.execute(sa.text("ALTER TABLE franchise_registration ADD CONSTRAINT ck_franchisee_submitted CHECK (status='Invited' OR (full_name IS NOT NULL AND cellphone IS NOT NULL AND identity_ciphertext IS NOT NULL AND address_ciphertext IS NOT NULL AND identity_digest IS NOT NULL AND submitted_at IS NOT NULL AND invitation_digest IS NULL))"))
        self.session.execute(sa.text('ALTER TABLE franchise_registration ADD CONSTRAINT ait_franchisee_registration_identity_digest_key UNIQUE(identity_digest)'));self.session.commit()
        constraints=self.conn.execute(sa.text("SELECT oid,conname,contype,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='franchise_registration'::regclass AND conname NOT IN ('ck_franchisee_submitted','ait_franchisee_registration_identity_digest_key') ORDER BY oid")).all();self.conn.rollback()
        revision=module('simplify_migration_test','migrations/versions/franchise_registration_003.py')
        revision.op=Operations(MigrationContext.configure(self.conn))
        with self.assertRaisesRegex(RuntimeError,'explicit migration authorisation'):revision.upgrade()
        self.conn.rollback()
        with self.assertRaisesRegex(RuntimeError,'rollback test'):
            with self.conn.begin():
                revision.op=Operations(MigrationContext.configure(self.conn,opts={'franchise_registration_simplify_approved':True}));revision.upgrade()
                raise RuntimeError('rollback test')
        self.assertEqual(self.conn.scalar(sa.text("SELECT count(*) FROM pg_constraint WHERE conrelid='franchise_registration'::regclass AND conname='ait_franchisee_registration_identity_digest_key'")),1)
        self.conn.rollback()
        with self.conn.begin():
            revision.op=Operations(MigrationContext.configure(self.conn,opts={'franchise_registration_simplify_approved':True}));revision.upgrade()
        self.assertEqual(before,self.conn.scalar(sa.text('SELECT to_jsonb(r)::text FROM franchise_registration r')))
        self.assertEqual(constraints,self.conn.execute(sa.text("SELECT oid,conname,contype,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='franchise_registration'::regclass AND conname NOT IN ('ck_franchisee_submitted','ait_franchisee_registration_identity_digest_key') ORDER BY oid")).all())
        self.assertEqual(self.conn.scalar(sa.text("SELECT count(*) FROM pg_constraint WHERE conrelid='franchise_registration'::regclass AND conname='ait_franchisee_registration_identity_digest_key'")),0)
        self.conn.rollback()
        new=self.register_self(3,full_name='No ID Profile')
        self.assertIsNone(new.identity_digest)

    def test_browser_save_edit_cancel_and_fresh_visit_without_id_or_email_fields(self):
        import threading
        from playwright.sync_api import sync_playwright
        from werkzeug.serving import make_server,WSGIRequestHandler
        class Quiet(WSGIRequestHandler):
            def log_request(self,*args,**kwargs):pass
        self.login(2);self.app.jinja_loader=FileSystemLoader(ROOT/'templates');self.app.jinja_env.cache.clear()
        self.app.config['WTF_CSRF_ENABLED']=True
        server=make_server('127.0.0.1',0,self.app,request_handler=Quiet,ssl_context='adhoc')
        origin=f'https://127.0.0.1:{server.server_port}'
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            with sync_playwright() as runtime:
                browser=runtime.chromium.launch(headless=True)
                try:
                    page=browser.new_page(ignore_https_errors=True,viewport={'width':1280,'height':900})
                    cookie=self.app.session_interface.get_signing_serializer(self.app).dumps({'_user_id':'2','_fresh':True})
                    page.context.add_cookies([{'name':self.app.config['SESSION_COOKIE_NAME'],'value':cookie,'url':origin}])
                    page.route('https://**/*',lambda route:route.continue_() if route.request.url.startswith(origin+'/') else route.abort())
                    page.goto(origin+'/franchisees/opportunities?area=register')
                    self.assertEqual(page.locator('[name=sa_id],[name=email]').count(),0)
                    for field,value in self.fields().items():
                        if field=='accept_terms':page.locator('[name=accept_terms]').check()
                        else:page.locator('[name='+field+']').fill(value)
                    page.get_by_role('button',name='Save',exact=True).click()
                    page.wait_for_url('**/franchisees/opportunities?area=register')
                    table=page.locator('#franchise-register-table')
                    self.assertIn('FR-0001',table.inner_text());self.assertIn('happiness@example.org',table.inner_text())
                    table.get_by_role('link',name='Edit',exact=True).click()
                    self.assertEqual(page.locator('[name=sa_id],[name=email]').count(),0)
                    self.assertEqual(page.locator('[name=full_name]').input_value(),'Happiness Example')
                    page.locator('[name=full_name]').fill('Cancelled')
                    page.get_by_role('link',name='Cancel',exact=True).click()
                    self.assertNotIn('Cancelled',table.inner_text())
                    table.get_by_role('link',name='Edit',exact=True).click()
                    page.locator('[name=full_name]').fill('Edited Name')
                    page.get_by_role('button',name='Save',exact=True).click()
                    page.wait_for_url('**/franchisees/opportunities?area=register')
                    page.reload()
                    self.assertIn('Edited Name',table.inner_text());self.assertIn('FR-0001',table.inner_text())
                    page.screenshot(path=str(ROOT/'tmp/franchise-simplified-registration.png'),full_page=True)
                finally:browser.close()
        finally:server.shutdown();thread.join(timeout=5);server.server_close()
