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


def valid_id(prefix='800101500908'):
    for digit in '0123456789':
        numbers = list(map(int, prefix + digit))
        if (sum(numbers[::2]) + sum(2*n//10+2*n % 10 for n in numbers[1::2])) % 10 == 0:
            return prefix + digit


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
        self.conn.exec_driver_sql('SET search_path TO pg_temp')
        migration = module('franchisee_migration_test','migrations/versions/ait_franchisee_001.py')
        config=Config();config.attributes['franchisee_registration_approved']=True
        with EnvironmentContext(config,ScriptDirectory(str(ROOT/'migrations'))) as environment:
            environment.configure(connection=self.conn)
            migration.op=Operations(environment.get_context());migration.upgrade()
        for name in ['ait_franchisee_registration','ait_franchisee_number_seq']:
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
        self.Shop,self.Document=Shop,Document
        fake_db = types.SimpleNamespace(Model=base,session=sessions)
        for name in ('Column','Integer','String','Text','ForeignKey','DateTime','JSON','CheckConstraint'):
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

    def fields(self,email='happiness@example.org',**changes):
        values=dict(full_name='Happiness Example',cellphone='082 123 4567',email=email,address='12 Synthetic Test Street',sa_id=valid_id(),occupation='Contractor')
        values.update(changes);return values

    def submit(self,token,**fields):
        self.login(None)
        return self.client.post('/franchisees/register/'+token,data=self.fields(**fields))

    def test_complete_workflow_links_existing_user_and_protects_details(self):
        before=self.session.execute(sa.text('SELECT * FROM "user" ORDER BY id')).all()
        token=self.invite()
        row=self.Registration.query.one();self.assertEqual(row.user_id,2)
        self.assertNotEqual(row.invitation_digest,token);self.assertEqual(row.status,'Invited')
        self.assertNotIn(token,json.dumps(row.history))
        self.login(None)
        form=self.client.get('/franchisees/register/'+token)
        self.assertEqual(form.status_code,200)
        self.assertIn('no-store',form.headers['Cache-Control'])
        self.assertIn("script-src 'none'",form.headers['Content-Security-Policy'])
        self.assertNotIn('Existing Happiness',form.text)
        self.assertEqual(self.submit(token,user_id='1',status='Approved',franchisee_number='FR-FAKE').status_code,302)
        row=self.Registration.query.one();self.assertEqual(row.status,'Submitted')
        self.assertIsNone(row.history[-1]['actor_id'])  # Anonymous bearer is not an authenticated login.
        self.assertIsNone(row.invitation_digest);self.assertIsNone(row.franchisee_number)
        self.assertNotIn(valid_id(),row.identity_ciphertext);self.assertNotIn('Test Street',row.address_ciphertext)
        self.assertEqual(self.submit(token).status_code,400)
        self.login(1)
        listing=self.client.get('/admin/franchisees');self.assertNotIn(valid_id(),listing.text);self.assertNotIn('Test Street',listing.text)
        detail=self.client.get('/admin/franchisees/'+str(row.id));self.assertIn(valid_id(),detail.text);self.assertIn('Test Street',detail.text)
        self.assertEqual(self.client.post('/admin/franchisees/'+str(row.id),data={'action':'approve'}).status_code,302)
        row=self.Registration.query.one();self.assertEqual(row.franchisee_number,'FR-0001');self.assertEqual(row.reviewed_by,1)
        self.assertEqual(row.user_id,2);self.assertIsNotNone(row.submitted_at)
        self.assertEqual(self.client.post('/admin/franchisees/'+str(row.id),data={'action':'approve'}).status_code,409)
        self.assertEqual(self.session.execute(sa.text('SELECT * FROM "user" ORDER BY id')).all(),before)

    def test_opportunities_navigation_is_scoped_and_activates_automatically(self):
        for path in ['/franchisees/opportunities','/franchisees/opportunities/register']:
            self.login(None)
            self.assertEqual(self.client.get(path).status_code,401)
        for uid in [1,2,3]:
            self.login(uid)
            response=self.client.get('/franchisees/opportunities')
            self.assertEqual(response.status_code,200)
            page=html.fromstring(response.text)
            self.assertEqual(page.xpath('//nav[@aria-label="Franchisee working areas"]/a/text()'),['Register','Clients','Earnings','ProTrade'])
            self.assertEqual(len(page.xpath('//section[@id="working-area"][@hidden]')),1)
        self.assertEqual(self.Registration.query.count(),0)
        token=self.invite()
        self.login(2)
        self.assertEqual(self.client.get('/franchisees/opportunities/register').location,'/franchisees/opportunities?area=register')
        response=self.client.get('/franchisees/opportunities?area=register')
        self.assertIn('name="sa_id"',response.text)
        self.assertNotIn(token,response.text)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,302)
        self.assertEqual(self.client.get('/franchisees/register/'+token).status_code,400)
        profile=self.client.get('/franchisees/opportunities?area=register')
        self.assertIn('My Profile',profile.text)
        self.assertNotIn('awaiting AIT Admin approval',profile.text)
        self.assertIn('Open ProTrade',self.client.get('/franchisees/opportunities?area=protrade').text)
        self.login(3)
        response=self.client.get('/franchisees/opportunities?area=register')
        self.assertIn('name="sa_id"',response.text)
        self.assertNotIn('Happiness',response.text)
        self.assertEqual(self.Registration.query.count(),1)

    def test_dashboard_registration_validates_encrypts_and_prevents_duplicate_submission(self):
        self.login(2)
        for field in ['full_name','cellphone','email','address','sa_id']:
            with self.subTest(field=field):
                self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields(**{field:''})).status_code,400)
        self.assertEqual(self.Registration.query.count(),0)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields(email='other@example.org')).status_code,400)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,302)
        row=self.Registration.query.one()
        self.assertEqual((row.user_id,row.created_by,row.status),(2,2,'Approved'))
        self.assertEqual(row.franchisee_number,'FR-0001')
        self.assertIsNone(row.reviewed_by)
        self.assertTrue(row.is_active)
        self.assertNotIn(valid_id(),row.identity_ciphertext)
        self.assertEqual(row.history[-1]['actor_id'],2)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,409)
        self.login(3)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields(email='other@example.org')).status_code,409)
        self.assertEqual(self.Registration.query.count(),1)
        self.app.config['WTF_CSRF_ENABLED']=True
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields(email='other@example.org')).status_code,400)
        self.app.config['WTF_CSRF_ENABLED']=False
        self.app.config['FRANCHISEE_PII_KEY']=None
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields(email='other@example.org')).status_code,503)
        self.assertEqual(self.Registration.query.count(),1)

    def test_clients_and_historical_earnings_are_readonly_and_user_scoped(self):
        self.login(2)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,302)
        row=self.Registration.query.one()
        self.login(1)
        self.client.post(f'/admin/franchisees/{row.id}',data={'action':'approve'})
        self.session.add_all([self.Shop(id=11,user_id=2,business_name='Own business',trade='boq'),
                              self.Shop(id=12,user_id=3,business_name='Private other business',trade='boq')])
        for identifier,user,shop,amount in [(1,2,11,3700),(2,3,12,9900),(3,2,12,8800)]:
            self.session.add(self.Document(id=identifier,user_id=user,shop_id=shop,kind='boq_section',document_key=f'section-{identifier}',
                                          snapshot={'commercial_terms':{'operator_retains_cents':amount}},created_at=self.flow.now()))
        self.session.add(self.Document(id=4,user_id=2,shop_id=11,kind='boq_section',document_key='legacy-section',snapshot={},created_at=self.flow.now()))
        self.session.commit()
        before=self.session.execute(sa.text('SELECT id,snapshot::text FROM protrade_document ORDER BY id')).all()
        self.login(2)
        response=self.client.get('/franchisees/opportunities?area=clients')
        self.assertIn('Own business',response.text)
        self.assertNotIn('Private other business',response.text)
        response=self.client.get('/franchisees/opportunities?area=earnings')
        self.assertIn('R37.00',response.text)
        self.assertIn('Terms not recorded',response.text)
        for private in ['Private other business','R99.00','R88.00']:
            self.assertNotIn(private,response.text)
        self.assertEqual(before,self.session.execute(sa.text('SELECT id,snapshot::text FROM protrade_document ORDER BY id')).all())

    def test_https_csrf_and_validation_errors_stay_inside_dashboard(self):
        self.login(2)
        self.app.config['WTF_CSRF_ENABLED']=True
        url='/franchisees/opportunities?area=register'
        origin='https://localhost'
        page=self.client.get(url,base_url=origin)
        self.assertEqual(page.headers['Referrer-Policy'],'strict-origin')
        token=html.fromstring(page.text).xpath('//input[@name="csrf_token"]/@value')[0]
        data=self.fields(csrf_token=token)
        for fields,headers in [(self.fields(),{'Referer':origin+'/'}),(data,{}),(data,{'Referer':'https://other.example/'})]:
            response=self.client.post(url,base_url=origin,data=fields,headers=headers)
            self.assertEqual(response.status_code,400)
            self.assertIn('Franchisee Dashboard',response.text)
            self.assertIn('Your form security check failed',response.text)
            self.assertEqual(self.Registration.query.count(),0)
        self.app.config['WTF_CSRF_TIME_LIMIT']=-1
        response=self.client.post(url,base_url=origin,data=data,headers={'Referer':origin+'/'})
        self.assertEqual(response.status_code,400)
        self.assertIn('Reload Register',response.text)
        self.app.config['WTF_CSRF_TIME_LIMIT']=3600
        response=self.client.post(url,base_url=origin,data=self.fields(csrf_token=token,full_name=' '),headers={'Referer':origin+'/'})
        self.assertEqual(response.status_code,400)
        self.assertIn('Enter your full name',response.text)
        self.assertIn('role="alert"',response.text)
        response=self.client.post(url,base_url=origin,data=data,headers={'Referer':origin+'/'})
        self.assertEqual(response.status_code,302)
        row=self.Registration.query.one()
        self.assertTrue(row.is_active)
        self.assertEqual(row.franchisee_number,'FR-0001')

    def test_unique_numbers_admin_visibility_and_audited_suspension(self):
        before=self.session.execute(sa.text('SELECT * FROM "user" ORDER BY id')).all()
        for uid,email,identity in [(2,'happiness@example.org',valid_id()),(3,'other@example.org',valid_id('800102500908'))]:
            self.login(uid)
            response=self.client.post('/franchisees/opportunities',data=self.fields(email=email,sa_id=identity,user_id='1',status='Submitted',franchisee_number='FR-FAKE'))
            self.assertEqual(response.status_code,302)
        rows=self.Registration.query.order_by(self.Registration.id).all()
        self.assertEqual([row.franchisee_number for row in rows],['FR-0001','FR-0002'])
        identifier=rows[0].id
        original_history=list(rows[0].history)
        self.login(1)
        listing=self.client.get('/admin/franchisees')
        self.assertIn('FR-0001',listing.text)
        self.assertIn('FR-0002',listing.text)
        self.assertIn('Suspend franchisee',self.client.get(f'/admin/franchisees/{identifier}').text)
        self.assertEqual(self.client.post(f'/admin/franchisees/{identifier}',data={'action':'suspend'}).status_code,302)
        row=self.Registration.query.get(identifier)
        self.assertTrue(row.is_suspended)
        self.assertFalse(row.is_active)
        self.assertEqual(row.franchisee_number,'FR-0001')
        self.assertEqual(row.history[:-1],original_history)
        self.assertEqual(row.history[-1]['actor_id'],1)
        self.assertEqual(self.client.post(f'/admin/franchisees/{identifier}',data={'action':'suspend'}).status_code,409)
        self.login(2)
        for area in ['register','clients','earnings','protrade']:
            response=self.client.get('/franchisees/opportunities?area='+area)
            self.assertIn('suspended by AIT Admin',response.text)
            self.assertNotIn('Open ProTrade',response.text)
        self.assertEqual(self.client.post('/franchisees/opportunities',data=self.fields()).status_code,409)
        self.assertEqual(self.client.post(f'/admin/franchisees/{identifier}',data={'action':'reinstate'}).status_code,403)
        self.login(1)
        self.assertEqual(self.client.post(f'/admin/franchisees/{identifier}',data={'action':'reinstate'}).status_code,302)
        row=self.Registration.query.get(identifier)
        self.assertTrue(row.is_active)
        self.assertEqual(row.franchisee_number,'FR-0001')
        self.assertEqual(row.history[-1]['action'],'reinstated')
        self.login(2)
        self.assertIn('Open ProTrade',self.client.get('/franchisees/opportunities?area=protrade').text)
        self.assertEqual(before,self.session.execute(sa.text('SELECT * FROM "user" ORDER BY id')).all())

    def test_previously_submitted_registration_can_activate_without_reentry(self):
        token=self.invite()
        self.assertEqual(self.submit(token).status_code,302)
        row=self.Registration.query.one()
        identity,address,history=row.identity_ciphertext,row.address_ciphertext,list(row.history)
        row.user_id=None  # A legacy submission made before its AIT account was linked.
        self.session.commit()
        self.login(2)
        page=self.client.get('/franchisees/opportunities?area=register')
        self.assertIn('Complete Registration',page.text)
        self.assertNotIn('awaiting AIT Admin approval',page.text)
        self.assertEqual(self.client.post('/franchisees/opportunities',data={'action':'activate'}).status_code,302)
        row=self.Registration.query.one()
        self.assertTrue(row.is_active)
        self.assertEqual(row.franchisee_number,'FR-0001')
        self.assertEqual(row.user_id,2)
        self.assertEqual((row.identity_ciphertext,row.address_ciphertext),(identity,address))
        self.assertEqual(row.history[:-1],history)
        self.assertEqual(self.client.post('/franchisees/opportunities',data={'action':'activate'}).status_code,409)

    def test_committed_registration_survives_fresh_session_and_dashboard_reload(self):
        self.login(2)
        commits=[]
        def committed(connection):commits.append(True)
        sa.event.listen(self.conn,'commit',committed)
        try:
            response=self.client.post('/franchisees/opportunities?area=register',data=self.fields())
        finally:
            sa.event.remove(self.conn,'commit',committed)
        self.assertEqual(response.status_code,302)
        self.assertTrue(commits)
        self.session.remove()
        with sa.orm.Session(bind=self.conn) as reader:
            saved=reader.execute(sa.text('SELECT user_id,status,franchisee_number,identity_ciphertext,address_ciphertext FROM ait_franchisee_registration')).one()
            self.assertEqual((saved.user_id,saved.status,saved.franchisee_number),(2,'Approved','FR-0001'))
            encryption=Fernet(self.app.config['FRANCHISEE_PII_KEY'].encode())
            self.assertEqual(encryption.decrypt(saved.identity_ciphertext.encode()).decode(),valid_id())
            self.assertEqual(encryption.decrypt(saved.address_ciphertext.encode()).decode(),'12 Synthetic Test Street')
        fresh=self.app.test_client()
        with fresh.session_transaction() as values:
            values['_user_id']='2';values['_fresh']=True
        dashboard=fresh.get('/franchisees/opportunities')
        self.assertIn('My Profile',dashboard.text)
        profile=fresh.get('/franchisees/opportunities?area=register')
        self.assertEqual(profile.status_code,200)
        self.assertIn('FR-0001',profile.text)
        for value in ['Happiness Example','0821234567','happiness@example.org','Contractor','12 Synthetic Test Street',valid_id()]:
            self.assertIn(value,profile.text)
        for area in ['clients','earnings','protrade']:
            response=fresh.get('/franchisees/opportunities?area='+area)
            self.assertEqual(response.status_code,200)
            self.assertNotIn('Complete Register to activate',response.text)
        self.assertEqual(self.Registration.query.count(),1)

    def test_failed_commit_never_announces_success_and_rolls_back(self):
        self.login(2)
        failure=sa.exc.OperationalError('COMMIT',{},Exception('private database details'))
        with patch.object(self.session,'commit',side_effect=failure):
            response=self.client.post('/franchisees/opportunities?area=register',data=self.fields())
        self.assertEqual(response.status_code,503)
        self.assertIn('Registration could not be confirmed',response.text)
        self.assertNotIn('Your franchisee account is active',response.text)
        self.assertNotIn('private database details',response.text)
        self.assertEqual(self.Registration.query.count(),0)

    def test_validation_retains_values_and_marks_only_incorrect_fields(self):
        self.login(2)
        entered=self.fields(full_name=' ',cellphone='082 123 4567',address='12 Test Street & Flat <3>\nSecond line',occupation='Builder & carpenter')
        response=self.client.post('/franchisees/opportunities?area=register',data=entered)
        self.assertEqual(response.status_code,400)
        self.assertNotIn('Location',response.headers)
        page=html.fromstring(response.text)
        for name,value in entered.items():
            element=page.xpath('//*[@name="'+name+'"]')[0]
            self.assertEqual(element.text if element.tag=='textarea' else element.get('value'),value)
        self.assertEqual(page.xpath('//*[@aria-invalid="true"]/@name'),['full_name'])
        error=page.xpath('//*[@id="franchisee-full_name-error"]')[0]
        self.assertIn('Enter your full name',error.text_content())
        self.assertEqual(error.getparent().tag,'label')
        self.assertEqual(self.Registration.query.count(),0)
        with self.client.session_transaction() as values:
            for private in [valid_id(),entered['address'],entered['cellphone']]:
                self.assertNotIn(private,json.dumps(dict(values)))
        entered['full_name']='Bill'
        self.assertEqual(self.client.post('/franchisees/opportunities?area=register',data=entered).status_code,302)
        self.session.remove()
        profile=self.client.get('/franchisees/opportunities?area=register')
        self.assertIn('FR-0001',profile.text)
        self.assertIn('My Profile',profile.text)
        text=html.fromstring(profile.text).text_content()
        for value in ['Bill','0821234567',entered['address'],entered['occupation']]:
            self.assertIn(value,text)
        self.login(1)
        row=self.Registration.query.one()
        detail=self.client.get(f'/admin/franchisees/{row.id}')
        self.assertIn('Bill',detail.text)
        self.assertIn(entered['address'],html.fromstring(detail.text).text_content())

    def test_names_follow_existing_ait_required_value_pattern(self):
        self.login(2)
        for name in ['Bill',"Jean-Luc O?Neill",'??','  Nandi   Dlamini  ']:
            with self.subTest(name=name):
                response=self.client.post('/franchisees/opportunities?area=register',data=self.fields(full_name=name,cellphone='invalid'))
                self.assertEqual(response.status_code,400)
                page=html.fromstring(response.text)
                self.assertEqual(page.xpath('//*[@aria-invalid="true"]/@name'),['cellphone'])
                self.assertEqual(page.xpath('//input[@name="full_name"]/@value'),[name])
        response=self.client.post('/franchisees/opportunities?area=register',data=self.fields(full_name='x'*161,sa_id='bad',cellphone='bad',address='x',occupation='x'*161))
        self.assertEqual(response.status_code,400)
        page=html.fromstring(response.text)
        self.assertEqual(set(page.xpath('//*[@aria-invalid="true"]/@name')),{'full_name','sa_id','cellphone','address','occupation'})
        self.assertEqual(self.Registration.query.count(),0)

    def test_bridge_franchisee_entry_is_paused_for_ordinary_users_and_admins(self):
        for endpoint in ['admin_bp.global_settings','admin_bp.programs_index','admin_bp.security_dashboard']:
            self.app.add_url_rule('/test/'+endpoint,endpoint=endpoint,view_func=lambda:'Existing admin destination')
        for admin in [False,True]:
            with self.app.test_request_context('/bridge'):
                markup=render_template('auth/bridge_dashboard.html',user=self.User(id=99,name='Example'),
                                       subjects=[],platform_admin_bridge=admin)
            page=html.fromstring(markup)
            links=page.xpath('//a[normalize-space(.)="AIT Franchise Opportunities"]')
            self.assertEqual(len(links),0)

    def test_browser_opportunities_and_compact_admin_register(self):
        import threading
        from playwright.sync_api import sync_playwright
        from werkzeug.serving import make_server, WSGIRequestHandler
        class QuietHandler(WSGIRequestHandler):
            def log_request(self,*args,**kwargs):pass
        token=self.invite()
        self.assertEqual(self.submit(token).status_code,302)
        self.login(1)
        row_id=self.Registration.query.one().id
        self.assertEqual(self.client.post(f'/admin/franchisees/{row_id}',data={'action':'approve'}).status_code,302)
        self.session.rollback()
        self.app.jinja_loader=FileSystemLoader(ROOT/'templates')
        self.app.jinja_env.cache.clear()
        self.app.config['WTF_CSRF_ENABLED']=True
        server=make_server('127.0.0.1',0,self.app,request_handler=QuietHandler,ssl_context='adhoc')
        origin=f'https://127.0.0.1:{server.server_port}'
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            with sync_playwright() as runtime:
                browser=runtime.chromium.launch(headless=True)
                try:
                    page=browser.new_page(viewport={'width':1280,'height':900},ignore_https_errors=True)
                    referrers=[]
                    page.on('request',lambda req:referrers.append(req.headers.get('referer')) if req.method=='POST' and '/franchisees/opportunities' in req.url else None)
                    page.route('https://**/*',lambda route:route.continue_() if route.request.url.startswith(origin+'/') else route.abort())
                    def login(uid):
                        cookie=self.app.session_interface.get_signing_serializer(self.app).dumps({'_user_id':str(uid),'_fresh':True})
                        page.context.add_cookies([{'name':self.app.config['SESSION_COOKIE_NAME'],'value':cookie,'url':origin}])
                    login(3)
                    page.goto(origin+'/franchisees/opportunities')
                    menu=page.get_by_role('navigation',name='Franchisee working areas')
                    positions=menu.locator('a').evaluate_all('(links) => links.map(link => link.getBoundingClientRect().top)')
                    self.assertEqual(len(set(positions)),1)
                    self.assertFalse(page.locator('#working-area').is_visible())
                    menu.get_by_role('link',name='Register',exact=True).click()
                    self.assertTrue(page.locator('#working-area').is_visible())
                    entered=self.fields(email='other@example.org',full_name='Bill',sa_id=valid_id('800102500908'))
                    for field,value in entered.items():
                        if field!='email':page.locator('[name='+field+']').fill(value)
                    page.locator('[name=cellphone]').fill('invalid')
                    page.get_by_role('button',name='Submit registration',exact=True).click()
                    page.locator('#franchisee-cellphone-error').wait_for()
                    for field,value in entered.items():
                        expected='invalid' if field=='cellphone' else value
                        self.assertEqual(page.locator('[name='+field+']').input_value(),expected)
                    page.screenshot(path=str(ROOT/'tmp/franchisee-validation.png'),full_page=True)
                    page.locator('[name=cellphone]').fill(entered['cellphone'])
                    page.get_by_role('button',name='Submit registration',exact=True).click()
                    page.wait_for_url('**/franchisees/opportunities?area=register')
                    self.assertEqual(referrers,[origin+'/',origin+'/'])
                    self.assertIn('My Profile',page.locator('main').inner_text())
                    self.assertNotIn('Admin approval',page.locator('main').inner_text())
                    page.get_by_role('link',name='Close',exact=True).click()
                    self.assertFalse(page.locator('#working-area').is_visible())
                    artifact=ROOT/'tmp/franchisee-dashboard.png'
                    page.screenshot(path=str(artifact),full_page=True)
                    login(2)
                    page.goto(origin+'/franchisees/opportunities?area=protrade')
                    page.get_by_role('link',name='Open ProTrade',exact=True).click()
                    self.assertIn('Existing ProTrade entry',page.content())
                    login(1)
                    page.goto(origin+'/admin/franchisees')
                    self.assertEqual(page.locator('tbody tr').count(),2)
                    self.assertEqual(page.locator('h1').inner_text(),'Franchisee Register')
                    self.assertFalse(page.locator('#bug-modal').is_visible())
                    self.assertEqual(page.locator('table').evaluate('(table) => getComputedStyle(table).fontSize'),'14px')
                    self.assertLess(page.locator('tbody tr').first.bounding_box()['height'],150)
                    artifact=ROOT/'tmp/franchisee-register-compact.png'
                    artifact.parent.mkdir(exist_ok=True)
                    page.screenshot(path=str(artifact),full_page=True)
                    page.locator('tr').filter(has_text='Happiness Example').get_by_role('link',name='View Details',exact=True).click()
                    self.assertIn('Happiness Example',page.locator('main').inner_text())
                    page.get_by_role('link',name='Back to Franchisee Register',exact=True).click()
                    page.set_viewport_size({'width':375,'height':850})
                    self.assertTrue(page.get_by_role('link',name='View Details',exact=True).first.is_visible())
                    self.assertTrue(page.evaluate('document.documentElement.scrollWidth <= window.innerWidth'))
                    page.get_by_role('link',name='Back to Admin Hub',exact=True).click()
                    self.assertIn('Admin Hub',page.content())
                finally:browser.close()
        finally:
            server.shutdown();thread.join(timeout=5);server.server_close()

    def test_required_fields_and_identity_validation(self):
        token=self.invite()
        for field in ['full_name','cellphone','email','address','sa_id']:
            with self.subTest(field=field):self.assertEqual(self.submit(token,**{field:''}).status_code,400)
        for identity in ['8001325009080','123','8001015009080']:
            if identity!=valid_id():self.assertEqual(self.submit(token,sa_id=identity).status_code,400)
        self.assertEqual(self.Registration.query.one().status,'Invited')
        self.assertEqual(self.submit(token,occupation='').status_code,302)

    def test_duplicates_expiry_resend_and_single_use(self):
        token=self.invite()
        self.assertEqual(self.client.post('/admin/franchisees',data={'email':'HAPPINESS@example.org'}).status_code,409)
        row=self.Registration.query.one();row.invitation_expires_at=self.flow.now()-timedelta(seconds=1);self.session.commit()
        self.assertEqual(self.submit(token).status_code,400)
        self.login(1)
        self.assertEqual(self.client.post(f'/admin/franchisees/{row.id}/resend').status_code,302)
        fresh=re.search(r'/franchisees/register/([A-Za-z0-9_-]+)',self.messages[-1].body).group(1)
        self.assertNotEqual(fresh,token);self.assertEqual(self.submit(token).status_code,400)
        self.assertEqual(self.submit(fresh).status_code,302)
        second=self.invite('second@example.org')
        self.assertEqual(self.submit(second,email='second@example.org').status_code,409)
        self.assertEqual(self.Registration.query.filter_by(email='second@example.org').one().status,'Invited')

    def test_rejection_and_numbers_only_for_approved_records(self):
        for index,status in enumerate(['reject','approve','approve'],1):
            email=f'person{index}@example.org';token=self.invite(email)
            self.assertEqual(self.submit(token,email=email,sa_id=valid_id(f'80010{index}500908')).status_code,302)
            row=self.Registration.query.filter_by(email=email).one();self.login(1)
            self.assertEqual(self.client.post(f'/admin/franchisees/{row.id}',data={'action':status}).status_code,302)
            row=self.Registration.query.filter_by(email=email).one()
            self.assertEqual(row.status,'Rejected' if status=='reject' else 'Approved')
            self.assertEqual(row.franchisee_number,None if status=='reject' else f'FR-{index-1:04d}')
        with self.assertRaises(sa.exc.IntegrityError):
            self.session.execute(sa.text("UPDATE ait_franchisee_registration SET franchisee_number='FR-0001' WHERE franchisee_number='FR-0002'"));self.session.commit()
        self.session.rollback()

    def test_unauthorised_access_wrong_account_and_csrf(self):
        token=self.invite();row=self.Registration.query.one()
        for uid,status in [(None,401),(2,403),(3,403)]:
            self.login(uid)
            for path in ['/admin/franchisees',f'/admin/franchisees/{row.id}',f'/admin/franchisees/{row.id}/resend']:
                method=self.client.post if path.endswith('/resend') else self.client.get
                response=method(path);self.assertEqual(response.status_code,status)
                self.assertNotIn('happiness@example.org',response.text)
        self.login(3);self.assertEqual(self.client.get('/franchisees/register/'+token).status_code,403)
        self.login(2);self.assertEqual(self.client.get('/franchisees/register/'+token).status_code,200)
        self.app.config['WTF_CSRF_ENABLED']=True
        self.assertEqual(self.client.post('/franchisees/register/'+token,data=self.fields()).status_code,400)
        self.login(1);self.assertEqual(self.client.post('/admin/franchisees',data={'email':'fresh@example.org'}).status_code,400)

    def test_missing_key_and_delivery_failure_are_safe(self):
        self.app.config['FRANCHISEE_PII_KEY']=None
        self.assertEqual(self.client.post('/admin/franchisees',data={'email':'happiness@example.org'}).status_code,503)
        self.assertEqual(self.Registration.query.count(),0);self.assertEqual(self.messages,[])
        self.app.config['FRANCHISEE_PII_KEY']=Fernet.generate_key().decode()
        with patch.object(self.mail,'send',side_effect=RuntimeError('Do not log recipient or token')):
            response=self.client.post('/admin/franchisees',data={'email':'happiness@example.org'})
        self.assertEqual(response.status_code,503);self.assertNotIn('Do not log',response.text)
        self.assertEqual(self.Registration.query.count(),1)
        self.assertEqual(self.Registration.query.one().status,'Invited')

    def test_migration_cannot_run_from_unapproved_startup_context(self):
        migration=module('franchisee_unapproved_test','migrations/versions/ait_franchisee_001.py')
        migration.op=Operations(MigrationContext.configure(self.conn))
        with self.assertRaisesRegex(RuntimeError,'explicitly approved'):
            migration.upgrade()
        self.session.execute(sa.text('DROP SEQUENCE ait_franchisee_number_seq'));self.session.commit()
        self.assertEqual(self.client.get('/admin/franchisees').status_code,503)

    def test_existing_log_filter_omits_private_payloads_and_token(self):
        privacy=module('franchisee_privacy_test','app/logging_security.py')
        token='sensitive-invitation-token'
        with self.app.test_request_context('/franchisees/register/'+token,method='POST',data=self.fields()):
            record=logging.LogRecord('test',logging.ERROR,'test',1,'body=%s', (self.fields(),),None)
            record.exc_text='secret '+valid_id()+' '+token
            privacy.RequestSecretFilter().filter(record)
            message=record.getMessage()
            for secret in [valid_id(),token,'Test Street','Happiness','happiness@example.org']:self.assertNotIn(secret,message)
            self.assertIsNone(record.exc_text)
            self.assertIn('/franchisees/register/<token>',message)

    def test_browser_invitation_form_and_admin_review(self):
        import threading
        from playwright.sync_api import sync_playwright
        from werkzeug.serving import make_server, WSGIRequestHandler
        class QuietHandler(WSGIRequestHandler):
            def log_request(self,*args,**kwargs):pass
        token=self.invite();self.login(None)
        self.app.config['WTF_CSRF_ENABLED']=True
        server=make_server('127.0.0.1',0,self.app,request_handler=QuietHandler)
        origin=f'http://127.0.0.1:{server.server_port}'
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            with sync_playwright() as runtime:
                browser=runtime.chromium.launch(headless=True)
                try:
                    page=browser.new_page(viewport={'width':1280,'height':900})
                    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                    page.goto(origin+'/franchisees/register/'+token)
                    for field,value in self.fields().items():page.locator('[name='+field+']').fill(value)
                    page.get_by_role('button',name='Submit registration').click()
                    page.wait_for_url('**/franchisees/submitted')
                    self.assertIn('Registration submitted',page.locator('h1').inner_text())
                    self.assertNotIn(valid_id(),page.content())
                    row_id=self.Registration.query.one().id;self.session.rollback()
                    cookie=self.app.session_interface.get_signing_serializer(self.app).dumps({'_user_id':'1','_fresh':True})
                    page.context.add_cookies([{'name':self.app.config['SESSION_COOKIE_NAME'],'value':cookie,'url':origin}])
                    page.goto(f'{origin}/admin/franchisees/{row_id}')
                    self.assertIn('Happiness Example',page.content())
                    self.assertIn('SAST',page.content())
                    artifact=ROOT/'tmp'/'franchisee-review.png';artifact.parent.mkdir(exist_ok=True)
                    page.screenshot(path=str(artifact),full_page=True)
                    page.get_by_role('button',name='Approve registration',exact=True).click()
                    page.wait_for_function("document.body.textContent.includes('FR-0001')")
                    self.assertEqual(self.Registration.query.one().status,'Approved')
                    self.assertEqual(errors,[])
                finally:browser.close()
        finally:
            server.shutdown();thread.join(timeout=5);server.server_close()
