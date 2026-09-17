"""Real local PostgreSQL requests, savepoint commits inside rolled-back fixtures.

Never invokes the application factory. No user records are changed. Immutable
review events disappear only by rolling back their original test transaction.
"""
from pathlib import Path
import sys
import types
import re
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
import threading
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from flask import Flask
from flask_login import UserMixin

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import retirement_stage2_local as local
import retirement_waiting_room_transition as transition
engine=local.local_engine()
with engine.connect() as c:
    local.identity(c)
    transition.verify(c)
    USERS=c.execute(sa.text('SELECT id FROM public."user" ORDER BY id LIMIT 3')).scalars().all()
    assert len(USERS)==3
    assert c.execute(sa.text('SELECT count(*) FROM public.retirement_organisation')).scalar_one()==0, 'Zero-home test requires empty local Retirement installation'
for name,directory in (('app','app'),('app.models','app/models')):
    package=types.ModuleType(name); package.__path__=[str(ROOT/directory)];sys.modules[name]=package
from app.extensions import db, login_manager, csrf
from app.models.retire import RetirementMembership as Member, RetirementAssociationReview as Review
from app.program_retire import retire_bp
from app.program_retire import waiting

class TestUser(db.Model,UserMixin):
    __tablename__='user'
    id=db.Column(db.Integer,primary_key=True)
    @property
    def name(self):return 'PRIVATE_ACCOUNT_NAME' if self.id==USERS[2] else 'Test home owner'
    email='PRIVATE_EMAIL_SENTINEL'

app=Flask('waiting_test',template_folder=str(ROOT/'templates'))
app.config.update(SECRET_KEY='local-only-waiting-test',TESTING=True,SQLALCHEMY_DATABASE_URI=engine.url,WTF_CSRF_ENABLED=True)
db.init_app(app);login_manager.init_app(app);csrf.init_app(app)
@login_manager.user_loader
def load_user(value):return db.session.get(TestUser,int(value))
@login_manager.unauthorized_handler
def unauthorized():return ('Authentication required',401)
for endpoint,path in [('public_bp.welcome','/'),('public_bp.contact','/contact'),('auth_bp.login','/login'),('auth_bp.logout','/logout')]:
    app.add_url_rule(path,endpoint,lambda:'test shell')
app.jinja_env.globals['now']=lambda:datetime.now(timezone.utc)
app.register_blueprint(retire_bp)


class WaitingTests(unittest.TestCase):
    def setUp(self):
        self.conn=engine.connect(); local.identity(self.conn);self.conn.rollback()
        self.tx=self.conn.begin()
        with app.app_context():
            self.original_engine=db.engines[None]
            db.engines[None]=self.conn
            db.session.remove();db.session.configure(join_transaction_mode='create_savepoint')
        self.owner,self.other,self.mary=[app.test_client() for _ in USERS]
        for client,uid in zip((self.owner,self.other,self.mary),USERS):
            with client.session_transaction() as s:s['_user_id']=str(uid);s['_fresh']=True
        self.roles=dict(self.conn.execute(sa.text('SELECT code,id FROM retirement_role')).all())

    def tearDown(self):
        with app.app_context():
            db.session.remove();db.engines[None]=self.original_engine
        self.tx.rollback();self.conn.close()

    def rows(self,table):
        return [dict(r) for r in self.conn.execute(sa.text('SELECT * FROM '+table+' ORDER BY id')).mappings()]

    def post(self,client,path,data):
        response=client.get(path)
        self.assertEqual(response.status_code,200)
        token=re.search(r'name="csrf_token"[^>]*value="([^"]+)"',response.get_data(as_text=True)).group(1)
        return client.post(path,data=dict(data,csrf_token=token))

    def enrol(self,consent='y',**fields):
        data=dict(preferred_name='Mary Example',home_name_clue='',discovery_consent=consent);data.update(fields)
        return self.post(self.mary,'/retire/waiting-room',data)

    def home(self,client=None,name='Archoney House'):
        response=self.post(client or self.owner,'/retire/register',{'name':name,'authority':'y'})
        self.assertEqual(response.status_code,302)
        return int(re.search(r'organisations/(\d+)/setup',response.location).group(1))

    def path(self,home):
        return '/retire/organisations/%s/discovery/%s' % (home,self.rows('retirement_waiting_user')[0]['id'])

    def decide(self,home,decision='approve',version=0,client=None,**fields):
        data=dict(version=version,decision=decision,recognise='y');data.update(fields)
        return self.post(client or self.owner,self.path(home),data)

    def member(self,home):
        return next(r for r in self.rows('retirement_membership') if r['organisation_id']==home and r['user_id']==USERS[2])

    def test_zero_homes_optional_clue_and_repeat(self):
        self.assertEqual(self.enrol().status_code,302)
        before=self.rows('retirement_waiting_user')[0]
        self.assertIsNone(before['home_name_clue'])
        self.assertEqual(before['user_id'],USERS[2])
        self.assertEqual(self.enrol(preferred_name='Mary Updated').status_code,302)
        rows=self.rows('retirement_waiting_user')
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['id'],before['id'])
        self.assertEqual(rows[0]['preferred_name'],'Mary Updated')
        self.assertFalse(self.rows('retirement_organisation'));self.assertFalse(self.rows('retirement_membership'))

    def test_initial_consent_required_and_unchecked(self):
        page=self.mary.get('/retire/waiting-room').get_data(as_text=True)
        self.assertNotIn('checked',re.search(r'<input[^>]*name="discovery_consent"[^>]*>',page).group())
        self.assertEqual(self.enrol(consent='').status_code,200)
        self.assertFalse(self.rows('retirement_waiting_user'))

    def test_other_requires_authentication_and_creates_nothing(self):
        self.assertEqual(app.test_client().get('/retire/other').status_code,401)
        response=self.mary.get('/retire/other')
        self.assertTrue(response.location.endswith('/waiting-room'))
        self.assertFalse(self.rows('retirement_waiting_user'))

    def test_discovery_limits_fields_and_authority(self):
        self.enrol();home=self.home()
        path=f'/retire/organisations/{home}/discovery'
        self.assertEqual(self.mary.get(path).status_code,403)
        self.assertEqual(self.other.get(path).status_code,403)
        self.assertEqual(app.test_client().get(path).status_code,401)
        page=self.owner.get(path).get_data(as_text=True)
        self.assertIn('Mary Example',page)
        self.assertNotIn('PRIVATE_ACCOUNT_NAME',page);self.assertNotIn('PRIVATE_EMAIL_SENTINEL',page)
        self.assertNotIn('user_id',page)

    def test_not_ours_is_home_specific_no_membership(self):
        self.enrol();a=self.home();b=self.home(self.other,'Samaritan Home')
        self.assertEqual(self.decide(a,'not_ours').status_code,302)
        self.assertEqual(len(self.rows('retirement_membership')),2)
        self.assertNotIn(b'Mary Example',self.owner.get(f'/retire/organisations/{a}/discovery').data)
        self.assertIn(b'Mary Example',self.other.get(f'/retire/organisations/{b}/discovery').data)
        page=self.other.get(self.path(b)).get_data(as_text=True)
        self.assertIn('No decision yet',page)
        self.assertEqual(self.decide(b,client=self.other).status_code,302)
        self.assertEqual(self.rows('retirement_association_review')[0]['decision'],'not_ours')

    def test_explicit_reconsideration_appends_history(self):
        self.enrol();a=self.home();self.decide(a,'not_ours')
        before=self.rows('retirement_association_review')[0]
        self.enrol(preferred_name='Mary Updated')
        self.assertNotIn(b'Mary Updated',self.owner.get(f'/retire/organisations/{a}/discovery').data)
        self.assertIn(b'Mary Updated',self.owner.get(f'/retire/organisations/{a}/discovery?dismissed=1').data)
        self.assertEqual(self.decide(a,version=1).status_code,409)
        self.assertEqual(self.decide(a,version=1,reconsider='y').status_code,302)
        events=self.rows('retirement_association_review')
        self.assertEqual(events[0],before);self.assertEqual([r['version'] for r in events],[1,2])

    def test_withdrawal_preserves_association_and_blocks_review(self):
        self.enrol();a=self.home();b=self.home(self.other,'Samaritan Home')
        self.decide(a);self.decide(b,'not_ours',client=self.other)
        before=self.member(a);history=self.rows('retirement_association_review')
        self.assertEqual(self.enrol(consent='').status_code,302)
        self.assertEqual(self.member(a),before);self.assertEqual(self.rows('retirement_association_review'),history)
        self.assertEqual(self.other.get(self.path(b)).status_code,404)
        self.assertNotIn(b'Mary Example',self.other.get(f'/retire/organisations/{b}/discovery?dismissed=1').data)
        # A previously obtained valid CSRF token cannot override withdrawn consent.
        with self.other.session_transaction() as s:token=s.get('csrf_token')
        from flask_wtf.csrf import generate_csrf
        with app.test_request_context():
            from flask import session
            session['csrf_token']=token
            signed=generate_csrf()
        self.assertEqual(self.other.post(self.path(b),data={'version':1,'decision':'approve','recognise':'y','reconsider':'y','csrf_token':signed}).status_code,404)

    def test_association_has_no_role_or_operational_access(self):
        self.enrol();a=self.home();self.decide(a)
        row=self.member(a)
        self.assertIsNone(row['status']);self.assertIsNone(row['requested_role_id']);self.assertIsNone(row['approved_role_id'])
        self.assertEqual(row['association_approved_by_user_id'],USERS[0]);self.assertIsNotNone(row['association_approved_at'])
        response=self.mary.get(f'/retire/organisations/{a}/dashboard')
        self.assertTrue(response.location.endswith('/status'))
        page=self.mary.get(response.location).data
        self.assertIn(b'Association approval grants no operational access',page)
        self.assertNotIn(b'Status: <strong>None',page)
        self.assertEqual(self.mary.get(f'/retire/organisations/{a}/members/pending').status_code,302)
        # Stage 3 has relationship tables, but association approval creates none.
        self.assertEqual(self.conn.execute(sa.text('SELECT count(*) FROM retirement_relationship WHERE membership_id=:m'), {'m':row['id']}).scalar_one(), 0)

    def test_multiple_homes_and_repeated_approval(self):
        self.enrol();a=self.home();b=self.home(self.other,'Samaritan Home')
        self.assertEqual(self.decide(a).status_code,302)
        self.assertEqual(self.decide(a).status_code,302)
        self.assertEqual(self.decide(b,client=self.other).status_code,302)
        self.assertEqual(len([m for m in self.rows('retirement_membership') if m['user_id']==USERS[2]]),2)
        self.assertEqual(len(self.rows('retirement_association_review')),2)

    def test_stale_conflicting_decision_rejected(self):
        self.enrol();a=self.home();self.decide(a,'not_ours')
        self.assertEqual(self.decide(a,version=0,reconsider='y').status_code,409)
        self.assertEqual(len(self.rows('retirement_association_review')),1)

    def test_forged_identity_and_cross_home_rejected(self):
        self.assertEqual(self.enrol(user_id=USERS[0]).status_code,400)
        self.enrol();a=self.home();b=self.home(self.other,'Samaritan Home')
        self.assertEqual(self.decide(a,reviewed_by_user_id=USERS[1]).status_code,400)
        self.assertEqual(self.decide(a,organisation_id=b).status_code,400)
        self.assertEqual(self.other.get(self.path(a)).status_code,403)
        self.assertFalse(self.rows('retirement_association_review'))

    def test_csrf_failures_write_nothing(self):
        self.assertEqual(self.mary.post('/retire/waiting-room',data={'preferred_name':'Mary','discovery_consent':'y'}).status_code,400)
        self.assertFalse(self.rows('retirement_waiting_user'))
        self.enrol();a=self.home()
        self.assertEqual(self.owner.post(self.path(a),data={'version':0,'decision':'approve','recognise':'y'}).status_code,400)
        self.assertFalse(self.rows('retirement_association_review'));self.assertEqual(len(self.rows('retirement_membership')),1)

    def test_atomic_failure_rolls_back_association_and_event(self):
        self.enrol();a=self.home()
        original=db.session.add
        def fail_event(obj):
            if isinstance(obj,Review):raise RuntimeError('Injected audit failure')
            return original(obj)
        with patch.object(db.session,'add',side_effect=fail_event):
            with self.assertRaisesRegex(RuntimeError,'Injected audit failure'):self.decide(a)
        self.assertEqual(len(self.rows('retirement_membership')),1)
        self.assertFalse(self.rows('retirement_association_review'))
        self.assertEqual(self.decide(a).status_code,302)

    def test_legacy_states_roles_and_history_unchanged(self):
        self.enrol()
        for state in ('pending','active','denied','disabled'):
            home=self.home(name='Legacy '+state)
            self.conn.execute(sa.text("INSERT INTO retirement_membership(organisation_id,user_id,status,requested_role_id,approved_role_id,reviewed_at,reviewed_by_user_id,review_reason) VALUES (:o,:u,:s,:r,:a,'2025-01-02T00:00:00Z',:reviewer,'Legacy history')"),{'o':home,'u':USERS[2],'s':state,'r':self.roles['care_staff'],'a':self.roles['care_staff'] if state in ('active','disabled') else None,'reviewer':USERS[0]})
            before=self.member(home);self.assertEqual(self.decide(home).status_code,302)
            after=self.member(home)
            for key,value in before.items():
                if not key.startswith('association_approved_'):self.assertEqual(after[key],value)
            response=self.mary.get(f'/retire/organisations/{home}/dashboard')
            # Legacy active/role fields no longer grant Staff authority after cutover.
            self.assertEqual(response.status_code,302)
            if state == 'active':
                # Without new Staff authority, the outer gate returns status.
                blocked=self.mary.get(f'/retire/organisations/{home}/discovery')
                self.assertEqual(blocked.status_code,302)
                self.assertTrue(blocked.location.endswith('/status'))

    def test_audit_update_delete_rejected_by_database(self):
        self.enrol();a=self.home();self.decide(a,'not_ours')
        for sql in ('UPDATE retirement_association_review SET decision=\'approve\'', 'DELETE FROM retirement_association_review'):
            savepoint=self.conn.begin_nested()
            with self.assertRaises(DBAPIError):self.conn.execute(sa.text(sql))
            savepoint.rollback()
        self.assertEqual(self.rows('retirement_association_review')[0]['decision'],'not_ours')

    def test_database_constraints_reject_fake_authority(self):
        self.enrol();a=self.home();self.decide(a)
        for sql in ("UPDATE retirement_membership SET association_approved_at=NULL WHERE status IS NULL",
                    "UPDATE retirement_membership SET approved_role_id=:r WHERE status IS NULL",
                    "UPDATE retirement_membership SET status='associated' WHERE status IS NULL"):
            savepoint=self.conn.begin_nested()
            with self.assertRaises(DBAPIError):self.conn.execute(sa.text(sql),{'r':self.roles['care_staff']})
            savepoint.rollback()

    def test_recognition_required_no_name_auto_approval(self):
        self.enrol();a=self.home()
        self.assertEqual(self.decide(a,recognise='').status_code,200)
        self.assertFalse(self.rows('retirement_association_review'))
        self.assertEqual(len(self.rows('retirement_membership')),1)

    def test_not_ours_cannot_revoke_approved_association(self):
        self.enrol();a=self.home();self.decide(a);before=self.member(a)
        self.assertEqual(self.decide(a,'not_ours',version=1).status_code,409)
        self.assertEqual(self.member(a),before)

    def test_reading_routes_have_no_side_effects_and_preserve_identities(self):
        self.enrol();a=self.home();before=self.rows('retirement_association_review')
        self.owner.get(self.path(a));self.owner.get(f'/retire/organisations/{a}/discovery')
        self.assertEqual(self.rows('retirement_association_review'),before)
        self.assertEqual(len(self.rows('retirement_membership')),1)
        rules=[r for r in app.url_map.iter_rules() if r.endpoint.startswith('retire_bp.')]
        self.assertTrue(rules);self.assertTrue(all(r.rule.startswith('/retire/') for r in rules))

    def test_home_clue_search_and_profile_markup_are_safe(self):
        self.enrol(preferred_name='<script>Mary</script>',home_name_clue='Samaritan Home')
        a=self.home()
        page=self.owner.get(f'/retire/organisations/{a}/discovery?q=Samaritan').data
        self.assertIn(b'Samaritan Home',page);self.assertIn(b'&lt;script&gt;Mary',page)
        self.assertNotIn(b'<script>Mary',page)
        self.assertNotIn(b'Samaritan Home',self.owner.get(f'/retire/organisations/{a}/discovery?q=Unmatched').data)

    def test_concurrent_review_loses_lock_safely(self):
        # Only base fixtures are committed so another connection can see them.
        # Decisions remain in this test's outer transaction and are rolled back.
        with engine.begin() as c:
            local.identity(c)
            oid=c.execute(sa.text("INSERT INTO retirement_organisation(name,owner_user_id) VALUES ('Concurrent review fixture',:u) RETURNING id"),{'u':USERS[0]}).scalar_one()
            c.execute(sa.text("INSERT INTO retirement_membership(organisation_id,user_id,status,approved_role_id) VALUES (:o,:u,'active',:r)"),{'o':oid,'u':USERS[0],'r':self.roles['organisation_owner']})
            wid=c.execute(sa.text("INSERT INTO retirement_waiting_user(user_id,preferred_name,discovery_consent,consent_updated_at,created_at,updated_at) VALUES (:u,'Mary Example',true,now(),now(),now()) RETURNING id"),{'u':USERS[2]}).scalar_one()
        def cleanup():
            with engine.begin() as c:
                local.identity(c)
                self.assertEqual(c.execute(sa.text('SELECT count(*) FROM retirement_association_review WHERE organisation_id=:o'),{'o':oid}).scalar_one(),0)
                c.execute(sa.text('DELETE FROM retirement_membership WHERE organisation_id=:o'),{'o':oid})
                c.execute(sa.text('DELETE FROM retirement_organisation WHERE id=:o'),{'o':oid})
                c.execute(sa.text('DELETE FROM retirement_waiting_user WHERE id=:w'),{'w':wid})
        self.addCleanup(cleanup)
        contender=Flask('waiting_contender',template_folder=str(ROOT/'templates'))
        contender.config.update(SECRET_KEY='local-contender',TESTING=True,SQLALCHEMY_DATABASE_URI=engine.url,WTF_CSRF_ENABLED=True,
            SQLALCHEMY_ENGINE_OPTIONS={'connect_args':{'options':'-c search_path=public -c lock_timeout=250 -c statement_timeout=3000'}})
        db.init_app(contender);login_manager.init_app(contender);csrf.init_app(contender)
        for endpoint,path in [('public_bp.welcome','/'),('public_bp.contact','/contact'),('auth_bp.login','/login'),('auth_bp.logout','/logout')]:
            contender.add_url_rule(path,endpoint,lambda:'test shell')
        contender.jinja_env.globals['now']=lambda:datetime.now(timezone.utc)
        contender.register_blueprint(retire_bp)
        client=contender.test_client()
        with client.session_transaction() as s:s['_user_id']=str(USERS[0]);s['_fresh']=True
        url=self.path(oid)
        page=client.get(url)
        token=re.search(r'name="csrf_token"[^>]*value="([^"]+)"',page.get_data(as_text=True)).group(1)
        self.assertEqual(self.decide(oid).status_code,302)
        result=[]
        def compete():
            try:
                result.append(client.post(url,data={'version':0,'decision':'approve','recognise':'y','csrf_token':token}).status_code)
            except Exception as error:result.append(type(error).__name__)
        thread=threading.Thread(target=compete);thread.start();thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result,[409])
        self.assertEqual(len(self.rows('retirement_association_review')),1)
        self.assertEqual(len([m for m in self.rows('retirement_membership') if m['user_id']==USERS[2]]),1)


if __name__=='__main__':
    unittest.main()
