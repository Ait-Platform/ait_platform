"""Real local PostgreSQL route tests; isolate only platform startup/auth shell.
Existing user IDs are read, never changed. Only uniquely tagged Retirement test
organisations and their memberships are deleted during finally cleanup.
"""
import importlib.util
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sys
import types
import uuid
from flask import Flask, redirect
from flask_login import UserMixin
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location("retire_local",ROOT/"scripts/retirement_stage2_local.py")
local=importlib.util.module_from_spec(spec);spec.loader.exec_module(local)
engine=local.local_engine()
os.environ['SKIP_AUTO_MIGRATE']='1'
# Explicit process-local overrides. No inherited SQLite URI reaches Flask.
for key in ('DATABASE_URL','SQLALCHEMY_DATABASE_URI','FLASK_SQLALCHEMY_DATABASE_URI'):
    os.environ[key]=engine.url.render_as_string(hide_password=False)
for name,directory in (("app","app"),("app.models","app/models")):
    package=types.ModuleType(name);package.__path__=[str(ROOT/directory)];sys.modules[name]=package
from app.extensions import db,login_manager,csrf
from app.models.retire import RetirementOrganisation as Org,RetirementMembership as Member,RetirementRole as Role
from app.program_retire import retire_bp,routes

class TestUser(db.Model,UserMixin):
    __tablename__='user'
    id=db.Column(db.Integer,primary_key=True)
    name='Local test account'
    email=''

app=Flask('retirement_isolated',template_folder=str(ROOT/'templates'))
app.config.update(SECRET_KEY=uuid.uuid4().hex,TESTING=True,SQLALCHEMY_DATABASE_URI=engine.url,
    SQLALCHEMY_ENGINE_OPTIONS={'connect_args':{'options':'-c search_path=public -c statement_timeout=15000'}},WTF_CSRF_ENABLED=True)
db.init_app(app);login_manager.init_app(app);csrf.init_app(app)
@login_manager.user_loader
def load_user(value):
    return db.session.get(TestUser,int(value))
@login_manager.unauthorized_handler
def unauthorized():
    return redirect('/test-login')
# Navigation endpoints are inert test-shell links, not substitute Retirement pages.
for endpoint,path in [('public_bp.welcome','/'),('public_bp.contact','/test-contact'),('auth_bp.login','/test-login'),('auth_bp.logout','/test-logout')]:
    app.add_url_rule(path,endpoint,lambda:'Isolated local Retirement test shell')
app.jinja_env.globals["now"] = lambda: datetime.now(timezone.utc)
app.register_blueprint(retire_bp)

prefix='retirement_stage2_verify_'+uuid.uuid4().hex
names=[prefix+'_A',prefix+'_B']
passed=[]
def check(condition,label):
    assert condition,label
    passed.append(label);print('PASS:',label,flush=True)
def signin(client,user_id):
    with client.session_transaction() as session:
        session['_user_id']=str(user_id);session['_fresh']=True

def post(client,path,data,page=None):
    response=client.get(page or path)
    assert response.status_code==200,(path,response.status_code)
    token=re.search(r'name="csrf_token"[^>]*value="([^"]+)"',response.get_data(as_text=True))
    assert token,'CSRF token missing'
    return client.post(path,data=dict(data,csrf_token=token.group(1)))

def snapshot(org_id,user_id):
    with engine.connect() as conn:
        return dict(conn.execute(sa.text('SELECT * FROM public.retirement_membership WHERE organisation_id=:o AND user_id=:u'),{'o':org_id,'u':user_id}).mappings().one())

try:
    with engine.connect() as conn:
        print('Application test target:',local.identity(conn))
        users=conn.execute(sa.text('SELECT id FROM public."user" ORDER BY id LIMIT 3')).scalars().all()
        assert len(users)==3,'Need three existing local user identities'
        assert conn.execute(sa.text('SELECT count(*) FROM public.retirement_organisation WHERE owner_user_id=ANY(:users)'),{'users':users}).scalar_one()==0,'Selected users already own Retirement organisations'
        roles=dict(conn.execute(sa.text('SELECT code,id FROM public.retirement_role')).all())
    with app.app_context():
        check(local.identity(db.session.connection())['db']=='ait_local_db','Flask uses explicitly selected local PostgreSQL')
        db.session.remove()
    owner,staff,other=[app.test_client() for _ in users]
    for client,user in zip((owner,staff,other),users):signin(client,user)
    owner_id,staff_id,other_id=users
    check(owner.get('/retire/entry').status_code==200,'Owner without membership sees entry options')
    response=post(owner,'/retire/register',{'name':names[0]})
    check(response.status_code==302,'Owner registration redirects successfully')
    with engine.connect() as conn:
        organisation=conn.execute(sa.text('SELECT id FROM public.retirement_organisation WHERE name=:name'),{'name':names[0]}).scalar_one()
        check(conn.execute(sa.text('SELECT count(*) FROM public.retirement_membership WHERE organisation_id=:id'),{'id':organisation}).scalar_one()==1,'Registration creates exactly one owner membership')
    member=snapshot(organisation,owner_id)
    check(member['status']=='active' and member['approved_role_id']==roles['organisation_owner'] and member['requested_role_id'] is None and member['reviewed_at'] is None and member['reviewed_by_user_id'] is None,'Owner establishment state is correct')
    dash=f'/retire/organisations/{organisation}/dashboard'
    pending=f'/retire/organisations/{organisation}/members/pending'
    response=owner.get(dash);body=response.get_data(as_text=True)
    check(response.status_code==200 and body.count('Coming soon')==10,'Owner dashboard renders all ten placeholder tiles')
    check(owner.get(pending).status_code==200,'Owner pending-member queue accessible')
    check(post(owner,'/retire/register',{'name':names[0]},page=dash).status_code==302,'Repeated registration resolves existing owner')
    join=f'/retire/organisations/{organisation}/join'
    check(staff.get('/retire/entry').status_code==200,'New staff sees entry options')
    check(post(staff,'/retire/join',{'organisation_id':organisation}).location.endswith(join),'Organisation lookup reaches confirmation')
    check(post(staff,join,{'role_id':roles['organisation_owner'],'confirm':'y'}).status_code==200,'Newcomer owner-role request rejected')
    with engine.connect() as conn:
        check(conn.execute(sa.text('SELECT count(*) FROM public.retirement_membership WHERE user_id=:u'),{'u':staff_id}).scalar_one()==0,'Rejected owner-role request creates no membership')
    response=post(staff,join,{'role_id':roles['care_staff'],'confirm':'y','user_id':owner_id,'status':'active','approved_role_id':roles['organisation_owner'],'reviewed_by_user_id':owner_id})
    member=snapshot(organisation,staff_id)
    check(member['status']=='pending' and member['requested_role_id']==roles['care_staff'] and member['approved_role_id'] is None and member['reviewed_by_user_id'] is None,'Pending request uses authenticated applicant and ignores injected privileged fields')
    status=f'/retire/organisations/{organisation}/status'
    check(response.location.endswith(status) and staff.get(status).status_code==200,'Pending staff reaches waiting room')
    check(staff.get(dash).location.endswith(status),'Pending staff blocked from dashboard')
    check(post(staff,join,{'role_id':roles['care_staff'],'confirm':'y'},page=status).status_code==302,'Repeated join resolves existing request')
    with engine.connect() as conn:
        check(conn.execute(sa.text('SELECT count(*) FROM public.retirement_membership WHERE organisation_id=:o AND user_id=:u'),{'o':organisation,'u':staff_id}).scalar_one()==1,'Repeated join creates no duplicates')
    review=f'/retire/organisations/{organisation}/members/{member["id"]}/review'
    check(owner.get(review).status_code==200,'Owner can review pending staff')
    check(post(owner,review,{'decision':'approve','role_id':roles['organisation_owner'],'reason':'Invalid'}).status_code==200,'Owner role cannot be approved through staff flow')
    check(post(owner,review,{'decision':'approve','role_id':999999,'reason':'Invalid'}).status_code==200,'Unknown approval role rejected')
    check(post(owner,review,{'decision':'approve','role_id':roles['care_staff'],'reason':'Verified local test'}).status_code==302,'Owner approval succeeds')
    member=snapshot(organisation,staff_id)
    check(member['status']=='active' and member['requested_role_id']==roles['care_staff'] and member['approved_role_id']==roles['care_staff'] and member['reviewed_by_user_id']==owner_id and member['reviewed_at'] is not None,'Approval retains request and records approved role/reviewer/time')
    check(staff.get('/retire/',follow_redirects=True).request.path==dash,'Returning active staff goes directly to dashboard')
    check(staff.get(review).status_code==403 and staff.get(pending).status_code==403,'Active non-owner cannot review or manage members')
    check(owner.get(f'/retire/organisations/{organisation}/members/{snapshot(organisation,owner_id)["id"]}/review').status_code==403,'Self-approval blocked')
    # Second organisation supplies cross-organisation and denial cases.
    check(post(other,'/retire/register',{'name':names[1]}).status_code==302,'Second test organisation registered')
    with engine.connect() as conn:
        second=conn.execute(sa.text('SELECT id FROM public.retirement_organisation WHERE name=:name'),{'name':names[1]}).scalar_one()
    second_join=f'/retire/organisations/{second}/join'
    check(post(staff,second_join,{'role_id':roles['finance'],'confirm':'y'}).status_code==302,'Staff can request membership in a separate Retirement organisation')
    second_member=snapshot(second,staff_id)
    second_review=f'/retire/organisations/{second}/members/{second_member["id"]}/review'
    check(owner.get(second_review).status_code==403,'Other organisation owner cannot review this organisation')
    check(owner.get(f'/retire/organisations/{organisation}/members/{second_member["id"]}/review').status_code==404,'Cross-organisation membership ID rejected')
    check(post(other,second_review,{'decision':'deny','role_id':0,'reason':'Denied local test'}).status_code==302,'Owner denial succeeds')
    denied=snapshot(second,staff_id)
    check(denied['status']=='denied' and denied['approved_role_id'] is None and denied['reviewed_by_user_id']==other_id and denied['reviewed_at'] is not None,'Denial records review with no approved role')
    check(staff.get(f'/retire/organisations/{second}/dashboard').location.endswith('/status'),'Denied membership blocked from dashboard')
    with engine.begin() as conn:
        local.identity(conn)
        conn.execute(sa.text("UPDATE public.retirement_membership SET status='disabled' WHERE id=:id"),{'id':member['id']})
    check(staff.get(dash).location.endswith('/status'),'Disabled membership blocked from dashboard')
    with engine.begin() as conn:
        local.identity(conn)
        conn.execute(sa.text("UPDATE public.retirement_membership SET status='active' WHERE id=:id"),{'id':member['id']})
    with engine.connect() as conn:
        local.identity(conn)
        transaction=conn.begin_nested()
        try:
            conn.execute(sa.text("INSERT INTO public.retirement_membership(organisation_id,user_id,status,approved_role_id) VALUES(:o,:u,'active',:r)"),{'o':organisation,'u':staff_id,'r':roles['care_staff']})
            raise AssertionError('Duplicate membership was accepted')
        except IntegrityError:
            transaction.rollback()
            check(True,'Database unique constraint rejects duplicate membership safely')
    for path in ('/retire/register','/retire/join',join,review):
        check(owner.post(path,data={}).status_code==400,'CSRF required: '+path)
    anon=app.test_client()
    for path in ('/retire/','/retire/about'):
        check(anon.get(path).status_code==200,'Anonymous Retirement smoke '+path)
    # Smoke root is explicitly an isolated shell, not a claim about full AIT factory.
    print('Root / is a test-shell endpoint; full AIT factory was intentionally not invoked.')
    print('LOCAL RETIREMENT FLOW PASS:',len(passed),'checks')
finally:
    with app.app_context():db.session.remove()
    with engine.begin() as conn:
        local.identity(conn)
        ids=conn.execute(sa.text('SELECT id FROM public.retirement_organisation WHERE name=ANY(:names)'),{'names':names}).scalars().all()
        if ids:
            deleted=conn.execute(sa.text('DELETE FROM public.retirement_membership WHERE organisation_id=ANY(:ids)'),{'ids':ids}).rowcount
            organisations_deleted=conn.execute(sa.text('DELETE FROM public.retirement_organisation WHERE id=ANY(:ids) AND name=ANY(:names)'),{'ids':ids,'names':names}).rowcount
            print('Cleanup:',deleted,'test memberships and',organisations_deleted,'test organisations removed')
        assert conn.execute(sa.text('SELECT count(*) FROM public.retirement_organisation WHERE name=ANY(:names)'),{'names':names}).scalar_one()==0
    module,metadata=local.definitions()
    with engine.connect() as conn:
        conn.exec_driver_sql('SET TRANSACTION READ ONLY')
        print('Final verified counts:',local.verify(conn,metadata,module))
    engine.dispose()
