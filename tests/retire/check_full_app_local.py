"""Actual create_app and real auth/models/routes; test-only SQL write isolation."""
import contextlib
import io
import logging
import os
from pathlib import Path
import re
import sys
import sqlalchemy as sa

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
import retirement_relationship_transition as schema
engine=schema.waiting.legacy.local_engine()
conn=engine.connect()
schema.waiting.legacy.identity(conn);schema.verify(conn);conn.rollback()
startup=True
suppressed=[]

@sa.event.listens_for(sa.engine.Engine,'before_cursor_execute',retval=True)
def isolate(connection,cursor,statement,parameters,context,executemany):
    assert connection.engine.url.database=='ait_local_db', 'Non-local engine forbidden'
    sql=statement.lstrip()
    if re.match(r'(?is)^(SELECT|SHOW|SAVEPOINT|RELEASE|ROLLBACK|SET)\b',sql):return statement,parameters
    if startup:
        suppressed.append(sql.split()[0]);return 'SELECT 1',()
    if re.match(r'(?is)^(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:public\.)?"?retirement_[a-z_]+\b',sql):return statement,parameters
    raise RuntimeError('Local RCM smoke blocks unrelated writes')

os.environ['DATABASE_URL']=engine.url.render_as_string(hide_password=False)
os.environ['SKIP_AUTO_MIGRATE']='1'
os.environ['DEFAULT_LOGIN_EMAIL']=''
logging.disable(logging.CRITICAL)
stage='factory import'
tx=None
try:
    with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
        from app import create_app
        from app.extensions import db
        stage='factory startup'
        app=create_app({'TESTING':True,'SECRET_KEY':'local-full-rcm-smoke',
            'SQLALCHEMY_DATABASE_URI':engine.url,
            'SQLALCHEMY_ENGINE_OPTIONS':{'connect_args':{'options':'-c default_transaction_read_only=on -c search_path=public'}},
            'WTF_CSRF_ENABLED':True})
    print('Actual full factory started; startup SQL writes suppressed:',len(suppressed))
    startup=False
    tx=conn.begin()
    with app.app_context():
        db.session.remove();db.engines[None].dispose();db.engines[None]=conn
        db.session.configure(join_transaction_mode='create_savepoint')
    users=conn.execute(sa.text('SELECT id FROM public."user" ORDER BY id LIMIT 3')).scalars().all()
    assert len(users)==3
    owner,other,mary=[app.test_client() for _ in users]
    for client,uid in zip((owner,other,mary),users):
        with client.session_transaction() as s:s['_user_id']=str(uid);s['_fresh']=True
    def get(client,path):
        response=client.get(path);assert response.status_code==200,(path,response.status_code)
        return response
    def post(client,path,data):
        page=get(client,path).get_data(as_text=True)
        token=re.search(r'name="csrf_token"[^>]*value="([^"]+)"',page).group(1)
        response=client.post(path,data=dict(data,csrf_token=token))
        assert response.status_code==302,(path,response.status_code)
        return response
    def home(client,name):
        get(client,'/retire/about');get(client,'/retire/onboarding')
        response=post(client,'/retire/register',{'name':name,'authority':'y'})
        page=get(client,response.location)
        assert b'Relationship management' in page.data
        oid=int(re.search(r'organisations/(\d+)/setup',response.location).group(1))
        get(client,f'/retire/organisations/{oid}/relationships');return oid
    stage='owner journey'
    a=home(owner,'Full app RCM smoke A');b=home(other,'Full app RCM smoke B')
    stage='Other and discovery journey'
    get(mary,'/retire/about');get(mary,'/retire/onboarding')
    assert mary.get('/retire/other').location.endswith('/waiting-room')
    post(mary,'/retire/waiting-room',{'preferred_name':'RCM full-app test person','discovery_consent':'y'})
    wid=conn.execute(sa.text('SELECT id FROM retirement_waiting_user WHERE user_id=:u'),{'u':users[2]}).scalar_one()
    for client,oid,decision in ((owner,a,'not_ours'),(other,b,'approve')):
        assert b'RCM full-app test person' in get(client,f'/retire/organisations/{oid}/discovery').data
        post(client,f'/retire/organisations/{oid}/discovery/{wid}',{'version':0,'decision':decision,'recognise':'y'})
    mid=conn.execute(sa.text('SELECT id FROM retirement_membership WHERE organisation_id=:o AND user_id=:u'),{'o':b,'u':users[2]}).scalar_one()
    assert mary.get(f'/retire/organisations/{b}/dashboard').status_code==302
    path=f'/retire/organisations/{b}/members/{mid}/relationships'
    def change(action,**kw):
        version=conn.execute(sa.text('SELECT coalesce(max(version),0) FROM retirement_authority_event WHERE membership_id=:m'),{'m':mid}).scalar_one()
        post(other,path,dict(action=action,version=version,kind=kw.pop('kind','staff'),role_id=kw.pop('role_id',0),reason='Full-app smoke',**kw))
    stage='relationships and role journey'
    change('grant_relationship')
    rid=conn.execute(sa.text("SELECT id FROM retirement_relationship WHERE membership_id=:m AND kind='staff'"),{'m':mid}).scalar_one()
    for code in ('care_staff','facility_manager'):
        role=conn.execute(sa.text('SELECT id FROM retirement_role WHERE code=:code'),{'code':code}).scalar_one()
        change('grant_role',target=rid,role_id=role)
    get(mary,f'/retire/organisations/{b}/dashboard')
    change('withdraw_relationship',target=rid)
    assert conn.execute(sa.text('SELECT count(*) FROM retirement_staff_role_assignment WHERE relationship_id=:r AND withdrawn_at IS NULL'),{'r':rid}).scalar_one()==0
    assert mary.get(f'/retire/organisations/{b}/dashboard').status_code==302
    change('grant_relationship',kind='resident');change('grant_relationship',kind='family_representative')
    assert owner.get(path).status_code==403
    assert mary.get(path).status_code==403
    for template in (ROOT/'templates/program_retire').glob('*.html'):app.jinja_env.get_template('program_retire/'+template.name)
    print('PASS: actual full-app Owner/Other/Not ours/approval/multiple Staff roles/withdrawal/Resident/Family/isolation smoke.')
except Exception as error:
    print('FULL-APP BLOCKER at',stage,':',type(error).__name__)
    if isinstance(error,(ImportError,ModuleNotFoundError,AssertionError,PermissionError)):print(str(error)[:500])
    raise SystemExit(1)
finally:
    if tx:
        with app.app_context():db.session.remove()
        tx.rollback()
        print('Full-app Retirement fixtures rolled back; no immutable history deleted.')
    conn.close()
