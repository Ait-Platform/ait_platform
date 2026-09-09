"""One consolidated local pilot validation; mail suppressed, provider mocked."""
import hashlib
import json
import os
import re
import uuid
from datetime import datetime,timedelta,timezone
from pathlib import Path
import pytest
import sqlalchemy as sa
from werkzeug.exceptions import HTTPException
from bootstrap import db,core,uip,User,ROOT
from app.extensions import mail
from app.uip.services import governance,invitations,ai
from app.services import ait_ai_gateway as gateway
from test_phase49 import eligible_member
from test_work_order_concurrency import concurrent_db,parallel

BASE="/uip/manor-gardens"

def key(): return str(uuid.uuid4())

@pytest.fixture
def pilot(app,data,monkeypatch):
    app.config.update(MAIL_SUPPRESS_SEND=True,MAIL_DEFAULT_SENDER="UIP <pilot@example.invalid>",UIP_PUBLIC_BASE_URL="https://uip.example.invalid",
        AIT_AI_PROVIDER="test",AIT_AI_MODEL="local-test",AIT_AI_CREDITS_PER_REQUEST=5)
    if "mail" not in app.extensions: mail.init_app(app)
    monkeypatch.setattr(User,"has_role",lambda self,role:self.id==data.users["manager"].id and role=="admin",raising=False)
    member=eligible_member(data); member.email="eligible@example.invalid"
    member.membership.user_id=None  # Registered voter deliberately has no AIT account.
    now=datetime.now(timezone.utc)
    survey=governance.survey(data.org.id,data.users["manager"].id,dict(title="Should Manor Gardens UIP approve the proposed application?",purpose="Consider the proposed application.",relationship="owner",opens_at=(now-timedelta(minutes=1)).isoformat(),closes_at=(now+timedelta(days=1)).isoformat(),identifiable="no"),[dict(title="Approve?",type="YES_NO")])
    db.session.commit()
    return survey,member

def invite(client,survey):
    path=BASE+f"/operations/surveys/{survey.id}/invitations"
    preview=client.get(path)
    assert preview.status_code==200
    token=re.search(rb'name="confirmation" value="([^"]+)"',preview.data)[1].decode()
    with mail.record_messages() as out:
        result=client.safe_post(path,{"confirmation":token})
    assert result.status_code==302,result.data
    assert len(out)==1 and out[0].recipients==["eligible@example.invalid"]
    code=re.search(r"#([A-Za-z0-9_-]{43})",out[0].body)[1]
    return code,out[0]

def public_post(browser,path,payload):
    from flask import g
    g.pop("csrf_token",None); g.pop("_login_user",None)
    get=browser.get(path)
    csrf=re.search(rb'name="csrf_token" value="([^"]+)"',get.data)[1].decode()
    return browser.post(path,data={**payload,"csrf_token":csrf})

def test_end_to_end_pilot(client,data,pilot,app,monkeypatch):
    survey,member=pilot; org,actor=data.org.id,data.users["manager"].id
    allocation=key()
    assert client.safe_post(BASE+"/administration/ai-wallet",dict(amount="5",reason="Controlled test pilot",request_key=allocation)).status_code==302
    assert b"5" in client.get(BASE+"/administration/ai-wallet").data
    code,message=invite(client,survey)
    invitation=uip.UipVotingInvitation.query.one()
    assert invitation.token_digest==hashlib.sha256(code.encode()).hexdigest() and code not in repr(invitation.__dict__)
    assert invitation.status=="TEST_SUPPRESSED"
    browser=app.test_client(); path=f"/uip/vote/manor-gardens/{survey.id}"
    opened=public_post(browser,path,{"token":code,"operation":"open"})
    assert opened.status_code==200 and b"Approve?" in opened.data and b"ui-sidebar" not in opened.data.split(b"</style>")[-1]
    assert opened.headers["Cache-Control"]=="no-store"
    assert public_post(browser,path,{"token":code,"operation":"vote","answer_1":"Yes"}).status_code==200
    assert public_post(browser,path,{"token":code,"operation":"vote","answer_1":"No"}).status_code==409
    assert uip.UipSurveyResponse.query.one().actor_user_id is None
    client.login("manager")
    page=client.get(BASE+f"/operations/surveys/{survey.id}/invitations")
    assert b"Voted" in page.data and b"Member #" not in page.data
    class ClosedClock(datetime):
        @classmethod
        def now(cls,tz=None): return survey.closes_at+timedelta(seconds=1)
    monkeypatch.setattr(governance,"datetime",ClosedClock)
    governance.finalize(org,actor,survey.id)
    decision=governance.decision(org,actor,dict(survey_id=survey.id,title="Application proposal",description="Decision based on recorded poll",status="RECORDED"))
    db.session.commit()
    assert survey.results["response_count"]==1 and decision.survey_id==survey.id
    assert b"How do I call a member vote?" in client.get(BASE+"/help").data
    request_id=key()
    payload=dict(feature="governance_notice",context="Please consider the proposed application.",approved="yes",request_key=request_id)
    answer=client.safe_post(BASE+"/assistant",payload)
    assert answer.status_code==200 and b"Draft for human review" in answer.data
    assert client.safe_post(BASE+"/assistant",payload).status_code==200
    assert core.CoreOrganizationWallet.query.one().balance==0
    assert core.CoreAiUsage.query.one().credits_charged==5
    assert core.CoreAiRequest.query.one().prompt_text is None and core.CoreAiRequest.query.one().response_text is None
    assert client.safe_post(BASE+"/assistant",{**payload,"request_key":key()}).status_code==200
    assert core.CoreAiRequest.query.order_by(core.CoreAiRequest.id.desc()).first().error_category=="insufficient_credits"
    assert client.get(BASE+"/finance").status_code==200
    assert client.get(BASE+"/work-orders").status_code==200
    client.login("provider")
    for target in ("/administration/ai-wallet","/assistant","/operations/surveys",f"/operations/surveys/{survey.id}/invitations"):
        assert client.get(BASE+target).status_code==403
    actions={e.action for e in uip.UipAuditEvent.query.all()}
    assert {"wallet.allocated","vote_invitation.batch_initiated","vote_invitation.attempted","survey.responded","survey.finalized","decision.recorded","ai.completed","ai.unavailable"}<=actions
    assert code not in json.dumps([e.metadata_json for e in uip.UipAuditEvent.query.all()])

@pytest.mark.parametrize("denial",["wrong_org","wrong_survey","expired","closed","bad_token","csrf"])
def test_public_voting_denials(client,data,pilot,app,denial,monkeypatch):
    survey,member=pilot; code,_=invite(client,survey); browser=app.test_client()
    path=f"/uip/vote/manor-gardens/{survey.id}"
    if denial=="wrong_org": path=f"/uip/vote/other/{survey.id}"
    if denial=="wrong_survey": path=f"/uip/vote/manor-gardens/{survey.id+9999}"
    if denial=="expired": uip.UipVotingInvitation.query.one().expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)
    if denial=="closed":
        class ClosedClock(datetime):
            @classmethod
            def now(cls,tz=None): return survey.closes_at+timedelta(seconds=1)
        monkeypatch.setattr(governance,"datetime",ClosedClock)
        governance.finalize(data.org.id,data.users["manager"].id,survey.id)
    if denial=="bad_token": code="a"*43
    db.session.commit()
    payload=dict(token=code,operation="vote",answer_1="Yes")
    response=browser.post(path,data=payload) if denial=="csrf" else public_post(browser,path,payload)
    assert response.status_code in {400,404,409,410}
    assert uip.UipSurveyResponse.query.count()==0

def test_resend_rotates_without_second_entitlement(client,data,pilot,app):
    survey,member=pilot; old,_=invite(client,survey)
    path=BASE+f"/operations/surveys/{survey.id}/invitations?member={member.id}"
    page=client.get(path); confirmation=re.search(rb'name="confirmation" value="([^"]+)"',page.data)[1].decode()
    with mail.record_messages() as out: assert client.safe_post(path,{"confirmation":confirmation}).status_code==302
    new=re.search(r"#([A-Za-z0-9_-]{43})",out[0].body)[1]
    assert old!=new and uip.UipVotingInvitation.query.count()==1
    browser=app.test_client(); public=f"/uip/vote/manor-gardens/{survey.id}"
    assert public_post(browser,public,dict(token=old,operation="vote",answer_1="Yes")).status_code==404
    assert public_post(browser,public,dict(token=new,operation="vote",answer_1="Yes")).status_code==200
    client.login("manager")
    page=client.get(path)
    assert b' disabled' in page.data

def test_missing_mail_and_failure_truthful(client,data,pilot,app,monkeypatch):
    survey,member=pilot
    member.email=None; db.session.commit()
    row,rows=invitations.roster(data.org.id,data.users["manager"].id,survey.id)
    assert len(rows)==1 and not rows[0]["email"] and not invitations.plan(rows)
    member.email="eligible@example.invalid"; db.session.commit()
    monkeypatch.setattr(mail,"send",lambda m:(_ for _ in ()).throw(RuntimeError("secret transport failure")))
    selected=invitations.plan(invitations.roster(data.org.id,data.users["manager"].id,survey.id)[1])
    invitations.send(data.org,data.users["manager"].id,survey.id,selected)
    assert uip.UipVotingInvitation.query.one().status=="FAILED"
    assert uip.UipVotingInvitation.query.one().error_category=="mail_error"

@pytest.mark.parametrize("role",["resident","owner","provider","committee_member"])
def test_wallet_permissions(client,data,pilot,role):
    client.login(role)
    assert client.get(BASE+"/administration/ai-wallet").status_code==403
    assert client.safe_post(BASE+"/administration/ai-wallet",dict(amount=100,reason="No",request_key=key())).status_code==403

def test_manager_cannot_mint(client,data,pilot,monkeypatch):
    monkeypatch.setattr(User,"has_role",lambda self,role:False,raising=False)
    assert client.get(BASE+"/administration/ai-wallet").status_code==200
    assert client.safe_post(BASE+"/administration/ai-wallet",dict(amount=100,reason="No",request_key=key())).status_code==403

def test_ai_scope_failure_refund_and_idempotency(client,data,pilot,app,monkeypatch):
    org,actor=data.org.id,data.users["manager"].id
    gateway.allocate(org,actor,15,"Pilot",key()); db.session.commit()
    with pytest.raises(HTTPException) as denied: ai.run(org,actor,"issue","Reviewed text",key(),data.foreign.id)
    assert denied.value.code==404
    calls=[]
    def provider(**kw):
        calls.append(kw)
        assert "Other issue" not in kw["prompt"]
        raise RuntimeError("API_KEY never store this")
    app.config.update(AIT_AI_PROVIDER="mock",AIT_AI_ADAPTERS={"mock":provider})
    rid=key(); result=ai.run(org,actor,"activity","",rid)
    assert result["status"]=="failed" and core.CoreOrganizationWallet.query.one().balance==15
    ai.run(org,actor,"activity","",rid)
    assert len(calls)==1 and core.CoreAiUsage.query.one().credits_charged==0
    assert core.CoreOrganizationLedger.query.count()==3
    app.config["AIT_AI_PROVIDER"]="missing"
    result=ai.run(org,actor,"activity","",key())
    assert result["status"]=="unavailable" and core.CoreOrganizationWallet.query.one().balance==15
    assert client.get(BASE+"/finance").status_code==200
    with pytest.raises(HTTPException): ai.run(org,actor,"minutes","password=secret",key())
    assert "API_KEY" not in json.dumps([r.error_category for r in core.CoreAiRequest.query.all()])

def test_wallet_ledger_immutable_and_retry_conflict(data,pilot):
    org,actor=data.org.id,data.users["manager"].id; ref=key()
    first=gateway.allocate(org,actor,10,"Pilot",ref); db.session.commit()
    assert gateway.allocate(org,actor,10,"Pilot",ref).id==first.id
    assert core.CoreOrganizationWallet.query.one().balance==10
    with pytest.raises(HTTPException): gateway.allocate(org,actor,11,"Pilot",ref)
    with db.session.begin_nested():
        with pytest.raises(sa.exc.DBAPIError):
            with db.session.begin_nested(): db.session.execute(sa.text("DELETE FROM core_organization_ledger WHERE id=:id"),{"id":first.id})

@pytest.mark.parametrize("same_key",[False,True])
def test_concurrent_ai_no_overspend(concurrent_db,same_key):
    app,engine,ids=concurrent_db
    from conftest import migrate_phase10,migrate_phase11
    with engine.begin() as c: migrate_phase10(c);migrate_phase11(c)
    app.config.update(TESTING=True,AIT_AI_PROVIDER="test",AIT_AI_MODEL="local",AIT_AI_CREDITS_PER_REQUEST=5)
    with app.app_context():
        wallet=core.CoreOrganizationWallet(organization_id=ids.org,balance=5,status="ACTIVE")
        db.session.add(wallet);db.session.flush()
        db.session.add(core.CoreOrganizationLedger(wallet_id=wallet.id,amount=5,product="uip",entry_type="ALLOCATION",reference=key(),balance_after=5))
        db.session.commit();db.session.remove()
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    barrier=Barrier(2)
    common_key=key()
    def run():
        with app.app_context():
            try:
                barrier.wait(timeout=5)
                return ai.run(ids.org,ids.manager,"minutes","Approved wording",common_key if same_key else key())["status"]
            finally: db.session.remove()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:run(),range(2)))
    if same_key: assert "completed" in results and set(results)<={"completed","pending"}
    else: assert sorted(results)==["completed","unavailable"]
    with engine.connect() as c:
        assert c.scalar(sa.text("SELECT balance FROM core_organization_wallet"))==0
        assert c.scalar(sa.text("SELECT count(*) FROM core_ai_usage"))==1

def test_templates_and_visuals(client,data,pilot,tmp_path):
    from jinja2 import Environment
    for path in (ROOT/"templates/uip").rglob("*.html"): Environment().parse(path.read_text(encoding="utf-8"))
    response=client.get(BASE+"/finance")
    assert b'/help#finance' in response.data
    help_page=client.get(BASE+"/help")
    for anchor in ("command-centre","residents-properties","operations","service-providers","governance","finance","reports","administration"):
        assert ('id="'+anchor+'"').encode() in help_page.data
    assert response.data.count(b'class="ui-nav-group"')==8
    from playwright.sync_api import sync_playwright
    out=Path(os.environ.get("TEMP",str(tmp_path)))/"uip-pilot-validation";out.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser=p.chromium.launch()
        for name,path in (("help","/help"),("wallet","/administration/ai-wallet"),("assistant","/assistant"),("invitations",f"/operations/surveys/{pilot[0].id}/invitations")):
            html=client.get(BASE+path).get_data(as_text=True)
            (out/(name+".html")).write_text(html,encoding="utf-8")
            for width in (1440,390):
                page=browser.new_page(viewport={"width":width,"height":1000})
                page.set_content(html,wait_until="networkidle")
                assert page.evaluate("document.documentElement.scrollWidth<=innerWidth"),(name,width)
                page.screenshot(path=str(out/f"{name}-{width}.png"),full_page=True)
                page.close()
        browser.close()


def test_phase11_parity_and_preservation(concurrent_db):
    from conftest import migrate_phase10,migrate_phase11
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    app,engine,ids=concurrent_db
    with engine.begin() as c:
        migrate_phase10(c)
        inspector=sa.inspect(c)
        names=[n for n in inspector.get_table_names() if n not in {"site_hit","visit_log"}]
        columns={n:[x["name"] for x in inspector.get_columns(n)] for n in names}
        def snapshot():
            return {n:sorted(c.exec_driver_sql('SELECT row_to_json(t)::text FROM (SELECT '+','.join('"'+col+'"' for col in columns[n])+' FROM "'+n+'") t').scalars().all()) for n in names}
        before=snapshot()
        migrate_phase11(c)
        assert before==snapshot()
        inspector=sa.inspect(c)
        assert set(inspector.get_table_names())-set(names)=={"uip_voting_invitation"}
        assert c.scalar(sa.text("SELECT count(*) FROM uip_voting_invitation"))==0
        for model in (uip.UipVotingInvitation,core.CoreOrganizationWallet,core.CoreOrganizationLedger,core.CoreAiRequest,core.CoreAiUsage,uip.UipSurvey,uip.UipSurveyResponse,uip.UipAuditEvent):
            actual=inspector.get_columns(model.__tablename__)
            assert {col["name"] for col in actual}==set(model.__table__.columns.keys())
            for col in actual:
                expected=model.__table__.columns[col["name"]]
                assert col["nullable"]==expected.nullable,(model.__tablename__,col)
                assert col["type"].compile(dialect=c.dialect)==expected.type.compile(dialect=c.dialect)
            assert {tuple(x["column_names"]) for x in inspector.get_unique_constraints(model.__tablename__)}=={tuple(x.columns.keys()) for x in model.__table__.constraints if isinstance(x,sa.UniqueConstraint)}
    script=ScriptDirectory.from_config(Config(str(ROOT/"alembic.ini")))
    assert script.get_revision("uip_p11_completion").down_revision=="uip_p10_finance"




def test_concurrent_public_vote_one_response(concurrent_db):
    from conftest import migrate_phase10,migrate_phase11
    from types import SimpleNamespace
    app,engine,ids=concurrent_db
    with engine.begin() as c: migrate_phase10(c);migrate_phase11(c)
    with app.app_context():
        issue=db.session.get(core.CoreInteraction,ids.issue)
        data=SimpleNamespace(org=db.session.get(core.CoreOrganization,ids.org),users={"manager":db.session.get(User,ids.manager),"resident":db.session.get(User,issue.creator_id)})
        member=eligible_member(data);member.membership.user_id=None
        now=datetime.now(timezone.utc)
        survey=governance.survey(ids.org,ids.manager,dict(title="Concurrent public vote",purpose="One ballot",relationship="owner",opens_at=(now-timedelta(minutes=1)).isoformat(),closes_at=(now+timedelta(hours=1)).isoformat(),identifiable="no"),[dict(title="Approve?",type="YES_NO")])
        token="b"*43
        db.session.add(uip.UipVotingInvitation(organization_id=ids.org,survey_id=survey.id,member_id=member.id,token_digest=hashlib.sha256(token.encode()).hexdigest(),expires_at=survey.closes_at,status="TEST_SUPPRESSED",attempts=1,attempted_at=now))
        survey_id=survey.id;db.session.commit();db.session.remove()
    results=parallel(app,[lambda:invitations.vote(ids.org,survey_id,token,{"1":"Yes"}),lambda:invitations.vote(ids.org,survey_id,token,{"1":"No"})])
    assert sorted(status for status,_ in results)==[200,409]
    with engine.connect() as c: assert c.scalar(sa.text("SELECT count(*) FROM uip_survey_response"))==1
