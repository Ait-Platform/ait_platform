"""Existing signed invitation, authenticated founding appointment, and replay safety."""
from unittest.mock import patch
import pytest
from itsdangerous import URLSafeTimedSerializer
from bootstrap import db, core, uip
from app.models.uip_governance import UipCommitteeMember

BASE = "/uip/manor-gardens/provisioning"
DETAILS = {"venue":"Town Hall", "meeting_date":"2026-09-12", "meeting_time":"10:00"}

def invitation(app, data, **overrides):
    payload = {"org_slug":data.org.slug, "email":data.users["manager"].email}
    payload.update(overrides)
    return URLSafeTimedSerializer(app.secret_key, salt="uip-provisioning").dumps(payload)

@pytest.mark.parametrize("kind", ["missing","forged","expired","wrong_org","wrong_email","malformed"])
def test_invalid_invitation_never_appoints(client, app, data, kind):
    token = invitation(app,data)
    if kind == "missing": token = ""
    if kind == "forged": token += "tampered"
    if kind == "wrong_org": token = invitation(app,data,org_slug=data.other.slug)
    if kind == "wrong_email": token = invitation(app,data,email="other@example.invalid")
    if kind == "malformed": token = URLSafeTimedSerializer(app.secret_key,salt="uip-provisioning").dumps([])
    if kind == "expired":
        with patch("itsdangerous.timed.TimestampSigner.get_timestamp", return_value=1):
            token=invitation(app,data)
    response=client.safe_post(BASE, {**DETAILS,"token":token})
    assert response.status_code == 403
    assert UipCommitteeMember.query.count() == 0
    assert uip.UipCommitteeMeeting.query.count() == 0

def test_anonymous_invitation_cannot_appoint(client,app,data):
    from flask import g
    token=invitation(app,data)
    with client.session_transaction() as session: session.clear()
    g.pop("_login_user",None)
    assert client.get(BASE+"?token="+token).status_code in (302,401)
    assert client.safe_post(BASE,{**DETAILS,"token":token}).status_code in (302,401)
    assert UipCommitteeMember.query.count()==0

def test_authenticated_invitation_and_repeat(client,app,data):
    token=invitation(app,data)
    response=client.get(BASE+"?token="+token)
    assert response.status_code==200
    assert token.encode() in response.data
    assert UipCommitteeMember.query.count()==0
    response=client.safe_post(BASE,{**DETAILS,"token":token})
    assert response.status_code==302
    member=UipCommitteeMember.query.one()
    assert member.email==data.users["manager"].email
    assert member.created_by==data.users["manager"].id
    assert member.position=="Secretary"
    assert client.safe_post(BASE,{**DETAILS,"token":token}).status_code==302
    assert UipCommitteeMember.query.count()==1
    assert uip.UipCommitteeMeeting.query.count()==1


def test_secretary_claim_cannot_bypass_signed_provisioning(client):
    response=client.safe_post("/uip/manor-gardens/verify/committee",{"position":"Secretary","token":"forged"})
    assert response.status_code==403
    assert UipCommitteeMember.query.count()==0
    assert uip.UipCommitteeMeeting.query.count()==0
