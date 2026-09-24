"""Treasurer appointment authority, isolated PostgreSQL only."""
from datetime import date
from decimal import Decimal
import pytest
from bootstrap import db, core, uip
from app.models.uip_governance import UipCommitteeMember, UipCommitteeTerm
from app.program_uip.services import finance as f
from test_finance import values, post
from phase3_helpers import key

BASE = "/uip/manor-gardens"
FIN = BASE + "/finance"


@pytest.fixture
def treasurer(data):
    user = data.users["resident"]
    core.CoreRoleAssignment.query.filter_by(user_id=user.id).delete()
    core.CoreOrganizationMember.query.filter_by(user_id=user.id).delete()
    term = UipCommitteeTerm(organization_id=data.org.id, term_name="Elected term")
    db.session.add(term); db.session.flush()
    appointment = UipCommitteeMember(organization_id=data.org.id, term_id=term.id,
        name=user.name, email=user.email, position="Treasurer", status="CURRENT")
    db.session.add(appointment); db.session.commit()
    return appointment


def identity_state():
    return {m.__tablename__: [tuple(getattr(row, c.name) for c in m.__table__.columns)
        for row in m.query.order_by(m.id).all()] for m in
        (core.CoreRoleAssignment, core.CoreOrganizationMember, UipCommitteeMember)}


def test_board_sidebar_finance_and_secretary_no_identity_writes(client, data, treasurer):
    client.login("resident")
    before = identity_state()
    response = client.get(BASE + "/treasurer-workspace")
    assert response.status_code == 200
    for label in (b"Treasurer Dashboard", b"Sec Control", b"Financial Tools", b"Draft Resolution"):
        assert label in response.data
    assert b'/secretary-workspace' in response.data and b'/finance' in response.data
    page = client.get(FIN)
    assert page.status_code == 200 and b"Record transaction" in page.data
    assert b"Financial Tools" in page.data and b"Sec Control" in page.data
    assert client.get(BASE + "/secretary-workspace").status_code == 200
    assert client.get(FIN + "/transactions/new").status_code == 200
    assert identity_state() == before
    assert core.CoreRoleAssignment.query.filter_by(user_id=data.users["resident"].id).count() == 0


@pytest.mark.parametrize("role", ["manager", "owner", "resident", "committee_member", "provider", "outsider"])
def test_non_treasurer_denied_private_routes(client, data, role):
    client.login(role)
    for path in ("/treasurer-workspace", "/treasurer-voting-room", "/treasurer-resolution/123"):
        assert client.get(BASE + path).status_code == 403
    assert client.safe_post(BASE + "/treasurer-resolution/123/vote", {"vote":"YEA"}).status_code == 403


@pytest.mark.parametrize("change", ["former", "foreign", "different_person", "different_position"])
def test_current_same_org_appointment_required(client, data, treasurer, change):
    if change == "former": treasurer.status = "FORMER"
    elif change == "foreign": treasurer.organization_id = data.other.id
    elif change == "different_person": treasurer.email = "other@example.invalid"
    else: treasurer.position = "Secretary"
    db.session.commit(); client.login("resident")
    assert client.get(BASE + "/treasurer-workspace").status_code == 403
    assert client.get(FIN).status_code == 403
    assert client.safe_post(FIN + "/transactions/new", values()).status_code == 403


def test_phase10_working_operations_without_core_role(client, data, treasurer):
    client.login("resident")
    before = identity_state()
    year = f.financial_year()[0].year
    budget = dict(year=year, category="Repairs", amount="5000", expected_revision="0",
        revised_date=date.today().isoformat(), description="Annual budget")
    post(client, FIN + "/budget", budget)
    post(client, FIN + "/budget", {**budget, "amount":"6000", "expected_revision":"1"})
    post(client, FIN + "/transactions/new", values(amount="10000"))
    post(client, FIN + "/transactions/new", values(kind="ADJUSTMENT", amount="-20"))
    post(client, FIN + "/commitments", values(amount="1000"))
    commitment = f.Commitment.query.one()
    post(client, FIN + f"/commitments/{commitment.id}", dict(operation="revise", expected_version=commitment.version, amount="1200", reason="Revised scope"))
    post(client, FIN + "/transactions/new", values(kind="EXPENDITURE", amount="200", commitment_id=commitment.id))
    payment = f.Transaction.query.filter_by(kind="EXPENDITURE").one()
    assert commitment.status == "PARTIALLY_PAID"
    post(client, FIN + f"/transactions/{payment.id}", dict(operation="reverse", expected_version=payment.version,
        request_key=key(), transaction_date=date.today().isoformat(), reason="Correction"))
    post(client, FIN + "/transactions/new", values(kind="EXPENDITURE", amount="250", commitment_id=commitment.id, correction_of_id=payment.id))
    post(client, FIN + f"/commitments/{commitment.id}", dict(operation="visibility", expected_version=commitment.version, visibility="PRIVATE"))
    post(client, FIN + f"/commitments/{commitment.id}", dict(operation="cancel", expected_version=commitment.version, reason="Remaining scope cancelled"))
    overview = f.overview(data.org.id, data.users["resident"].id)
    assert overview["cash"] == Decimal("9730") and overview["outstanding"] == 0
    for report in ("transactions", "income-expenditure", "budget", "commitments", "category", "provider"):
        assert client.get(FIN + f"/reports/{report}.csv").status_code == 200
    assert identity_state() == before
    events = uip.UipAuditEvent.query.filter(uip.UipAuditEvent.action.like("finance.%")).all()
    assert events and all(e.actor_user_id == data.users["resident"].id for e in events)


def test_member_transparency_no_write_ui(client, data):
    client.login("resident")
    for path in ("", "/transactions", "/budget", "/commitments"):
        page = client.get(FIN + path)
        assert page.status_code == 200
        assert b"Log Transaction" not in page.data and b"Record transaction" not in page.data
        assert b'<form method="post"' not in page.data
    assert client.safe_post(FIN + "/transactions/new", values()).status_code == 403
