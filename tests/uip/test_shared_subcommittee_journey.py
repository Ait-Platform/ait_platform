"""Focused shared membership and returning-RP HTTP journeys; disposable PostgreSQL."""
from datetime import date, timedelta
import importlib.util
import pytest
from alembic.operations import Operations
from alembic.migration import MigrationContext
from bootstrap import db, core, uip, ROOT
from app.models.uip_governance import UipCommitteeMember, UipSubcommitteeMembership as Membership
from app.models.uip_proposal import UipProposal
from app.models.uip_finance import UipFinanceTransaction, UipFinanceCommitment
from test_subcomm_tools import subcomm_setup, make_resolution
from test_ratepayer_journey import vault, snapshot
BASE = "/uip/manor-gardens"
VALUES = dict(title="Greening plan", description="Plant trees", motivation="Shade", proposed_budget="500.00")


@pytest.fixture(scope="session", autouse=True)
def membership_schema(engine):
    spec = importlib.util.spec_from_file_location("membership_revision", ROOT / "migrations/versions/uip_p56_subcommittee_membership.py")
    revision = importlib.util.module_from_spec(spec); spec.loader.exec_module(revision)
    with engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            revision.upgrade()


@pytest.fixture
def team(data, subcomm_setup):
    org, green, security, *_ = subcomm_setup
    term = uip.UipCommitteeTerm.query.first() if hasattr(uip, 'UipCommitteeTerm') else None
    from app.models.uip_governance import UipCommitteeTerm
    term = UipCommitteeTerm.query.filter_by(organization_id=org.id).first()
    rows = []
    for role in ("resident", "provider"):
        user = data.users[role]
        member = UipCommitteeMember(organization_id=org.id, term_id=term.id, name=user.name,
            email=user.email, user_id=user.id, position="Committee Member", status="CURRENT")
        db.session.add(member); db.session.flush()
        appointment = Membership(organization_id=org.id, subcommittee_id=green.id, member_id=member.id,
            appointing_resolution_id=green.establishing_resolution_id, valid_from=date.today(), recorded_by=data.users["receptionist"].id)
        db.session.add(appointment); rows.append(appointment)
    # Four existing verified EXCO capacities, separate from the two Subcommittee members.
    for role, position in (("manager", "Vice-Chairperson"), ("committee_member", "Treasurer")):
        user = data.users[role]
        db.session.add(UipCommitteeMember(organization_id=org.id, term_id=term.id, name=user.name,
            email=user.email, position=position, status="CURRENT"))
    db.session.commit()
    return green, security, rows


@pytest.mark.parametrize("role", ["resident", "provider"])
def test_same_board_members_and_isolation(client, data, team, role):
    green, security, _ = team
    client.login(role)
    response = client.get(BASE + "/sub-comm-tools")
    assert response.status_code == 302 and response.location.endswith(f"/{green.id}/board")
    assert client.get(response.location).status_code == 200
    page = client.get(BASE + f"/subcommittee/{green.id}/members")
    assert page.status_code == 200
    assert b"Resident" in page.data and b"Provider" in page.data
    assert client.get(BASE + f"/subcommittee/{security.id}/board").status_code == 403
    assert client.get(BASE + "/router").status_code == 302
    assert client.get(BASE + "/router").location.endswith('/sub-comm-tools')


@pytest.mark.parametrize("role", ["resident", "provider"])
def test_any_member_draft_shared_internal_then_one_exco_matter(client, data, team, role):
    green, _, _ = team
    client.login(role)
    response = client.safe_post(BASE + f"/subcommittee/{green.id}/proposals", VALUES)
    assert response.status_code == 302
    row = UipProposal.query.one()
    assert row.originator_id == data.users[role].id and row.originating_subcommittee_id == green.id
    other = "provider" if role == "resident" else "resident"
    client.login(other)
    assert client.get(response.location).status_code == 200
    assert row.title.encode() in client.get(BASE + f"/subcommittee/{green.id}/proposals").data
    for official in ("owner", "receptionist", "manager", "committee_member"):
        client.login(official)
        assert client.get(response.location).status_code == 404
        assert row.title.encode() not in client.get(BASE + "/sub-comm-control").data
    client.login(other)
    assert client.safe_post(BASE + f"/proposals/{row.id}/submit").status_code == 302
    assert row.status == "SUBMITTED"
    assert client.safe_post(BASE + f"/proposals/{row.id}/convert", {"meeting_id":1}).status_code == 403
    for official in ("owner", "receptionist", "manager", "committee_member"):
        client.login(official)
        page = client.get(BASE + "/sub-comm-control")
        assert page.status_code == 200 and b"Greening" in page.data and row.title.encode() in page.data
        assert b"1 awaiting governance attention" in page.data and b"#fee2e2" in page.data
        assert client.get(response.location).status_code == 200
    assert UipProposal.query.count() == 1 and row.resolution_id is None
    assert UipFinanceTransaction.query.count() == UipFinanceCommitment.query.count() == 0


def test_generic_role_and_responsible_seat_do_not_grant_membership(client, data, team):
    for role in ("committee_member", "owner"):
        generic = core.CoreRole(organization_id=data.org.id, slug="subcommittee_member", name="Subcommittee")
        if role == "committee_member":
            db.session.add(generic); db.session.flush()
            db.session.add(core.CoreRoleAssignment(organization_id=data.org.id, user_id=data.users[role].id, role_id=generic.id)); db.session.commit()
        client.login(role)
        assert client.get(BASE + "/sub-comm-tools").status_code == 403
        assert client.get(BASE + f"/subcommittee/{team[0].id}/board").status_code == 403


@pytest.mark.parametrize("invalid", ["expired", "future", "former", "unadopted", "member_former", "wrong_identity"])
def test_membership_validity(client, data, team, invalid):
    row = team[2][0]
    member = db.session.get(UipCommitteeMember, row.member_id)
    if invalid == "expired": row.valid_from=date.today()-timedelta(days=10); row.valid_to=date.today()-timedelta(days=1)
    elif invalid == "future": row.valid_from=date.today()+timedelta(days=1)
    elif invalid == "former": row.status="FORMER"
    elif invalid == "member_former": member.status="FORMER"
    elif invalid == "wrong_identity": member.user_id=data.users["manager"].id
    else:
        resolution=make_resolution(data.org.id, data.users["manager"].id, "Not adopted", "DRAFT")
        row.appointing_resolution_id=resolution.id
    db.session.commit(); client.login("resident")
    assert client.get(BASE + f"/subcommittee/{team[0].id}/board").status_code == 403


def test_cross_org_member_denied(client, data, team):
    client.login("outsider")
    assert client.get(BASE + f"/subcommittee/{team[0].id}/board").status_code == 403
    assert client.get('/uip/other/subcommittee/' + str(team[0].id) + '/board').status_code == 404


def test_verified_rp_login_dispatch_without_role_writes(client, data):
    vault(data)
    core.CoreRoleAssignment.query.filter_by(user_id=data.users["resident"].id).delete()
    core.CoreOrganizationMember.query.filter_by(user_id=data.users["resident"].id).delete()
    db.session.commit(); client.login("resident")
    before = snapshot()
    for path in ('/router', '/dashboard'):
        response=client.get(BASE+path)
        assert response.status_code == 302 and response.location.endswith('/ratepayer-workspace')
        assert client.get(response.location).status_code == 200
    assert snapshot() == before


def test_unverified_rp_cannot_enter_board(client, data):
    client.login("resident")
    response = client.get(BASE + '/ratepayer-workspace')
    assert response.status_code == 302 and response.location.endswith('/verify/ratepayer')
    assert client.get(BASE + '/verify/ratepayer').status_code == 200
    assert client.get(BASE + '/router').location.endswith('/dashboard')


def test_other_vault_identity_does_not_bypass(client, data):
    vault(data, email="someone_else@example.invalid")
    client.login("resident")
    assert client.get(BASE + '/verify/ratepayer').status_code == 403
    response = client.get(BASE + '/ratepayer-workspace')
    assert response.status_code == 302 and response.location.endswith('/verify/ratepayer')


def test_secretary_records_resolution_appointment_http(client, data, team):
    member = db.session.get(UipCommitteeMember, team[2][0].member_id)
    values = dict(action="record_subcommittee_membership", subcommittee_id=team[1].id, member_id=member.id,
        resolution_id=team[1].establishing_resolution_id, valid_from=date.today().isoformat())
    client.login("receptionist")
    response=client.safe_post(BASE + '/secretary/organogram', values)
    assert response.status_code in (200,302)
    assert Membership.query.filter_by(subcommittee_id=team[1].id,member_id=member.id).count() == 1
    client.login("resident")
    assert client.get(BASE + f'/subcommittee/{team[1].id}/board').status_code == 200
