"""Safety boundaries against real UIP handlers in a disposable PostgreSQL schema."""
import ast
from pathlib import Path
import pytest
import sqlalchemy as sa
from flask import g
from werkzeug.exceptions import Forbidden
from bootstrap import db,core,ROOT
from app.program_uip.routes import _require_role

@pytest.mark.parametrize("role",["resident","provider","outsider"])
def test_role_fails_closed(app,client,data,role):
    client.login(role)
    with app.test_request_context():
        from flask_login import login_user
        login_user(data.outsider if role=="outsider" else data.users[role])
        g.organization=data.org
        with pytest.raises(Forbidden):_require_role("manager")
        assert _require_role("manager",abort_on_fail=False) is None

def test_legitimate_and_inactive_role(app,client,data):
    with app.test_request_context():
        from flask_login import login_user
        login_user(data.users["manager"]);g.organization=data.org
        assert _require_role("manager")=="manager"
        core.CoreOrganizationMember.query.filter_by(user_id=data.users["manager"].id).one().is_active=False
        db.session.flush()
        with pytest.raises(Forbidden):_require_role("manager")

@pytest.mark.parametrize("path",["reset-genesis","remove-trigger","apply-patch","nuke-test-votes","fix-meeting","revert-meeting"])
def test_retired_maintenance_never_mutates(client,app,path):
    statements=[]
    connection=db.session.connection()
    def capture(conn,cursor,statement,*args):statements.append(statement)
    sa.event.listen(connection,"before_cursor_execute",capture)
    try:
        with client.session_transaction() as session:session.clear()
        g.pop("_login_user",None)
        response=client.get("/uip/manor-gardens/"+path)
        assert response.status_code in (302,401,410)
        assert not any(s.lstrip().split()[0].upper() in {"ALTER","DROP","CREATE","DELETE","UPDATE","INSERT"} for s in statements), statements
    finally:sa.event.remove(connection,"before_cursor_execute",capture)

@pytest.mark.parametrize("path",["about","organogram","interaction/new","settings"])
def test_navigation_has_no_ddl_or_repair(client,path):
    statements=[];connection=db.session.connection()
    def capture(conn,cursor,statement,*args):statements.append(statement)
    sa.event.listen(connection,"before_cursor_execute",capture)
    try:
        assert client.get("/uip/manor-gardens/"+path).status_code==200
        assert not any(s.lstrip().split()[0].upper() in {"ALTER","DROP","CREATE","DELETE","UPDATE","INSERT"} for s in statements), statements
    finally:sa.event.remove(connection,"before_cursor_execute",capture)

def test_no_global_patch_hook(app):
    assert not any(f.__name__=="auto_patch_db" for fs in app.before_request_funcs.values() for f in fs)

def test_single_production_user_declaration():
    declarations=[]
    for path in (ROOT/"app/models").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"))):
            if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant) and node.value.value=="user" and any(isinstance(t,ast.Name) and t.id=="__tablename__" for t in node.targets):declarations.append(path.name)
    assert declarations==["auth.py"]


def test_entry_and_router_do_not_seed_or_repair(client,data):
    from app.models.uip_governance import UipCommitteeTerm,UipCommitteeMember,UipOrganogramSeat
    term=UipCommitteeTerm(organization_id=data.org.id,term_name="Recorded term")
    db.session.add(term);db.session.flush()
    appointment=UipCommitteeMember(organization_id=data.org.id,term_id=term.id,name="Existing member",email=data.users["manager"].email,position="Committee Member",status="CURRENT")
    db.session.add(appointment);db.session.commit()
    statements=[];connection=db.session.connection()
    def capture(conn,cursor,statement,*args):statements.append(statement)
    sa.event.listen(connection,"before_cursor_execute",capture)
    try:
        assert client.get("/uip/").status_code==302
        assert client.get("/uip/manor-gardens/router").status_code==302
        assert client.get("/uip/manor-gardens/router?force=1").status_code==200
        assert appointment.position=="Committee Member"
        assert UipOrganogramSeat.query.count()==0
        assert not any(s.lstrip().split()[0].upper() in {"ALTER","DROP","CREATE","DELETE","UPDATE","INSERT"} for s in statements), statements
    finally:sa.event.remove(connection,"before_cursor_execute",capture)


def test_uip_bootstrap_never_calls_create_all():
    tree=ast.parse((ROOT/"app/__init__.py").read_text(encoding="utf-8-sig"))
    assert not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=="create_all" for n in ast.walk(tree))

def test_root_collection_excludes_isolated_uip_bootstrap():
    tree=ast.parse((ROOT/"tests/conftest.py").read_text(encoding="utf-8-sig"))
    ignores=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="collect_ignore" for t in n.targets))
    assert "uip" in ignores

@pytest.mark.parametrize("path",["secretary/organogram","secretary/onboarding-campaign","finance"])
def test_authorized_get_does_not_seed_roles_or_campaigns(client,data,path):
    client.login("owner")
    statements=[];connection=db.session.connection()
    def capture(conn,cursor,statement,*args):statements.append(statement)
    sa.event.listen(connection,"before_cursor_execute",capture)
    try:
        response=client.get("/uip/manor-gardens/"+path)
        assert response.status_code==200
        assert not any(s.lstrip().split()[0].upper() in {"ALTER","DROP","CREATE","DELETE","UPDATE","INSERT"} for s in statements),statements
    finally:sa.event.remove(connection,"before_cursor_execute",capture)
