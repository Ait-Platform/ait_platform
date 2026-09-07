"""Disposable PostgreSQL schema only; use --confcutdir=tests/uip.
Requires UIP_TEST_DATABASE_URL with a localhost database. No production fallback.
"""
import os
import uuid
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from alembic.migration import MigrationContext
from alembic.operations import Operations
from flask import Flask
from bootstrap import db, User, core, uip, ROOT, NEW_TABLES
from app.extensions import login_manager, csrf
from app.uip import uip_bp


def safe_url(value):
    if not value:
        raise RuntimeError("Set UIP_TEST_DATABASE_URL to an explicitly local PostgreSQL test database")
    url = sa.engine.make_url(value)
    if url.get_backend_name() != "postgresql" or url.host not in ("localhost", "127.0.0.1", "::1"):
        raise RuntimeError("UIP tests refuse non-local or non-PostgreSQL databases")
    if url.database != "ait_local_db" and not (url.database or "").startswith("uip_test_"):
        raise RuntimeError("UIP tests refuse this database name")
    if url.query:
        raise RuntimeError("URL query overrides are forbidden in UIP tests")
    return url


def revision():
    path = ROOT / "migrations/versions/a27c9e4b6102_uip_phase2_register_audit.py"
    spec = importlib.util.spec_from_file_location("uip_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def migrate(connection, direction="upgrade"):
    with Operations.context(MigrationContext.configure(connection)):
        getattr(revision(), direction)()


def baseline(connection):
    """Frozen synthetic baseline, independent of current application models."""
    connection.exec_driver_sql((ROOT / "tests/uip/fixtures/phase1_schema.sql").read_text())


def migrate_phase3(connection):
    spec = importlib.util.spec_from_file_location("uip_phase3_revision", ROOT / "migrations/versions/uip_p3_work_orders.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with Operations.context(MigrationContext.configure(connection)):
        module.upgrade()


@pytest.fixture(scope="session")
def phase2_engine():
    url = safe_url(os.environ.get("UIP_TEST_DATABASE_URL"))
    schema = "uip_test_" + uuid.uuid4().hex
    control = sa.create_engine(url)
    with control.begin() as connection:
        connection.execute(sa.text('CREATE SCHEMA "' + schema + '"'))
    engine = sa.create_engine(url, connect_args={"options": "-csearch_path=" + schema})
    try:
        with engine.begin() as connection:
            assert connection.scalar(sa.text("SELECT current_schema()")) == schema
            baseline(connection)
            migrate(connection)
        yield engine
    finally:
        engine.dispose()
        with control.begin() as connection:
            connection.execute(sa.text('DROP SCHEMA "' + schema + '" CASCADE'))
        control.dispose()


@pytest.fixture(scope="session")
def engine():
    url = safe_url(os.environ.get("UIP_TEST_DATABASE_URL"))
    schema = "uip_test_" + uuid.uuid4().hex
    control = sa.create_engine(url)
    with control.begin() as connection:
        connection.execute(sa.text('CREATE SCHEMA "' + schema + '"'))
    engine = sa.create_engine(url, connect_args={"options": "-csearch_path=" + schema})
    try:
        with engine.begin() as connection:
            assert connection.scalar(sa.text("SELECT current_schema()")) == schema
            baseline(connection)
            migrate(connection)
            migrate_phase3(connection)
        yield engine
    finally:
        engine.dispose()
        with control.begin() as connection:
            connection.execute(sa.text('DROP SCHEMA "' + schema + '" CASCADE'))
        control.dispose()


@pytest.fixture
def app(engine):
    with engine.connect() as probe:
        schema = probe.scalar(sa.text("SELECT current_schema()"))
    app = Flask("uip_isolated", template_folder=str(ROOT / "templates"))
    app.config.update(TESTING=True, SECRET_KEY="isolated-uip-test-only",
                      SQLALCHEMY_DATABASE_URI=engine.url,
                      SQLALCHEMY_ENGINE_OPTIONS={"connect_args": {"options": "-csearch_path=" + schema}})
    db.init_app(app)
    login_manager.init_app(app)
    csrf.init_app(app)
    login_manager.user_loader(lambda uid: db.session.get(User, int(uid)))
    app.register_blueprint(uip_bp)
    app.add_url_rule("/", endpoint="public_bp.welcome", view_func=lambda: "Test home")
    # Real UIP templates/layout; omit the unrelated global navigation.
    app.context_processor(lambda: dict(hide_navbar=True))
    with app.app_context():
        connection = engine.connect()
        transaction = connection.begin()
        original = db.session
        db.session = scoped_session(sessionmaker(bind=connection, join_transaction_mode="create_savepoint"))
        try:
            yield app
        finally:
            db.session.remove()
            db.session = original
            transaction.rollback()
            connection.close()
            db.engine.dispose()


@pytest.fixture
def data(app):
    org = core.CoreOrganization(name="Manor Gardens Test", slug="manor-gardens")
    other = core.CoreOrganization(name="Other Test", slug="other")
    db.session.add_all([org, other]); db.session.flush()
    users = {}
    for role in ("manager", "receptionist", "resident", "provider", "committee_member", "owner"):
        user = User(name=role.title(), email=role + "@example.invalid")
        db.session.add(user); db.session.flush()
        db.session.add(core.CoreOrganizationMember(organization_id=org.id, user_id=user.id, is_active=True))
        r = core.CoreRole(name=role, slug=role, organization_id=org.id)
        db.session.add(r); db.session.flush()
        db.session.add(core.CoreRoleAssignment(user_id=user.id, organization_id=org.id, role_id=r.id))
        users[role] = user
    outsider = User(name="Outsider", email="outsider@example.invalid")
    db.session.add(outsider); db.session.flush()
    db.session.add(core.CoreOrganizationMember(organization_id=other.id, user_id=outsider.id, is_active=True))
    role = core.CoreRole(name="manager", slug="manager", organization_id=other.id)
    db.session.add(role); db.session.flush()
    db.session.add(core.CoreRoleAssignment(user_id=outsider.id, organization_id=other.id, role_id=role.id))
    issue = core.CoreInteraction(organization_id=org.id, creator_id=users["resident"].id, reference="MG-TEST", title="Test issue", interaction_type="SECURITY", status="NEW")
    foreign = core.CoreInteraction(organization_id=other.id, creator_id=outsider.id, reference="OTHER-TEST", title="Other issue", interaction_type="SECURITY", status="NEW")
    db.session.add_all([issue, foreign]); db.session.commit()
    return SimpleNamespace(org=org, other=other, users=users, outsider=outsider, issue=issue, foreign=foreign)


@pytest.fixture
def client(app, data):
    client = app.test_client()
    def login(role="manager"):
        from flask import g
        g.pop("_login_user", None)
        user = data.outsider if role == "outsider" else data.users[role]
        with client.session_transaction() as session:
            session.clear(); session["_user_id"] = str(user.id); session["_fresh"] = True
        return client
    client.login = login
    def post(path, data=None):
        import re
        response = client.get("/uip/manor-gardens/interaction/new")
        token = re.search(rb'name="csrf-token" content="([^"]+)"', response.data)
        if not token:
            # Generate a token for actors denied intake, without bypassing CSRF validation.
            from flask_wtf.csrf import generate_csrf
            with app.test_request_context():
                from flask import g
                g.pop("csrf_token", None)
                token_value = generate_csrf()
                from flask import session
                raw = session["csrf_token"]
            with client.session_transaction() as session:
                session["csrf_token"] = raw
        else:
            token_value = token.group(1).decode()
        return client.post(path, data={**(data or {}), "csrf_token": token_value})
    client.safe_post = post
    return login()
