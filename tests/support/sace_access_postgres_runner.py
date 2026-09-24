"""Real HTTP journeys and SQL writes in connection-local PostgreSQL temporary tables.

Run: python -B tests/test_sace_access_postgres.py
Does not import/create the platform app or run startup migrations. Uses actual
SACE modules, auth view functions and model columns; unrelated ORM relationships
are omitted. Tables clone the local schema without sharing its sequences/data.
"""
import ast
import importlib
import json
import os
from pathlib import Path
import sys
import types
import unittest
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
import uuid
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import flask
from flask import Flask, Blueprint
from flask_login import UserMixin, current_user, login_required, login_user, logout_user
from jinja2 import ChoiceLoader, DictLoader
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
for name, relative in (("app", "app"), ("app.models", "app/models"),
                       ("app.program_sace", "app/program_sace"), ("app.auth", "app/auth"),
                       ("app.admin", "app/admin")):
    package = types.ModuleType(name)
    package.__path__ = [str(ROOT / relative)]
    sys.modules[name] = package

from app.extensions import db, login_manager, csrf


def model_module(name, path, classes):
    module = types.ModuleType(name)
    module.__dict__.update(db=db, UserMixin=UserMixin, datetime=datetime,
                          generate_password_hash=generate_password_hash,
                          check_password_hash=check_password_hash)
    nodes = []
    for node in ast.parse((ROOT / path).read_text(encoding="utf-8-sig")).body:
        if isinstance(node, ast.ClassDef) and node.name in classes:
            node.body = [n for n in node.body if not (
                isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
                and ast.unparse(n.value.func) == "db.relationship")]
            nodes.append(node)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), path, "exec"), module.__dict__)
    sys.modules[name] = module
    return module


auth_models = model_module("app.models.auth", "app/models/auth.py",
                           {"User", "AuthSubject", "AuthSubjectAdmin", "UserEnrollment", "ApprovedAdmin"})
sys.modules["app.models"].User = auth_models.User
sys.modules["app.models.subject"] = auth_models
model_module("app.models.core", "app/models/core.py", {"CoreAuditEvent"})
from app.models.sace import SaceWorkshopInteraction as Interaction, SaceDocument
# Metadata-only FK targets for unrelated models; no corresponding tables/data.
for table in list(db.metadata.tables.values()):
    for foreign_key in table.foreign_keys:
        target = foreign_key.target_fullname.rsplit(".", 1)[0]
        if target not in db.metadata.tables:
            db.Table(target, db.metadata, db.Column("id", db.Integer, primary_key=True))

from app.program_sace import access, endorsement

sace_bp = Blueprint("sace_bp", __name__)
sys.modules["app.program_sace"].sace_bp = sace_bp
auth_bp = Blueprint("auth_bp", __name__)
admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")
sys.modules["app.admin"].admin_bp = admin_bp


def view_module(name, path, env, names=None):
    module = types.ModuleType(name)
    module.__package__ = name.rpartition(".")[0]
    module.__dict__.update(env)
    nodes = [n for n in ast.parse((ROOT / path).read_text(encoding="utf-8-sig")).body
             if isinstance(n, ast.FunctionDef) and (names is None or n.name in names)]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), path, "exec"), module.__dict__)
    sys.modules[name] = module
    return module


env = {n: getattr(flask, n) for n in (
    "abort", "current_app", "flash", "g", "jsonify", "make_response", "redirect",
    "render_template", "request", "send_from_directory", "session", "url_for")}
env.update(db=db, csrf=csrf, current_user=current_user, login_required=login_required,
           login_user=login_user, logout_user=logout_user, os=os, datetime=datetime,
           json=json, text=text, sa_text=text, timedelta=timedelta)
view_module("app.program_sace.routes", "app/program_sace/routes.py",
            dict(env, sace_bp=sace_bp, SaceWorkshopInteraction=Interaction))
from app.program_sace import endorsement_routes
from app.auth.forms import LoginForm
from app.services.users import _ensure_or_create_user_from_session
auth_views = view_module("app.auth.routes", "app/auth/routes.py",
    dict(env, auth_bp=auth_bp, User=auth_models.User, AuthSubject=auth_models.AuthSubject,
         LoginForm=LoginForm, generate_password_hash=generate_password_hash,
         check_password_hash=check_password_hash,
         _ensure_or_create_user_from_session=_ensure_or_create_user_from_session),
    {"register", "register_decision", "login", "logout", "_save_reg_ctx", "dashboard_info", "bridge_dashboard"})
bridge_bp = Blueprint("bridge_bp", __name__)
view_module("isolated_bridge_routes", "app/bridge/routes.py", dict(env, bridge_bp=bridge_bp), {"bridge_dashboard"})
program_bp = Blueprint("program_bp", __name__)
view_module("isolated_program_routes", "app/program.py", dict(env, program_bp=program_bp), {"program_entry"})
from app.utils.roles import is_admin
view_module("isolated_admin_guard", "app/admin/__init__.py",
            dict(env, admin_bp=admin_bp, is_admin=is_admin), {"_guard"})
view_module("isolated_sace_management", "app/admin/security/routes.py",
            dict(env, admin_bp=admin_bp), {"sace_management"})


class AccessJourneys(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        url = make_url(dotenv_values(ROOT / ".env")["DATABASE_URL"])
        if url.get_backend_name() != "postgresql" or url.host not in ("localhost", "127.0.0.1", "::1") or url.database != "ait_local_db" or url.query:
            raise RuntimeError("Tests require verified localhost ait_local_db without URL overrides")
        cls.tables = ["user", "auth_subject", "auth_subject_admin", "user_enrollment",
                      "auth_approved_admin", "sace_workshop_interactions", "sace_documents",
                      "core_audit_event", "rdp_lesson"]
        cls.app = Flask("sace_access_tests", template_folder=str(ROOT / "templates"))
        cls.app.config.update(SECRET_KEY="test-only-sace-key", TESTING=True, WTF_CSRF_ENABLED=False,
                              SQLALCHEMY_DATABASE_URI=url,
                              SQLALCHEMY_ENGINE_OPTIONS={"poolclass": __import__("sqlalchemy").pool.StaticPool},
                              DEFAULT_LOGIN_EMAIL="")
        db.init_app(cls.app)
        with cls.app.app_context():
            with db.engine.begin() as conn:
                for table in cls.tables:
                    conn.execute(text('CREATE TEMP TABLE "' + table + '" (LIKE public."' + table + '" INCLUDING DEFAULTS)'))
                    conn.execute(text('ALTER TABLE pg_temp."' + table + '" ALTER COLUMN id DROP DEFAULT'))
                    conn.execute(text('ALTER TABLE pg_temp."' + table + '" ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY'))
                conn.execute(text("SET search_path TO pg_temp"))
        # Login inspects available application tables; expose the real temporary
        # tables to that inspection without substituting any query/ORM behavior.
        import sqlalchemy
        from unittest.mock import patch
        original_inspect = sqlalchemy.inspect
        def inspect_test_tables(bind):
            inspector = original_inspect(bind)
            if hasattr(inspector, "get_temp_table_names"):
                inspector.get_table_names = inspector.get_temp_table_names
            return inspector
        cls.inspection_patch = patch("sqlalchemy.inspect", inspect_test_tables)
        cls.inspection_patch.start()
        login_manager.init_app(cls.app)
        csrf.init_app(cls.app)
        login_manager.login_view = "auth_bp.login"
        login_manager.user_loader(lambda uid: db.session.get(auth_models.User, int(uid)))
        cls.app.register_blueprint(sace_bp)
        cls.app.register_blueprint(auth_bp)
        cls.app.register_blueprint(admin_bp)
        cls.app.register_blueprint(bridge_bp)
        cls.app.register_blueprint(program_bp)
        cls.app.add_url_rule("/", endpoint="public_bp.welcome", view_func=lambda: "Welcome")
        cls.app.add_url_rule("/admin/security", endpoint="admin_bp.security_dashboard", view_func=lambda: "Security")
        cls.app.jinja_env.globals["csrf_token"] = lambda: "test-csrf"
        cls.app.jinja_loader = ChoiceLoader([DictLoader({
            "layout.html": "{% block content %}{% endblock %}",
            "auth/login.html": "{{ form.email }}{{ form.password }}",
        }), cls.app.jinja_loader])

    @classmethod
    def tearDownClass(cls):
        with cls.app.app_context():
            db.session.remove()
            db.engine.dispose()
        cls.inspection_patch.stop()

    def setUp(self):
        with self.app.app_context():
            for table in reversed(self.tables):
                db.session.execute(text('DELETE FROM "' + table + '"'))
            db.session.add(auth_models.AuthSubject(id=900, slug="sace_endorsement", name="SACE Provider Endorsement", is_active=1, commercial_mode="free", program_type="free"))
            db.session.add(auth_models.AuthSubject(id=44, slug="sace_participant", name="Not endorsement", is_active=1))
            db.session.commit()
        self.client = self.app.test_client()

    def user(self, email, name="Test Person"):
        with self.app.app_context():
            row = auth_models.User(email=email, name=name, is_active=1)
            row.set_password("test-password")
            db.session.add(row)
            db.session.commit()
            return row.id

    def token(self, email):
        with self.app.app_context():
            return access.make_provisioning_token(email)

    def login(self, client, email, next_url=None):
        return client.post("/login", query_string={"next": next_url} if next_url else {},
                           data={"email": email, "password": "test-password"})

    def provision(self, client, email, existing=False):
        response = client.get("/sace/provisioning", query_string={"token": self.token(email)}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response = client.post("/sace/provisioning/pledge", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        if existing:
            response = self.login(client, email)
            self.assertEqual(response.location, "/sace/provisioning")
        else:
            client.get("/register?subject=sace_endorsement&next=/sace/provisioning")
            response = client.post("/register", data={"subject": "sace_endorsement",
                "next": "/sace/provisioning", "full_name": "Renielwe Test",
                "email": email, "password": "test-password"}, follow_redirects=True)
            self.assertEqual(response.status_code, 200)
        response = client.get("/sace/provisioning", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Provisioned Auditors", response.data)

    def code(self, client):
        response = client.post("/sace/provisioning/generate_code")
        self.assertEqual(response.status_code, 302)
        with self.app.app_context():
            row = Interaction.query.filter_by(activity_slug="auditor_provisioned").order_by(Interaction.id.desc()).first()
            return endorsement.payload(row)["code"], row.id

    def test_new_controller_provision_return_login_and_code(self):
        self.provision(self.client, "r@example.test")
        with self.app.app_context():
            self.assertEqual(auth_models.AuthSubjectAdmin.query.one().subject_id, 900)
            self.assertEqual(auth_models.UserEnrollment.query.one().status, "active")
            self.assertEqual(auth_models.User.query.one().name, "Renielwe Test")
            self.assertEqual(auth_models.ApprovedAdmin.query.count(), 0)
        self.code(self.client)
        self.client.get("/logout")
        response = self.login(self.client, "r@example.test")
        self.assertEqual(response.location, "/sace/provisioning")
        self.assertEqual(self.client.get(response.location).status_code, 200)
        with self.client.session_transaction() as sess:
            self.assertFalse(sess.get("is_admin"))
            self.assertNotEqual(sess.get("role"), "admin")
        self.assertEqual(self.client.get("/admin/security/sace-management").status_code, 302)
        self.assertEqual(self.client.get("/sace/join").location, "/sace/provisioning")
        self.assertEqual(self.client.get("/sace/reading").location, "/sace/provisioning")

    def test_returning_controller_bridge_and_dispatch_ignore_blank_endpoints(self):
        self.provision(self.client, "r@example.test")
        with self.app.app_context():
            subject = auth_models.AuthSubject.query.filter_by(slug="sace_endorsement").one()
            subject.start_endpoint = None
            subject.admin_start_endpoint = None
            subject.bypass_dashboard_endpoint = None
            subject.is_hidden_on_bridge = True
            subject.program_type = "admin"
            db.session.commit()
        for target in ("/dashboard", "/bridge"):
            self.client.get("/logout")
            response = self.login(self.client, "r@example.test", target)
            self.assertEqual(response.location, target)
            response = self.client.get(target)
            self.assertEqual(response.location, "/sace/provisioning")
            self.assertEqual(self.client.get(response.location).status_code, 200)
            with self.client.session_transaction() as state:
                self.assertFalse(state.get("is_admin"))
                self.assertEqual(state.get("role"), "user")
        response = self.client.get("/program/sace_endorsement/start", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Provisioned Auditors", response.data)
        self.assertEqual(self.client.get("/dashboard?force=1").location, "/sace/provisioning")

    def test_central_sace_dispatch_preserves_assignment_boundary(self):
        # Anonymous visitors must authenticate; mere enrollment is not a grant.
        response = self.client.get("/program/sace_endorsement/start", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Provisioned Auditors", response.data)
        self.user("ordinary@example.test")
        self.login(self.client, "ordinary@example.test", "/program/sace_endorsement/start")
        response = self.client.get("/program/sace_endorsement/start", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Provisioned Auditors", response.data)
        with self.app.app_context():
            self.assertEqual(auth_models.AuthSubjectAdmin.query.count(), 0)

    def test_existing_controller_account_and_enrollment_reused(self):
        uid = self.user("existing@example.test")
        with self.app.app_context():
            db.session.add(auth_models.UserEnrollment(user_id=uid, subject_id=900, status="active",
                local_currency="ZAR", local_amount_cents=0, zar_amount_cents=0))
            db.session.commit()
        self.provision(self.client, "existing@example.test", existing=True)
        self.provision(self.client, "existing@example.test", existing=True)
        with self.app.app_context():
            self.assertEqual(auth_models.User.query.count(), 1)
            self.assertEqual(auth_models.UserEnrollment.query.count(), 1)
            self.assertEqual(auth_models.AuthSubjectAdmin.query.count(), 1)
        self.code(self.client)

    def test_existing_registration_and_pledge_complete_from_named_link(self):
        uid = self.user("r@example.test")
        with self.app.app_context():
            db.session.add(auth_models.UserEnrollment(user_id=uid, subject_id=900, status="active",
                local_currency="ZAR", local_amount_cents=0, zar_amount_cents=0))
            db.session.add(Interaction(user_id=uid, activity_slug="admin_patent_pledge",
                                       response_data="Admin accepted IP pledge"))
            db.session.commit()
        self.login(self.client, "r@example.test")
        response = self.client.get("/sace/provisioning", query_string={"token": self.token("r@example.test")}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.code(self.client)
        with self.app.app_context():
            self.assertEqual(auth_models.AuthSubjectAdmin.query.one().subject_id, 900)
            self.assertEqual(auth_models.UserEnrollment.query.count(), 1)
            self.assertEqual(Interaction.query.filter_by(activity_slug="admin_patent_pledge").count(), 1)

    def test_auditor_new_registration_claim_and_board(self):
        self.user("r@example.test")
        self.provision(self.client, "r@example.test", existing=True)
        code, row_id = self.code(self.client)
        auditor = self.app.test_client()
        self.assertEqual(auditor.post("/sace/join", data={"code": code}).location, "/sace/auditor_pledge")
        response = auditor.post("/sace/auditor_pledge")
        auditor.get(response.location)
        response = auditor.post("/register", data={"subject": "sace_endorsement", "next": "/sace/claim_code",
            "full_name": "Auditor Full Name", "email": "a@example.test", "password": "test-password"}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Auditor Board", response.data)
        with self.app.app_context():
            state = endorsement.payload(db.session.get(Interaction, row_id))
            user = auth_models.User.query.filter_by(email="a@example.test").one()
            self.assertEqual(state["claimed_by_user_id"], user.id)
            self.assertEqual(state["first_name"], "Auditor Full Name")
            self.assertEqual(auth_models.AuthSubjectAdmin.query.count(), 1)
        self.assertEqual(auditor.post("/sace/provisioning/generate_code").status_code, 403)
        response = auditor.get("/program/sace_endorsement/start", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Auditor Board", response.data)

    def test_auditor_existing_login_preserves_pending_join(self):
        self.user("r@example.test")
        self.user("a@example.test", "Existing Auditor")
        self.provision(self.client, "r@example.test", existing=True)
        code, _ = self.code(self.client)
        auditor = self.app.test_client()
        auditor.post("/sace/join", data={"code": code})
        auditor.post("/sace/auditor_pledge")
        response = self.login(auditor, "a@example.test")
        self.assertEqual(response.location, "/sace/claim_code")
        response = auditor.get(response.location, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Auditor Board", response.data)
        auditor.get("/logout")
        self.assertEqual(self.login(auditor, "a@example.test").location, "/sace/reading")

    def test_no_appointment_from_enrollment_or_bare_url(self):
        uid = self.user("a@example.test")
        with self.app.app_context():
            db.session.add(auth_models.AuthSubjectAdmin(email="a@example.test", subject_id=44))
            db.session.commit()
        self.login(self.client, "a@example.test")
        response = self.client.get("/sace/dashboard", follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"provisioning link", response.data)
        self.assertNotIn(b"Auditor Board", response.data)
        self.assertEqual(self.client.post("/sace/provisioning/pledge").status_code, 400)
        self.assertEqual(self.client.post("/sace/provisioning/generate_code").status_code, 403)
        with self.app.app_context():
            self.assertEqual(auth_models.AuthSubjectAdmin.query.one().subject_id, 44)

    def test_expiry_checked_on_join_and_again_at_claim(self):
        self.user("r@example.test")
        self.user("a@example.test")
        self.provision(self.client, "r@example.test", existing=True)
        code, row_id = self.code(self.client)
        auditor = self.app.test_client()
        auditor.post("/sace/join", data={"code": code})
        auditor.post("/sace/auditor_pledge")
        with self.app.app_context():
            row = db.session.get(Interaction, row_id)
            state = endorsement.payload(row)
            state["expires_at"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
            row.response_data = json.dumps(state)
            db.session.commit()
        response = self.login(auditor, "a@example.test")
        self.assertEqual(auditor.get(response.location).status_code, 409)
        fresh = self.app.test_client()
        response = fresh.post("/sace/join", data={"code": code}, follow_redirects=True)
        self.assertIn(b"expired", response.data)
        with self.app.app_context():
            self.assertEqual(endorsement.payload(db.session.get(Interaction, row_id))["status"], "Unclaimed")

    def test_named_link_cannot_appoint_different_account(self):
        self.user("wrong@example.test")
        self.login(self.client, "wrong@example.test")
        response = self.client.get("/sace/provisioning", query_string={"token": self.token("r@example.test")}, follow_redirects=True)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.client.post("/sace/provisioning/pledge").status_code, 400)
        with self.app.app_context():
            self.assertEqual(auth_models.AuthSubjectAdmin.query.count(), 0)



    def test_platform_admin_issues_named_link_without_granting_account(self):
        import re
        import html
        uid = self.user("platform@example.test")
        with self.app.app_context():
            db.session.execute(text("INSERT INTO auth_approved_admin (email, active) VALUES (:email, 1)"), {"email": "platform@example.test"})
            db.session.commit()
        self.login(self.client, "platform@example.test")
        response = self.client.post("/admin/security/sace-management",
                                    data={"action": "provision_controller", "email": "r@example.test"})
        self.assertEqual(response.status_code, 200)
        match = re.search(rb'value="(http[^"]+/sace/provisioning[^"]+)"', response.data)
        self.assertIsNotNone(match)
        link = html.unescape(match.group(1).decode())
        from urllib.parse import parse_qs
        token = parse_qs(urlparse(link).query)["token"][0]
        with self.app.app_context():
            self.assertEqual(access.provisioning_invitation(token)["email"], "r@example.test")
            self.assertEqual(auth_models.AuthSubjectAdmin.query.count(), 0)
            self.assertEqual(auth_models.User.query.count(), 1)

    def test_tampered_and_expired_provisioning_links_do_not_appoint(self):
        from unittest.mock import patch
        self.assertEqual(self.client.get("/sace/provisioning?token=invalid").status_code, 400)
        with patch("time.time", return_value=1000):
            token = self.token("r@example.test")
        response = self.client.get("/sace/provisioning", query_string={"token": token})
        self.assertEqual(response.status_code, 400)
        with self.app.app_context():
            self.assertEqual(auth_models.AuthSubjectAdmin.query.count(), 0)

    def test_used_and_malformed_expiry_codes_cannot_join(self):
        self.user("r@example.test")
        self.provision(self.client, "r@example.test", existing=True)
        code, row_id = self.code(self.client)
        for updates in ({"status": "Claimed"}, {"status": "Unclaimed", "expires_at": "not-a-date"}):
            with self.app.app_context():
                row = db.session.get(Interaction, row_id)
                state = endorsement.payload(row)
                state.update(updates)
                row.response_data = json.dumps(state)
                db.session.commit()
            auditor = self.app.test_client()
            response = auditor.post("/sace/join", data={"code": code}, follow_redirects=True)
            self.assertEqual(response.status_code, 200)
            with auditor.session_transaction() as sess:
                self.assertNotIn("pending_sace_code", sess)

    def test_existing_account_registration_resumes_join(self):
        self.user("r@example.test")
        self.user("a@example.test", "Existing Auditor")
        self.provision(self.client, "r@example.test", existing=True)
        code, _ = self.code(self.client)
        auditor = self.app.test_client()
        auditor.post("/sace/join", data={"code": code})
        response = auditor.post("/sace/auditor_pledge")
        auditor.get(response.location)
        response = auditor.post("/register", data={"subject": "sace_endorsement",
            "next": "/sace/claim_code", "full_name": "Existing Auditor",
            "email": "a@example.test", "password": "test-password"}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Auditor Board", response.data)
        with self.app.app_context():
            self.assertEqual(auth_models.User.query.count(), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
