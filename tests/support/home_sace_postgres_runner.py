"""Focused HOME HTTP checks in temporary PostgreSQL tables; no application startup.
Migration upgrade/downgrade is exercised in a transactionally rolled-back private schema.
"""
import importlib.util
import re
import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from sqlalchemy import text, create_engine, inspect
from sqlalchemy.schema import CreateTable, CreateIndex
from alembic.migration import MigrationContext
from alembic.operations import Operations

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("sace_test_support", ROOT / "tests/support/sace_access_postgres_runner.py")
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
from app.program_sace_home import home_sace_bp, service as s
from app.models.sace_home import (HomeController, HomeProvisioning, HomeInvitation, HomePledge,
    HomeAssignment, HomeDocument, HomeDocumentVersion, HomeEvidence, now)
from app.extensions import db

HOME_TABLES = [t for t in db.metadata.sorted_tables if t.name.startswith("sace_home_")]


class HomeFoundation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h.AccessJourneys.setUpClass.__func__(cls)
        cls.app.register_blueprint(home_sace_bp)
        with cls.app.app_context():
            with db.engine.begin() as conn:
                conn.execute(text('ALTER TABLE pg_temp."user" ADD PRIMARY KEY (id)'))
                conn.execute(text('ALTER TABLE pg_temp."user" ADD UNIQUE (email)'))
                for table in HOME_TABLES:
                    ddl = str(CreateTable(table).compile(dialect=conn.dialect))
                    conn.execute(text(ddl.replace("CREATE TABLE", "CREATE TEMPORARY TABLE", 1)))
                    for index in table.indexes:
                        conn.execute(CreateIndex(index))
        cls.tables += [t.name for t in HOME_TABLES]
        cls.documents = tempfile.TemporaryDirectory(prefix="home-sace-documents-")
        cls.app.config["SACE_HOME_DOCUMENT_ROOT"] = cls.documents.name

    @classmethod
    def tearDownClass(cls):
        cls.documents.cleanup()
        h.AccessJourneys.tearDownClass.__func__(cls)

    setUp = h.AccessJourneys.setUp
    user = h.AccessJourneys.user
    login = h.AccessJourneys.login

    def provision_home(self, email="home-r@example.test"):
        with self.app.app_context():
            token = s.issue_provisioning(email, "local-test")
            db.session.commit()
        self.assertEqual(self.client.get("/sace/home/provisioning?token=" + token, follow_redirects=True).status_code, 200)
        self.assertEqual(self.client.post("/sace/home/provisioning", data={"signature": "HOME R", "accept": "yes"}).status_code, 302)
        result = self.client.post("/register", data={"subject": s.SUBJECT, "full_name": "HOME R",
            "email": email, "password": "test-password"}, follow_redirects=True)
        self.assertEqual(result.status_code, 200, result.data[:500])
        self.assertIn(b"HOME Control Centre", result.data)
        with self.app.app_context():
            return HomeController.query.one().user_id

    def code_home(self):
        result = self.client.post("/sace/home/control/codes")
        self.assertEqual(result.status_code, 200)
        return re.search(rb"HOME-[A-F0-9]{24}", result.data).group().decode()

    def join_home(self, code, email="home-a@example.test", existing=False):
        client = self.app.test_client()
        self.assertEqual(client.post("/sace/home/join", data={"code": code}).location, "/sace/home/pledge")
        self.assertEqual(client.get("/sace/home/pledge").status_code, 200)
        self.assertEqual(client.post("/sace/home/pledge", data={"signature": "HOME Auditor", "accept": "yes"}).status_code, 302)
        if existing:
            result = self.login(client, email, "/sace/home/claim")
            self.assertEqual(result.location, "/sace/home/claim")
            result = client.get(result.location, follow_redirects=True)
        else:
            result = client.post("/register", data={"subject": s.SUBJECT, "full_name": "HOME A",
                "email": email, "password": "test-password"}, follow_redirects=True)
        self.assertEqual(result.status_code, 200, result.data[:500])
        self.assertIn(b"HOME Auditor Board", result.data)
        with self.app.app_context():
            row = HomeAssignment.query.order_by(HomeAssignment.id.desc()).first()
            return client, row.id

    def test_new_and_returning_home_controller(self):
        uid = self.provision_home()
        self.code_home()
        self.assertEqual(self.client.get("/sace/home/control/documents").status_code, 200)
        self.assertIn(b"HOME Workshop / Participant Manual", self.client.get("/sace/home/control/documents").data)
        with self.app.app_context():
            self.assertEqual(HomePledge.query.filter_by(role="controller").count(), 1)
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.count(), 0)
            self.assertEqual(h.Interaction.query.count(), 0)
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "home-r@example.test", "/sace/home/").location, "/sace/home/")
        self.assertEqual(self.client.get("/sace/home/", follow_redirects=True).status_code, 200)
        self.assertEqual(self.client.get("/sace/reading").status_code, 403)

    def test_new_auditor_summary_and_returning_assignment(self):
        self.provision_home()
        client, aid = self.join_home(self.code_home())
        base = f"/sace/home/assignments/{aid}"
        self.assertEqual(client.get(base + "/summary").status_code, 200)
        self.assertEqual(client.post(base + "/summary").location, base + "/board")
        self.assertIn(b"Examined", client.get(base + "/board").data)
        with self.app.app_context():
            self.assertEqual(HomePledge.query.filter_by(role="auditor").count(), 1)
            self.assertEqual(HomeEvidence.query.filter_by(item="summary", event="examined").count(), 1)
            self.assertEqual(h.Interaction.query.count(), 0)
            self.assertEqual(h.endorsement.assignments(db.session.get(HomeAssignment, aid).auditor_id), [])
        self.assertEqual(client.get(base + "/experience").status_code, 200)
        self.assertEqual(client.post(base + "/completion").status_code, 409)
        self.assertEqual(client.get("/sace/reading").status_code, 403)
        client.get("/logout")
        self.login(client, "home-a@example.test", "/sace/home/")
        self.assertEqual(client.get("/sace/home/").location, base + "/board")

    def test_existing_account_join_and_assignment_ownership(self):
        self.provision_home()
        uid = self.user("existing-home-a@example.test")
        client, aid = self.join_home(self.code_home(), "existing-home-a@example.test", existing=True)
        with self.app.app_context():
            self.assertEqual(db.session.get(HomeAssignment, aid).auditor_id, uid)
            self.assertEqual(h.auth_models.User.query.count(), 2)
        self.assertEqual(self.client.get(f"/sace/home/assignments/{aid}/board").status_code, 403)
        self.assertEqual(client.get("/sace/home/control").status_code, 403)

    def test_existing_registration_branch(self):
        self.provision_home()
        uid = self.user("home-a@example.test")
        client, aid = self.join_home(self.code_home())
        with self.app.app_context():
            self.assertEqual(db.session.get(HomeAssignment, aid).auditor_id, uid)

    def test_invalid_expired_claimed_codes(self):
        self.provision_home()
        other = self.app.test_client()
        self.assertEqual(other.post("/sace/home/join", data={"code": "NOT-HOME"}).status_code, 400)
        expired = self.code_home()
        with self.app.app_context():
            row = HomeInvitation.query.filter_by(code_hash=s.digest(expired)).one()
            row.expires_at = now() - timedelta(seconds=1)
            db.session.commit()
        self.assertEqual(other.post("/sace/home/join", data={"code": expired}).status_code, 400)
        code = self.code_home()
        self.join_home(code)
        self.assertEqual(other.post("/sace/home/join", data={"code": code}).status_code, 400)

    def test_pledge_and_provisioning_protection(self):
        self.assertEqual(self.client.get("/sace/home/provisioning").status_code, 403)
        self.provision_home()
        code = self.code_home()
        client = self.app.test_client()
        client.post("/sace/home/join", data={"code": code})
        self.assertEqual(client.post("/sace/home/pledge", data={"signature": "Test"}).status_code, 400)
        self.assertEqual(client.get("/register?subject=" + s.SUBJECT).status_code, 400)
        self.assertEqual(self.client.post("/sace/home/join", data={"code": code}).status_code, 302)
        self.client.post("/sace/home/pledge", data={"signature": "R", "accept": "yes"})
        self.assertEqual(self.client.get("/sace/home/claim").status_code, 403)

    def test_litre_authority_code_and_evidence_do_not_grant_home(self):
        uid = self.user("litre@example.test")
        with self.app.app_context():
            db.session.add(h.auth_models.AuthSubjectAdmin(subject_id=900, email="litre@example.test"))
            row = h.Interaction(user_id=uid, activity_slug="auditor_provisioned",
                response_data=h.json.dumps({"code": "LITRE-TEST", "status": "Claimed", "claimed_by_user_id": uid}))
            db.session.add(row)
            db.session.flush()
            db.session.add(h.Interaction(user_id=uid, workshop_session_id=h.endorsement.room(row),
                activity_slug="map_reviewed", response_data="{}"))
            db.session.commit()
        self.login(self.client, "litre@example.test", "/sace/home/")
        self.assertEqual(self.client.get("/sace/home/control").status_code, 403)
        self.assertEqual(self.client.post("/sace/home/join", data={"code": "LITRE-TEST"}).status_code, 400)
        with self.app.app_context():
            self.assertEqual(HomeAssignment.query.count(), 0)
            self.assertEqual(HomeEvidence.query.count(), 0)
        self.assertEqual(self.client.get("/sace/provisioning").status_code, 200)
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "litre@example.test", "/sace/reading").location, "/sace/provisioning")

    def test_home_code_rejected_by_litre(self):
        self.provision_home()
        code = self.code_home()
        client = self.app.test_client()
        result = client.post("/sace/join", data={"code": code}, follow_redirects=True)
        self.assertNotEqual(result.request.path, "/sace/auditor_pledge")
        with client.session_transaction() as state:
            self.assertFalse(state.get("pending_sace_code"))
        with self.app.app_context():
            self.assertEqual(h.Interaction.query.count(), 0)
            self.assertEqual(HomeInvitation.query.one().status, "unclaimed")

    def test_document_version_examination_and_unavailable_manuals(self):
        self.provision_home()
        client, aid = self.join_home(self.code_home())
        base = f"/sace/home/assignments/{aid}"
        self.assertEqual(client.post(base + "/materials/participant_manual").status_code, 409)
        with self.app.app_context():
            path = Path(self.documents.name) / "test-evidence.pdf"
            path.write_bytes(b"%PDF-1.4\nHOME test fixture only")
            version = s.publish_document(HomeController.query.one(), "assessment", "test-v1",
                path.name, {"fixture": True})
            db.session.commit()
            vid = version.id
        target = base + "/materials/assessment"
        self.assertEqual(client.post(target, data={"version_id": vid}).status_code, 409)
        with client.get(f"/sace/home/documents/{vid}/content?assignment_id={aid}") as response:
            self.assertEqual(response.status_code, 200)
        self.assertEqual(client.post(target, data={"version_id": vid}).status_code, 302)
        with self.app.app_context():
            self.assertTrue(s.examined(db.session.get(HomeAssignment, aid), "assessment", vid))
            version = db.session.get(HomeDocumentVersion, vid)
            version2 = HomeDocumentVersion(document_id=version.document_id, version="test-v2", storage_key=version.storage_key,
                sha256=version.sha256, source_manifest={"fixture": True}, approved_by=version.approved_by)
            db.session.add(version2)
            db.session.commit()
            self.assertFalse(next(x for x in s.board_items(db.session.get(HomeAssignment, aid)) if x["kind"] == "assessment")["examined"])
        self.assertEqual(self.app.test_client().get(f"/sace/home/documents/{vid}/content").status_code, 302)

    def test_same_identity_evidence_stays_separate(self):
        self.provision_home()
        uid = self.user("dual-a@example.test")
        with self.app.app_context():
            row = h.Interaction(user_id=uid, activity_slug="auditor_provisioned",
                response_data=h.json.dumps({"code": "LITRE-ONLY", "status": "Claimed", "claimed_by_user_id": uid}))
            db.session.add(row)
            db.session.flush()
            room = h.endorsement.room(row)
            db.session.add(h.Interaction(user_id=uid, workshop_session_id=room,
                activity_slug="map_reviewed", response_data="{}"))
            db.session.commit()
        client, aid = self.join_home(self.code_home(), "dual-a@example.test", existing=True)
        with self.app.app_context():
            self.assertFalse(s.examined(db.session.get(HomeAssignment, aid), "summary"))
            before = h.Interaction.query.count()
        client.post(f"/sace/home/assignments/{aid}/summary")
        client.post(f"/sace/home/assignments/{aid}/summary")
        with self.app.app_context():
            self.assertEqual(h.Interaction.query.count(), before)
            self.assertEqual(HomeEvidence.query.filter_by(item="summary", event="examined").count(), 1)
            self.assertEqual(len(h.endorsement.assignments(uid)), 1)

    def test_provisioning_email_binding_and_expiry_after_pledge(self):
        self.user("wrong@example.test")
        with self.app.app_context():
            token = s.issue_provisioning("intended@example.test", "local-test")
            db.session.commit()
        client = self.app.test_client()
        client.get("/sace/home/provisioning?token=" + token)
        client.post("/sace/home/provisioning", data={"signature": "Test", "accept": "yes"})
        self.login(client, "wrong@example.test", "/sace/home/provisioning")
        self.assertEqual(client.get("/sace/home/provisioning").status_code, 403)
        self.provision_home()
        code = self.code_home()
        client.post("/sace/home/join", data={"code": code})
        client.post("/sace/home/pledge", data={"signature": "A", "accept": "yes"})
        with self.app.app_context():
            invitation = HomeInvitation.query.filter_by(code_hash=s.digest(code)).one()
            invitation.expires_at = now() - timedelta(seconds=1)
            db.session.commit()
        self.assertEqual(client.get("/sace/home/claim").status_code, 400)
        with self.app.app_context():
            self.assertEqual(HomeAssignment.query.count(), 0)

    def test_manual_publication_requires_approved_source(self):
        self.provision_home()
        with self.app.app_context():
            with self.assertRaises(ValueError):
                s.publish_document(HomeController.query.one(), "participant_manual", "v1", "missing.pdf", {})
            self.assertEqual(HomeDocumentVersion.query.count(), 0)
            db.session.rollback()

    def test_real_layout_and_csrf(self):
        from jinja2 import FileSystemLoader
        loader = self.app.jinja_loader
        self.app.jinja_loader = FileSystemLoader(str(ROOT / "templates"))
        self.app.jinja_env.cache.clear()
        try:
            response = self.client.get("/sace/home/join")
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"<!DOCTYPE html>", response.data)
        finally:
            self.app.jinja_loader = loader
            self.app.jinja_env.cache.clear()
        self.app.config["WTF_CSRF_ENABLED"] = True
        try:
            self.assertEqual(self.client.post("/sace/home/join", data={"code": "HOME-TEST"}).status_code, 400)
        finally:
            self.app.config["WTF_CSRF_ENABLED"] = False

    def test_migration_roundtrip_is_additive_and_isolated(self):
        from dotenv import dotenv_values
        engine = create_engine(dotenv_values(ROOT / ".env")["DATABASE_URL"])
        spec = importlib.util.spec_from_file_location("home_migration", ROOT / "migrations/versions/home_sace_001_foundation.py")
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        schema = "pg_temp"
        try:
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(text('SET LOCAL search_path TO "' + schema + '"'))
                    conn.execute(text('CREATE TEMP TABLE "user" (id INTEGER PRIMARY KEY)'))
                    conn.execute(text('CREATE TEMP TABLE sace_workshop_interactions (id INTEGER PRIMARY KEY, response_data TEXT)'))
                    conn.execute(text("INSERT INTO sace_workshop_interactions VALUES (1, 'unchanged LITRE sentinel')"))
                    with Operations.context(MigrationContext.configure(conn)):
                        migration.upgrade()
                        names = conn.execute(text("SELECT relname FROM pg_class WHERE relnamespace = pg_my_temp_schema() AND relkind = 'r'")).scalars().all()
                        self.assertEqual(len([n for n in names if n.startswith("sace_home_")]), 8)
                        migration.downgrade()
                    self.assertEqual(sorted(conn.execute(text("SELECT relname FROM pg_class WHERE relnamespace = pg_my_temp_schema() AND relkind = 'r'")).scalars().all()), ["sace_workshop_interactions", "user"])
                    self.assertEqual(conn.execute(text('SELECT response_data FROM sace_workshop_interactions')).scalar(), 'unchanged LITRE sentinel')
                finally:
                    transaction.rollback()
        finally:
            engine.dispose()


if __name__ == "__main__":
    unittest.main(verbosity=2)
