"""Reading audit checks on verified localhost fixtures; no application startup."""
import importlib.util
from pathlib import Path
import unittest
from sqlalchemy import text, create_engine
from sqlalchemy.exc import DBAPIError
from alembic.operations import Operations
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("audit_home_support", ROOT / "tests/support/home_sace_postgres_runner.py")
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.extensions import db
from app.models.core import CoreAuditEvent
from app.models.sace_reading_audit import ReadingAuditEvent
from app.program_sace import lifecycle as reading
from app.program_sace_home import lifecycle as home


class ReadingAudit(f.HomeFoundation):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Enforce append-only writes during real HTTP journeys as well as the
        # separate migration round-trip, including provisioning's ORM flushes.
        with cls.app.app_context(), db.engine.begin() as conn:
            conn.execute(text("""CREATE FUNCTION pg_temp.reading_audit_fixture_reject() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'append-only'; END; $$"""))
            conn.execute(text("""CREATE TRIGGER reading_audit_fixture_append_only BEFORE UPDATE OR DELETE
                ON sace_reading_audit_event FOR EACH ROW
                EXECUTE FUNCTION pg_temp.reading_audit_fixture_reject()"""))

    def setUp(self):
        # TRUNCATE resets connection-local fixtures without mutating event rows.
        with self.app.app_context():
            db.session.execute(text("TRUNCATE pg_temp.sace_reading_audit_event"))
            db.session.commit()
        super().setUp()

    def core_snapshot(self):
        with self.app.app_context():
            return [dict(row) for row in db.session.execute(text("SELECT * FROM core_audit_event ORDER BY id")).mappings()]

    def sentinel(self):
        with self.app.app_context():
            db.session.add(CoreAuditEvent(action="PLEDGE_ACCEPTED", entity_type="SACE_PLEDGE", details="immutable legacy Reading"))
            db.session.commit()
        return self.core_snapshot()

    def test_audit_runtime_writers_preserve_legacy_core(self):
        before = self.sentinel()
        f.h.AccessJourneys.provision(self, self.client, "reading-r@example.test")
        with self.app.app_context():
            audit = ReadingAuditEvent.query.one()
            pledge = f.h.Interaction.query.filter_by(activity_slug="admin_patent_pledge").one()
            self.assertEqual(audit.entity_id, pledge.id)
            self.assertEqual(audit.created_at, pledge.timestamp)
            self.assertEqual(audit.engagement_id, reading.Appointment.query.one().engagement_id)
            self.assertEqual(audit.metadata_json["source_table"], "sace_workshop_interactions")
        self.assertEqual(self.client.get("/sace/provisioning/document/action/app1?action=view").status_code, 302)
        # Existing handler records the email action before delivery validation.
        self.assertEqual(self.client.post("/sace/provisioning/document/action/app1", data={"action": "email"}).status_code, 302)
        self.client.get("/logout")
        f.h.AccessJourneys.user(self, "telemetry@example.test")
        self.login(self.client, "telemetry@example.test")
        self.assertEqual(self.client.post("/sace/log_event", json={"action": "DEMO_CLICK", "details": "reported click"}).status_code, 200)
        f.h.AccessJourneys.provision(self, self.app.test_client(), "other-r@example.test")
        # Evaluator acknowledgement requires an actual Reading Auditor assignment.
        rclient = self.app.test_client()
        self.login(rclient, "reading-r@example.test", "/sace/provisioning")
        code, _ = f.h.AccessJourneys.code(self, rclient)
        auditor = self.app.test_client()
        self.assertEqual(auditor.post("/sace/join", data={"code": code}).location, "/sace/auditor_pledge")
        response = auditor.post("/sace/auditor_pledge")
        auditor.get(response.location)
        response = auditor.post("/register", data={"subject": "sace_endorsement", "next": "/sace/claim_code",
            "full_name": "Reading Auditor", "email": "reading-a@example.test", "password": "test-password"}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(auditor.post("/sace/acknowledge_patent").status_code, 302)
        with self.app.app_context():
            actions = [r.action for r in ReadingAuditEvent.query.all()]
            for action in ("DOCUMENT_ACCESSED", "DOCUMENT_EMAILED", "DEMO_CLICK"):
                self.assertIn(action, actions)
            self.assertEqual(actions.count("PLEDGE_ACCEPTED"), 3)
        self.assertEqual(self.core_snapshot(), before)

    def test_audit_home_stays_home_and_reading_audit_grants_nothing(self):
        before = self.sentinel()
        uid = self.user("audit-only@example.test")
        with self.app.app_context():
            db.session.add(ReadingAuditEvent(user_id=uid, action="controller_provisioned", engagement_id=999,
                details="Observational record only", metadata_json={"assignment_id": 999}))
            db.session.commit()
        self.login(self.client, "audit-only@example.test")
        self.assertEqual(self.client.get("/sace/reading").status_code, 403)
        self.assertEqual(self.client.get("/sace/home/control").status_code, 403)
        with self.app.app_context():
            self.assertEqual(reading.Appointment.query.count(), 0)
            self.assertEqual(reading.Engagement.query.count(), 0)
            self.assertEqual(f.h.Interaction.query.count(), 0)
        self.client.get("/logout")
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        auditor.post(f"/sace/home/assignments/{aid}/summary")
        with self.app.app_context():
            self.assertEqual(ReadingAuditEvent.query.count(), 1)
            self.assertGreater(home.HomeAuditEvent.query.count(), 0)
            self.assertEqual(home.HomeAuditEvent.query.filter_by(event="controller_provisioned").count(), 1)
            self.assertEqual(f.HomeEvidence.query.filter_by(item="summary", event="examined").count(), 1)
        self.assertEqual(self.core_snapshot(), before)

    def test_audit_additive_migration_and_immutability(self):
        spec = importlib.util.spec_from_file_location("reading_audit_migration", ROOT / "migrations/versions/reading_sace_audit_001.py")
        migration = importlib.util.module_from_spec(spec); spec.loader.exec_module(migration)
        self.assertIsNone(migration.down_revision)
        self.assertIsNone(migration.depends_on)
        # setUpClass has already verified localhost ait_local_db before any connection.
        with self.app.app_context():
            engine = create_engine(db.engine.url)
        try:
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(text('CREATE TEMP TABLE "user" (id INTEGER PRIMARY KEY)'))
                    conn.execute(text("SET LOCAL search_path TO pg_temp"))
                    conn.execute(text('INSERT INTO "user" VALUES (1)'))
                    conn.execute(text("CREATE TEMP TABLE core_audit_event (id INTEGER, details TEXT)"))
                    conn.execute(text("INSERT INTO core_audit_event VALUES (1, 'legacy')"))
                    with Operations.context(MigrationContext.configure(conn)):
                        migration.upgrade()
                        migration.downgrade()
                        migration.upgrade()
                        conn.execute(text("INSERT INTO sace_reading_audit_event (user_id,action,metadata_json,created_at) VALUES (1,'PLEDGE_ACCEPTED','{}',CURRENT_TIMESTAMP)"))
                        for statement in ("UPDATE sace_reading_audit_event SET action='tampered'", "DELETE FROM sace_reading_audit_event"):
                            with self.assertRaises(DBAPIError):
                                with conn.begin_nested(): conn.execute(text(statement))
                        with self.assertRaises(RuntimeError): migration.downgrade()
                    self.assertEqual(conn.execute(text("SELECT details FROM core_audit_event")).scalar_one(), "legacy")
                finally: transaction.rollback()
        finally: engine.dispose()

    def test_audit_graph_and_uip_source_unchanged_boundary(self):
        self.assertEqual(set(ScriptDirectory(str(ROOT / "migrations")).get_heads()),
            {"reading_sace_002", "home_sace_002", "uip_p57", "reading_sace_audit_001"})
        for path in list((ROOT / "app/program_uip").rglob("*.py")) + [ROOT / "app/models/uip.py"]:
            source = path.read_text(encoding="utf-8-sig")
            self.assertNotIn("sace_reading_audit", source)
            self.assertNotIn("CoreAuditEvent", source)
        for path in (ROOT / "app/program_sace").rglob("*.py"):
            self.assertNotIn("CoreAuditEvent", path.read_text(encoding="utf-8-sig"))


if __name__ == "__main__":
    names = [n for n in unittest.defaultTestLoader.getTestCaseNames(ReadingAudit) if n.startswith("test_audit_")]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(ReadingAudit(n) for n in names))
    raise SystemExit(not result.wasSuccessful())
