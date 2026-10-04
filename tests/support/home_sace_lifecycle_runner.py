"""HOME Phase 2A checks: verified local PostgreSQL fixtures, no app factory."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
from sqlalchemy import text, create_engine
from flask_login import login_user
from alembic.operations import Operations
from alembic.migration import MigrationContext

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("home_foundation_support", ROOT / "tests/support/home_sace_postgres_runner.py")
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.program_sace_home import lifecycle as lc, service as s
from app.extensions import db
from app.models.sace_home import HomeController, HomeInvitation, HomeAssignment, HomePledge, HomeEvidence, HomeProvisioning

COMPLETE = "/sace/home/control/completion"
CANCEL = "/sace/home/control/cancel-completion"


class HomeLifecycle(f.HomeFoundation):
    def pending(self):
        self.assertEqual(self.client.post(COMPLETE, data={"decision": "yes"}).status_code, 302)
        with self.app.app_context():
            row = lc.Engagement.query.one()
            return row.id, row.completion_deadline

    def test_phase2_provisioning_creates_coherent_home_authority_and_audit(self):
        uid = self.provision_home()
        with self.app.app_context():
            owner, actor, engagement = HomeController.query.one(), lc.Appointment.query.one(), lc.Engagement.query.one()
            grant = f.h.auth_models.AuthSubjectAdmin.query.one()
            subject = db.session.get(f.h.auth_models.AuthSubject, grant.subject_id)
            pledge = HomePledge.query.one()
            self.assertEqual(subject.slug, s.SUBJECT)
            self.assertEqual((actor.controller_id, actor.engagement_id, actor.operational_grant_id), (owner.id, engagement.id, grant.id))
            self.assertEqual((owner.user_id, engagement.created_by_user_id, pledge.user_id), (uid, uid, uid))
            self.assertEqual(actor.pledge_id, pledge.id)
            self.assertEqual(HomeProvisioning.query.one().claimed_by, uid)
            event = lc.HomeAuditEvent.query.filter_by(event="controller_provisioned").one()
            self.assertLessEqual(pledge.accepted_at, event.created_at)
            self.assertEqual(event.details["grant_id"], grant.id)
            self.assertEqual(f.h.Interaction.query.count(), 0)
            self.assertEqual(f.h.auth_models.UserEnrollment.query.count(), 0)

    def test_phase2_home_r_a_authority_requires_provisioning_assignment_without_payment(self):
        # Exercise the repaired policy through registration and protected HTTP access.
        with self.app.app_context():
            subject = f.h.auth_models.AuthSubject.query.filter_by(slug=s.SUBJECT).one()
            subject.enroll_policy = "post_payment"
            subject.commercial_mode = "free"
            subject.requires_price = 0
            subject.allow_country_pricing = 0
            db.session.commit()
        uid = self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        self.assertEqual(self.client.get("/sace/home/control").status_code, 200)
        self.assertEqual(auditor.get(f"/sace/home/assignments/{aid}/board").status_code, 200)
        with self.app.app_context():
            self.assertEqual(lc.Appointment.query.one().provisioning_id, HomeProvisioning.query.one().id)
            self.assertEqual(HomeProvisioning.query.one().claimed_by, uid)
            self.assertEqual(db.session.get(HomeAssignment, aid).invitation_id, HomeInvitation.query.one().id)
            self.assertEqual(f.h.auth_models.UserEnrollment.query.count(), 0)
            # No payment tables exist in this harness, so these journeys cannot
            # rely on payment records or checkout to grant either role authority.
            db.session.get(HomeAssignment, aid).status = "revoked"
            db.session.commit()
        self.assertEqual(auditor.get(f"/sace/home/assignments/{aid}/board").status_code, 403)
        with self.app.app_context():
            actor = lc.Appointment.query.one()
            actor.status, actor.ended_at = "revoked", lc.now()
            db.session.commit()
        self.assertEqual(self.client.get("/sace/home/control").status_code, 403)

    def test_phase2_returning_r_and_a_ordinary_login_and_access_audit(self):
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "home-r@example.test").location, "/sace/home/control")
        self.assertEqual(self.client.get("/sace/home/control").status_code, 200)
        auditor.get("/logout")
        result = self.login(auditor, "home-a@example.test")
        self.assertEqual(result.location, "/sace/home/")
        self.assertEqual(auditor.get(result.location).location, f"/sace/home/assignments/{aid}/board")
        with self.app.app_context():
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 1)
            for role in ("controller", "auditor"):
                self.assertGreater(lc.HomeAuditEvent.query.filter_by(role=role, event="authenticated").count(), 0)
                self.assertGreater(lc.HomeAuditEvent.query.filter_by(role=role, event="access").count(), 0)
            self.assertEqual(HomeInvitation.query.one().appointment_id, lc.Appointment.query.one().id)

    def test_phase2_no_and_confirmation_yes_exact_deadline(self):
        uid = self.provision_home()
        page = self.client.get(COMPLETE)
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"Are you sure the endorsement process for this activity is complete?", page.data)
        self.assertEqual(self.client.post(COMPLETE, data={"decision": "no"}).status_code, 302)
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.one().status, "active")
            self.assertEqual(lc.HomeAuditEvent.query.filter_by(event="completion_requested").count(), 0)
        instant = datetime.now(timezone.utc)
        with patch.object(lc, "now", return_value=instant):
            eid, deadline = self.pending()
        self.assertEqual(deadline, instant + timedelta(hours=48))
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.one().completion_requested_by_user_id, uid)
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 1)
            self.assertEqual(lc.HomeAuditEvent.query.filter_by(event="completion_requested").count(), 1)
        self.assertEqual(self.client.post(COMPLETE, data={"decision": "yes"}).status_code, 409)

    def test_phase2_access_before_deadline_and_cancel_preserves_history(self):
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        eid, deadline = self.pending()
        with patch.object(lc, "now", return_value=deadline - timedelta(microseconds=1)):
            self.assertEqual(self.client.get("/sace/home/control").status_code, 200)
            self.assertEqual(auditor.get(f"/sace/home/assignments/{aid}/board").status_code, 200)
            self.assertEqual(auditor.post(CANCEL).status_code, 403)
            self.assertEqual(self.client.post(CANCEL).status_code, 302)
        with self.app.app_context():
            row = lc.Engagement.query.one()
            self.assertEqual(row.status, "active")
            self.assertIsNone(row.completion_deadline)
            self.assertIsNone(row.completion_requested_by_user_id)
            self.assertEqual(lc.HomeAuditEvent.query.filter_by(event="completion_requested").count(), 1)
            event = lc.HomeAuditEvent.query.filter_by(event="completion_cancelled").one()
            self.assertEqual(event.details["deadline"], deadline.isoformat())
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 1)
        self.pending()
        with self.app.app_context():
            self.assertEqual(lc.HomeAuditEvent.query.filter_by(event="completion_requested").count(), 2)

    def test_phase2_deadline_denies_both_roles_without_finalizer_and_no_cancellation(self):
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        unclaimed = self.code_home()
        eid, deadline = self.pending()
        for instant in (deadline, deadline + timedelta(seconds=1)):
            with patch.object(lc, "now", return_value=instant):
                self.assertEqual(self.client.get("/sace/home/control").status_code, 403)
                self.assertEqual(self.client.post(CANCEL).status_code, 403)
                self.assertEqual(auditor.get(f"/sace/home/assignments/{aid}/board").status_code, 403)
                self.assertEqual(auditor.post(f"/sace/home/assignments/{aid}/summary").status_code, 403)
                self.assertEqual(self.app.test_client().post("/sace/home/join", data={"code": unclaimed}).status_code, 403)
                self.assertEqual(self.client.get("/sace/home/ip-pledge").status_code, 403)
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.one().status, "completion_pending")
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 1)

    def test_phase2_finalization_exact_grant_idempotent_history_and_reendorsement(self):
        uid = self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        auditor.post(f"/sace/home/assignments/{aid}/summary")
        with self.app.app_context():
            old_grant = lc.Appointment.query.one().operational_grant_id
            db.session.add(f.h.auth_models.AuthSubjectAdmin(subject_id=44, email="home-r@example.test"))
            db.session.commit()
            evidence = [(r.id, r.details) for r in HomeEvidence.query.order_by(HomeEvidence.id)]
        eid, deadline = self.pending()
        with self.app.app_context(), patch.object(lc, "now", return_value=deadline):
            self.assertEqual(lc.finalize_due_completions(), [eid]); db.session.commit()
            self.assertEqual(lc.finalize_due_completions(), []); db.session.commit()
            self.assertIsNone(db.session.get(f.h.auth_models.AuthSubjectAdmin, old_grant))
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.one().subject_id, 44)
            self.assertEqual(lc.Appointment.query.one().grant_id_at_issue, old_grant)
            self.assertEqual(lc.Appointment.query.one().status, "completed")
            self.assertEqual([(r.id, r.details) for r in HomeEvidence.query.order_by(HomeEvidence.id)], evidence)
            self.assertEqual(HomePledge.query.count(), 2)
            self.assertEqual(HomeAssignment.query.one().status, "active")
            self.assertEqual(HomeInvitation.query.one().status, "claimed")
            token = s.issue_provisioning("home-r@example.test", "new-reviewed-engagement")
            db.session.commit()
        self.assertEqual(self.client.get("/sace/home/control").status_code, 403)
        self.client.get("/sace/home/provisioning?token=" + token)
        self.assertEqual(self.client.post("/sace/home/provisioning", data={"signature": "HOME R", "accept": "yes"}).status_code, 302)
        with self.app.app_context():
            self.assertEqual(HomeController.query.count(), 1)
            self.assertEqual(lc.Engagement.query.count(), 2)
            self.assertEqual(lc.Engagement.query.filter_by(status="active").count(), 1)
            self.assertEqual(lc.Engagement.query.filter_by(status="completed").count(), 1)
            self.assertEqual(HomeController.query.one().user_id, uid)
        self.assertEqual(auditor.get(f"/sace/home/assignments/{aid}/board").status_code, 403)
        self.assertEqual(self.client.get(f"/sace/home/control/assignments/{aid}").status_code, 404)

    def test_phase2_auditor_completion_never_completes_engagement(self):
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        self.assertEqual(auditor.post(COMPLETE, data={"decision": "yes"}).status_code, 403)
        with patch.object(s, "missing", return_value=[]):
            self.assertEqual(auditor.post(f"/sace/home/assignments/{aid}/completion").status_code, 200)
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.one().status, "active")
            self.assertEqual(HomeAssignment.query.one().status, "completed")
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 1)
        self.assertEqual(auditor.get(f"/sace/home/assignments/{aid}/board").status_code, 403)
        self.assertEqual(self.client.get("/sace/home/control").status_code, 200)

    def test_phase2_no_identity_grant_enrollment_session_or_platform_fallback(self):
        uid = self.user("orphan@example.test")
        with self.app.app_context():
            db.session.add(HomeController(user_id=uid))
            db.session.add(f.h.auth_models.AuthSubjectAdmin(subject_id=901, email="orphan@example.test"))
            db.session.add(f.h.auth_models.UserEnrollment(subject_id=901, user_id=uid, status="active"))
            db.session.execute(text("INSERT INTO auth_approved_admin (email, active) VALUES ('orphan@example.test', 1)"))
            db.session.commit()
        self.assertNotEqual(self.login(self.client, "orphan@example.test").location, "/sace/home/control")
        with self.client.session_transaction() as state:
            state["is_admin"] = True
            state["admin_subjects"] = [s.SUBJECT]
        self.assertEqual(self.client.get("/sace/home/control").status_code, 403)
        self.assertEqual(self.client.get("/sace/home/provisioning").status_code, 302)
        with self.client.session_transaction() as state:
            nonce = state[s.PROVISIONING_CONTEXT]['nonce']
        self.assertEqual(self.client.post('/sace/home/provisioning', data={
            'journey': nonce, 'signature': 'Orphan', 'accept': 'yes'}).status_code, 409)
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.count(), 0)
            self.assertEqual(lc.Appointment.query.count(), 0)

    def test_phase2_exact_grant_and_parent_required(self):
        self.provision_home()
        for column, changed in (("email", "other@example.test"), ("subject_id", 44)):
            with self.app.app_context():
                grant = f.h.auth_models.AuthSubjectAdmin.query.one()
                original = getattr(grant, column)
                setattr(grant, column, changed); db.session.commit()
            self.assertEqual(self.client.get("/sace/home/control").status_code, 403)
            with self.app.app_context():
                setattr(f.h.auth_models.AuthSubjectAdmin.query.one(), column, original); db.session.commit()
        with self.app.app_context():
            row = lc.Engagement.query.one(); row.status = "revoked"; row.revoked_at = lc.now(); db.session.commit()
        self.assertEqual(self.client.get("/sace/home/control").status_code, 403)

    def test_phase2_failed_provisioning_rolls_back_all_authority_and_pledge(self):
        self.user("new@example.test")
        self.login(self.client, "new@example.test", "/sace/home/")
        with self.app.app_context():
            token = s.issue_provisioning("new@example.test", "test")
            db.session.commit()
        self.client.get("/sace/home/provisioning?token=" + token)
        with patch.object(lc, "audit", side_effect=RuntimeError("forced local failure")):
            with self.assertRaises(RuntimeError):
                self.client.post("/sace/home/provisioning", data={"signature": "New R", "accept": "yes"})
        with self.app.app_context():
            self.assertEqual(HomeController.query.count(), 0)
            self.assertEqual(lc.Engagement.query.count(), 0)
            self.assertEqual(lc.Appointment.query.count(), 0)
            self.assertEqual(HomePledge.query.count(), 0)
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 0)
            self.assertIsNone(HomeProvisioning.query.one().claimed_at)

    def test_phase2_request_and_finalizer_rollback(self):
        self.provision_home()
        with patch.object(lc, "audit", side_effect=RuntimeError("forced local failure")):
            with self.assertRaises(RuntimeError):
                self.client.post(COMPLETE, data={"decision": "yes"})
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.one().status, "active")
        eid, deadline = self.pending()
        with self.app.app_context(), patch.object(lc, "now", return_value=deadline):
            original = lc.audit
            def fail_after_retirement(*args, **kwargs):
                if args[2] == "engagement_completed":
                    raise RuntimeError("forced finalization rollback")
                return original(*args, **kwargs)
            with patch.object(lc, "audit", side_effect=fail_after_retirement):
                with self.assertRaises(RuntimeError):
                    lc.finalize_due_completions()
            db.session.rollback()
            self.assertEqual(lc.Engagement.query.one().status, "completion_pending")
            self.assertEqual(lc.Appointment.query.one().status, "active")
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 1)
            self.assertEqual(lc.HomeAuditEvent.query.filter_by(event="grant_retired").count(), 0)

    def test_phase2_dual_authority_requires_explicit_destination(self):
        self.provision_home()
        self.client.get("/logout")
        f.h.AccessJourneys.provision(self, self.client, "home-r@example.test", existing=True)
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "home-r@example.test").status_code, 409)
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "home-r@example.test", "/sace/home/control").location, "/sace/home/control")
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "home-r@example.test", "/sace/dashboard").location, "/sace/provisioning")

    pledge = f.h.AccessJourneys.pledge

    def test_phase2_dual_authority_stale_reading_code_cannot_choose_activity(self):
        self.provision_home()
        self.client.get("/logout")
        f.h.AccessJourneys.provision(self, self.client, "home-r@example.test", existing=True)
        self.client.get("/logout")
        with self.client.session_transaction() as state:
            state["pending_sace_code"] = "STALE-READING-CODE"
            state["sace_evaluator_pledged"] = True
        self.assertEqual(self.login(self.client, "home-r@example.test").status_code, 409)

    def test_phase2_auditor_requires_exact_grant_and_active_appointment(self):
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        board = f"/sace/home/assignments/{aid}/board"
        for column, changed in (("email", "other@example.test"), ("subject_id", 44)):
            with self.app.app_context():
                grant = f.h.auth_models.AuthSubjectAdmin.query.one()
                original = getattr(grant, column)
                setattr(grant, column, changed)
                db.session.commit()
            self.assertEqual(auditor.get(board).status_code, 403)
            with self.app.app_context():
                setattr(f.h.auth_models.AuthSubjectAdmin.query.one(), column, original)
                db.session.commit()
            self.assertEqual(auditor.get(board).status_code, 200)
        with self.app.app_context():
            actor = lc.Appointment.query.one()
            actor.status, actor.ended_at = "revoked", lc.now()
            db.session.commit()
        self.assertEqual(auditor.get(board).status_code, 403)
        auditor.get("/logout")
        self.assertNotEqual(self.login(auditor, "home-a@example.test").location, "/sace/home/")

    def test_phase2_consumed_provisioning_cannot_replay(self):
        self.provision_home()
        with self.app.app_context():
            self.assertEqual(HomeProvisioning.query.count(), 1)
        self.client.get("/logout")
        self.user("other@example.test")
        self.login(self.client, "other@example.test")
        self.assertEqual(self.client.get("/sace/home/provisioning").status_code, 302)
        # A fresh GET may start a new journey, but creates no new authority.
        with self.app.app_context():
            self.assertEqual(lc.Appointment.query.count(), 1)

    def test_phase2_concurrent_finalizers(self, operation=False):
        import threading, uuid
        from flask import Flask
        from sqlalchemy import event as sql_event
        from werkzeug.exceptions import HTTPException
        uid = self.provision_home()
        with patch.object(lc, "now", return_value=datetime(2020, 1, 1, tzinfo=timezone.utc)):
            eid, deadline = self.pending()
        prefix = "hlc_" + uuid.uuid4().hex[:12] + "_"
        with self.app.app_context():
            url = db.engine.url
            with db.engine.begin() as conn:
                for table in self.tables:
                    conn.execute(text('CREATE TABLE public."' + prefix + table + '" (LIKE pg_temp."' + table + '" INCLUDING ALL)'))
                    if table != "sace_reading_assignment_context":
                        conn.execute(text('ALTER TABLE public."' + prefix + table + '" ALTER COLUMN id DROP IDENTITY IF EXISTS'))
                        conn.execute(text('ALTER TABLE public."' + prefix + table + '" ALTER COLUMN id DROP DEFAULT'))
                        conn.execute(text('ALTER TABLE public."' + prefix + table + '" ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY'))
                    conn.execute(text('INSERT INTO public."' + prefix + table + '" SELECT * FROM pg_temp."' + table + '"'))
                    if table != "sace_reading_assignment_context":
                        conn.execute(text("SELECT setval(pg_get_serial_sequence(:table,'id'), COALESCE((SELECT max(id) FROM public.\"" + prefix + table + "\"),0)+1,false)"), {"table": "public." + prefix + table})
        app = Flask("home-concurrency"); app.secret_key = "local-only"
        app.config.update(SQLALCHEMY_DATABASE_URI=url, SQLALCHEMY_ENGINE_OPTIONS={"connect_args": {"options": "-csearch_path=pg_temp -clock_timeout=5000"}})
        db.init_app(app); f.h.login_manager.init_app(app)
        with app.app_context():
            @sql_event.listens_for(db.engine, "connect")
            def fixture_views(connection, record):
                cursor = connection.cursor()
                for table in self.tables:
                    cursor.execute('CREATE TEMP VIEW "' + table + '" AS SELECT * FROM public."' + prefix + table + '"')
                connection.commit(); cursor.close()
        locked, release, attempted, finished = [threading.Event() for _ in range(4)]
        outcomes, errors = [], []
        def first():
            try:
                with app.test_request_context("/"):
                    login_user(db.session.get(f.h.auth_models.User, uid)); lc.subject_lock(); locked.set()
                    if not release.wait(5): raise RuntimeError("test timeout")
                    outcomes.append(lc.finalize_due_completions()); db.session.commit()
            except Exception as exc:
                errors.append(exc); locked.set()
        def second():
            try:
                with app.test_request_context("/"):
                    login_user(db.session.get(f.h.auth_models.User, uid)); attempted.set()
                    try:
                        if operation:
                            lc.require_appointment(); outcomes.append("unexpected authority")
                        else:
                            outcomes.append(lc.finalize_due_completions()); db.session.commit()
                    except HTTPException as exc:
                        outcomes.append(exc.code)
                    finally:
                        db.session.rollback()
            except Exception as exc: errors.append(exc)
            finally: finished.set()
        threads = [threading.Thread(target=first), threading.Thread(target=second)]
        try:
            threads[0].start(); self.assertTrue(locked.wait(3)); threads[1].start(); self.assertTrue(attempted.wait(3))
            self.assertFalse(finished.wait(.2)); release.set()
            for thread in threads: thread.join(7)
            self.assertFalse(any(t.is_alive() for t in threads)); self.assertEqual(errors, [])
            self.assertEqual(outcomes, [[eid], 403] if operation else [[eid], []])
        finally:
            release.set()
            for thread in threads:
                if thread.ident: thread.join(7)
            with app.app_context(): db.session.remove(); db.engine.dispose()
            engine = create_engine(url)
            try:
                with engine.begin() as conn:
                    for table in reversed(self.tables): conn.execute(text('DROP TABLE public."' + prefix + table + '"'))
            finally: engine.dispose()

    def test_phase2_concurrent_finalizer_denies_late_authority(self):
        self.test_phase2_concurrent_finalizers(operation=True)

    def test_phase2_migration_subject_seed_existing_slug_and_rollback(self):
        from dotenv import dotenv_values
        engine = create_engine(dotenv_values(ROOT / ".env")["DATABASE_URL"])
        def load(name, file):
            spec = importlib.util.spec_from_file_location(name, ROOT / "migrations/versions" / file)
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            return module
        foundation = load("home001", "home_sace_001_foundation.py")
        migration = load("home002", "home_sace_002_lifecycle.py")
        self.assertEqual(migration.down_revision, "home_sace_001")
        try:
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(text("SET LOCAL search_path TO pg_temp"))
                    conn.execute(text('CREATE TEMP TABLE "user" (id INTEGER PRIMARY KEY)'))
                    conn.execute(text("""CREATE TEMP TABLE auth_subject (
                        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        slug TEXT UNIQUE NOT NULL, name TEXT NOT NULL, is_active INTEGER NOT NULL,
                        trial_days FLOAT, commercial_mode TEXT, billing_scope TEXT, enroll_policy TEXT,
                        processor_default TEXT, requires_price INTEGER, allow_country_pricing INTEGER,
                        mor_mode INTEGER, program_type TEXT, is_hidden_on_bridge BOOLEAN,
                        show_on_welcome BOOLEAN, start_endpoint TEXT, admin_start_endpoint TEXT,
                        CONSTRAINT ck_auth_subject_enroll_policy
                        CHECK (enroll_policy IN ('auto_enroll', 'post_payment')))"""))
                    conn.execute(text("CREATE TEMP TABLE auth_subject_admin (id INTEGER PRIMARY KEY, subject_id INTEGER REFERENCES auth_subject(id), email TEXT)"))
                    conn.execute(text("INSERT INTO auth_subject (slug,name,is_active) VALUES ('home','Ordinary HOME',1),('sace_endorsement','Reading sentinel',1)"))
                    with Operations.context(MigrationContext.configure(conn)):
                        foundation.upgrade()
                        migration.upgrade()
                        subject = conn.execute(text("SELECT * FROM auth_subject WHERE slug='sace_home_endorsement'")).mappings().one()
                        self.assertEqual((subject["is_active"], subject["processor_default"], subject["enroll_policy"]), (1, "paystack", "post_payment"))
                        self.assertEqual(
                            (subject["commercial_mode"], subject["requires_price"], subject["allow_country_pricing"]),
                            ("free", 0, 0))
                        self.assertTrue(subject["is_hidden_on_bridge"])
                        names = conn.execute(text("SELECT relname FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relkind='r'")).scalars().all()
                        self.assertEqual(len([n for n in names if n.startswith("sace_home_")]), 11)
                        self.assertEqual(conn.execute(text("SELECT count(*) FROM sace_home_controller_appointment")).scalar(), 0)
                        migration.ensure_subject()
                        self.assertEqual(conn.execute(text("SELECT count(*) FROM auth_subject WHERE slug='sace_home_endorsement'")).scalar(), 1)
                        conn.execute(text("UPDATE auth_subject SET name='Existing approved name',is_active=0 WHERE slug='sace_home_endorsement'"))
                        migration.ensure_subject()
                        retained = conn.execute(text("SELECT name,is_active FROM auth_subject WHERE slug='sace_home_endorsement'")).one()
                        self.assertEqual(tuple(retained), ("Existing approved name", 0))
                        conn.execute(text('INSERT INTO "user" VALUES (1)'))
                        conn.execute(text("INSERT INTO sace_home_engagement (reference,status,started_at,created_by_user_id) VALUES ('history','active',CURRENT_TIMESTAMP,1)"))
                        with self.assertRaises(RuntimeError): migration.downgrade()
                        conn.execute(text("DELETE FROM sace_home_engagement"))
                        migration.downgrade()
                        self.assertEqual(conn.execute(text("SELECT count(*) FROM auth_subject")).scalar(), 3)
                        self.assertEqual(conn.execute(text("SELECT name FROM auth_subject WHERE slug='sace_endorsement'")).scalar(), "Reading sentinel")
                        foundation.downgrade()
                    self.assertIsNone(conn.execute(text("SELECT to_regclass('sace_home_engagement')")).scalar())
                finally: transaction.rollback()
        finally: engine.dispose()

    def test_phase2_inactive_subject_blocks_provisioning_and_access(self):
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        with self.app.app_context():
            db.session.query(f.h.auth_models.AuthSubject).filter_by(slug=s.SUBJECT).update({"is_active": 0})
            db.session.commit()
        self.assertEqual(self.client.get("/sace/home/control").status_code, 503)
        self.assertEqual(auditor.get(f"/sace/home/assignments/{aid}/board").status_code, 403)



if __name__ == "__main__":
    names = [n for n in unittest.defaultTestLoader.getTestCaseNames(HomeLifecycle) if n.startswith("test_phase2_")]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(HomeLifecycle(n) for n in names))
    raise SystemExit(not result.wasSuccessful())
