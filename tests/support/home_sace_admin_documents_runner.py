"""Focused admin uploads against connection-local PostgreSQL fixtures and fake R2."""
import importlib.util
import io
from pathlib import Path
import re
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from flask_login import login_user
from pypdf import PdfWriter
from sqlalchemy import text, create_engine
from alembic.operations import Operations
from alembic.migration import MigrationContext

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('home_admin_support', ROOT / 'tests/support/home_sace_postgres_runner.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.extensions import db
from app.program_sace_home import admin_documents as pub, service as s, examination as ex
from app.program_sace_home import document_storage as storage
from app.models.sace_home import HomeDocumentVersion, HomeDocument, HomeEvidence, HomeAssignment, HomeController, HomeInvitation, now
from app.admin.programs.home import routes
from app.admin.programs import routes as program_routes
from flask_wtf.csrf import generate_csrf

URL = '/admin/home/endorsement-documents'


def pdf_bytes(width=100):
    writer = PdfWriter()
    writer.add_blank_page(width=width, height=100)
    stream = io.BytesIO()
    writer.write(stream)
    return stream.getvalue()


class AdminDocuments(f.HomeFoundation):
    def setUp(self):
        super().setUp()
        self.app.config['SACE_HOME_R2_BUCKET'] = 'test-home-private'
        self.objects = {}
        def upload(key, content, *, bucket):
            self.assertEqual(bucket, 'test-home-private')
            self.objects[key] = content
            return key
        def read(key, *, bucket):
            self.assertEqual(bucket, 'test-home-private')
            return self.objects[key]
        self.upload_patch = patch.object(storage.r2, 'upload_bytes_to_r2', side_effect=upload)
        self.read_patch = patch.object(storage.r2, 'read_file_from_r2', side_effect=read)
        self.upload_mock = self.upload_patch.start()
        self.read_mock = self.read_patch.start()
        self.addCleanup(self.upload_patch.stop)
        self.addCleanup(self.read_patch.stop)
        self.uid = self.user('platform-admin@example.test')
        with self.app.app_context():
            db.session.add(f.h.auth_models.AuthSubject(id=902, slug='home', name='HOME', is_active=1))
            db.session.execute(text("INSERT INTO auth_approved_admin (email, active) VALUES ('platform-admin@example.test', 1)"))
            db.session.commit()
        self.signin(self.uid)

    def signin(self, uid):
        with self.client.session_transaction() as state:
            state['_user_id'] = str(uid)
            state['_fresh'] = True
            state['is_admin'] = True
            state['role'] = 'admin'

    def post_pdf(self, kind='timetable', content=None, **extra):
        return self.client.post(URL, data=dict(kind=kind,
            pdf=(io.BytesIO(content if content is not None else pdf_bytes()), 'approved.pdf'), **extra))

    def test_admin_all_five_upload_replace_history_and_provenance(self):
        for kind in pub.KINDS:
            self.assertEqual(self.post_pdf(kind).status_code, 302)
            self.assertEqual(self.post_pdf(kind, pdf_bytes(200)).status_code, 302)
        with self.app.app_context():
            self.assertEqual(HomeDocument.query.count(), 5)
            self.assertEqual(HomeDocumentVersion.query.count(), 10)
            self.assertEqual(f.HomeController.query.count(), 0)
            for row in HomeDocumentVersion.query.all():
                self.assertIsNone(row.approved_by)
                self.assertEqual(row.approved_by_admin_user_id, self.uid)
                self.assertEqual(row.source_manifest['home_approval']['reference'], 'admin-approved uploaded PDF')
                self.assertNotIn('source_approval', row.source_manifest)
                self.assertNotIn('production_parity_checked', str(row.source_manifest))
                self.assertTrue(ex.valid_document(row))
                self.assertEqual(s.document_content(row), self.objects[row.storage_key])
        manage = self.client.get('/admin/home/')
        self.assertEqual(manage.status_code, 200)
        self.assertNotIn(URL.encode(), manage.data)
        page = self.client.get(URL)
        self.assertEqual(page.status_code, 200)
        self.assertEqual(page.headers['Cache-Control'], 'private, no-store')
        self.assertEqual(page.data.count(b'>Replace</button>'), 5)
        self.assertNotIn(b'endorsement/admin/', page.data)

    def test_admin_authority_rejects_flags_subject_grants_inactive_and_revoked(self):
        other = self.user('subject-admin@example.test')
        with self.app.app_context():
            db.session.add(f.h.auth_models.AuthSubjectAdmin(email='subject-admin@example.test', subject_id=901))
            db.session.commit()
        self.signin(other)
        self.assertEqual(self.client.get(URL).status_code, 403)
        self.assertEqual(self.post_pdf().status_code, 403)
        self.signin(self.uid)
        with self.app.app_context():
            db.session.execute(text('UPDATE auth_approved_admin SET active=0'))
            db.session.commit()
        self.assertEqual(self.post_pdf().status_code, 403)
        with self.app.app_context():
            db.session.execute(text('UPDATE auth_approved_admin SET active=1'))
            db.session.get(f.h.auth_models.User, self.uid).is_active = 0
            db.session.commit()
        self.assertEqual(self.post_pdf().status_code, 403)
        with self.client.session_transaction() as state:
            state.pop('_user_id', None)
        self.assertEqual(self.post_pdf().status_code, 403)
        self.assertEqual(self.upload_mock.call_count, 0)

    def test_admin_storage_failure_missing_bucket_and_public_bucket_fail_closed(self):
        with patch.object(storage.r2, 'upload_bytes_to_r2', side_effect=OSError('offline')):
            self.assertEqual(self.post_pdf().status_code, 503)
        self.app.config['SACE_HOME_R2_BUCKET'] = ''
        with patch.dict('os.environ', {'SACE_HOME_R2_BUCKET': ''}):
            self.assertEqual(self.post_pdf().status_code, 503)
        self.app.config['SACE_HOME_R2_BUCKET'] = 'public-reading'
        with patch.dict('os.environ', {'R2_BUCKET_NAME': 'public-reading'}):
            self.assertEqual(self.post_pdf().status_code, 400)
        with self.app.app_context():
            self.assertEqual(HomeDocumentVersion.query.count(), 0)
            self.assertEqual(HomeDocument.query.count(), 0)
        self.app.config['SACE_HOME_R2_BUCKET'] = 'test-home-private'
        self.assertEqual(self.post_pdf().status_code, 302)

    def test_admin_invalid_pdf_kind_size_and_csrf(self):
        self.assertEqual(self.post_pdf(content=b'%PDF-not valid').status_code, 400)
        self.assertEqual(self.post_pdf(content=b'').status_code, 400)
        self.assertEqual(self.post_pdf(kind='certificate').status_code, 400)
        with patch.object(pub, 'MAX_BYTES', 10):
            self.assertEqual(self.post_pdf().status_code, 400)
        self.app.config['WTF_CSRF_ENABLED'] = True
        original = self.app.jinja_env.globals['csrf_token']
        self.app.jinja_env.globals['csrf_token'] = generate_csrf
        try:
            self.assertEqual(self.post_pdf().status_code, 400)
            page = self.client.get(URL)
            token = re.search(rb'name="csrf_token" value="([^"]+)"', page.data)[1].decode()
            self.assertEqual(self.post_pdf(csrf_token=token).status_code, 302)
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.app.jinja_env.globals['csrf_token'] = original

    def test_admin_replacement_preserves_existing_assignment_binding(self):
        self.assertEqual(self.post_pdf().status_code, 302)
        with self.app.app_context():
            first_id = s.latest_version('timetable').id
        with self.app.app_context():
            owner = HomeController(user_id=self.uid)
            db.session.add(owner)
            db.session.flush()
            invitation = HomeInvitation(controller_id=owner.id, code_hash='a' * 64,
                expires_at=now(), status='claimed', claimed_at=now())
            db.session.add(invitation)
            db.session.flush()
            assignment = HomeAssignment(invitation_id=invitation.id, auditor_id=self.uid)
            db.session.add(assignment)
            db.session.commit()
            aid = assignment.id
        with self.app.test_request_context():
            assignment = db.session.get(HomeAssignment, aid)
            login_user(db.session.get(f.h.auth_models.User, assignment.auditor_id))
            self.assertEqual(ex.document(assignment, 'timetable', bind=True).id, first_id)
            db.session.commit()
        self.signin(self.uid)
        self.assertEqual(self.post_pdf(content=pdf_bytes(250)).status_code, 302)
        with self.app.app_context():
            self.assertNotEqual(s.latest_version('timetable').id, first_id)
            self.assertEqual(ex.document(db.session.get(HomeAssignment, aid), 'timetable').id, first_id)
            self.assertEqual(HomeEvidence.query.filter_by(assignment_id=aid, item='timetable', event='document_bound').count(), 1)
            invitation = HomeInvitation(controller_id=HomeController.query.one().id,
                code_hash='b' * 64, expires_at=now())
            db.session.add(invitation)
            db.session.flush()
            fresh = HomeAssignment(invitation_id=invitation.id, auditor_id=self.uid)
            db.session.add(fresh)
            db.session.flush()
            self.assertEqual(ex.document(fresh, 'timetable').id, s.latest_version('timetable').id)
            db.session.commit()

    def test_admin_controller_publication_contract_and_manual_gate_unchanged(self):
        with self.app.app_context():
            owner = HomeController(user_id=self.uid)
            db.session.add(owner)
            db.session.flush()
            content = pdf_bytes()
            manifest = {'subject': s.SUBJECT, 'kind': 'application_form_1',
                'home_approval': {'approved_by': 'controller source approval', 'reference': 'test'}}
            with patch.object(s.lc, 'appointment', return_value=SimpleNamespace(controller_id=owner.id)), \
                    patch.object(storage.r2, 'upload_bytes_to_r2', side_effect=OSError('offline')):
                row = s.publish_document(owner, 'application_form_1', 'legacy-controller-test',
                    'controller/test.pdf', manifest, content=content)
                db.session.commit()
            self.assertEqual(row.approved_by, owner.id)
            self.assertIsNone(row.approved_by_admin_user_id)
            self.assertEqual(storage.disk_path(row.storage_key).read_bytes(), content)
            with self.assertRaises(ValueError):
                s.publish_document(owner, 'participant_manual', 'unapproved', 'controller/manual.pdf',
                    manifest, content=content)

    def test_admin_migration_roundtrip_legacy_preservation_and_immutability(self):
        spec = importlib.util.spec_from_file_location('admin_migration', ROOT / 'migrations/versions/home_sace_003_admin_documents.py')
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        with self.app.app_context():
            engine = create_engine(db.engine.url)
        try:
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text('CREATE TEMP TABLE "user" (id integer PRIMARY KEY)'))
                    conn.execute(text('SET LOCAL search_path TO pg_temp'))
                    conn.execute(text('CREATE TEMP TABLE sace_home_document_version (id integer PRIMARY KEY, approved_by integer NOT NULL, version text, storage_key text, sha256 text, source_manifest json)'))
                    conn.execute(text('INSERT INTO "user" VALUES (1)'))
                    conn.execute(text("INSERT INTO sace_home_document_version VALUES (1, 99, 'v1', 'private/old.pdf', 'digest', '{\"legacy\": true}')"))
                    conn.execute(text('CREATE TEMP TABLE binding (version_id integer REFERENCES sace_home_document_version(id))'))
                    conn.execute(text('INSERT INTO binding VALUES (1)'))
                    before = conn.execute(text('SELECT id, approved_by, version, storage_key, sha256, source_manifest::text FROM sace_home_document_version')).one()
                    with Operations.context(MigrationContext.configure(conn)):
                        migration.upgrade()
                        self.assertEqual(conn.execute(text('SELECT approved_by FROM sace_home_document_version')).scalar_one(), 99)
                        after = conn.execute(text('SELECT id, approved_by, version, storage_key, sha256, source_manifest::text FROM sace_home_document_version')).one()
                        self.assertEqual(before, after)
                        self.assertEqual(conn.execute(text('SELECT version_id FROM binding')).scalar_one(), 1)
                        migration.downgrade()
                        migration.upgrade()
                        conn.execute(text('INSERT INTO sace_home_document_version (id, approved_by, approved_by_admin_user_id) VALUES (2, NULL, 1)'))
                        for sql in ('UPDATE sace_home_document_version SET approved_by=98 WHERE id=1',
                                    'DELETE FROM sace_home_document_version WHERE id=2',
                                    'INSERT INTO sace_home_document_version (id, approved_by, approved_by_admin_user_id) VALUES (3, NULL, NULL)',
                                    'INSERT INTO sace_home_document_version (id, approved_by, approved_by_admin_user_id) VALUES (3, 99, 1)'):
                            with conn.begin_nested() as savepoint:
                                with self.assertRaises(Exception):
                                    conn.execute(text(sql))
                                savepoint.rollback()
                        with self.assertRaises(RuntimeError):
                            migration.downgrade()
                        self.assertEqual(conn.execute(text('SELECT count(*) FROM sace_home_document_version')).scalar_one(), 2)
                finally:
                    trans.rollback()
        finally:
            engine.dispose()

    def test_admin_all_slots_publish_and_replace_without_disk(self):
        with patch.object(storage, 'disk_path', side_effect=ValueError('invalid fallback path')):
            for kind in pub.KINDS:
                self.assertEqual(self.post_pdf(kind).status_code, 302)
                self.assertEqual(self.post_pdf(kind, pdf_bytes(200)).status_code, 302)
            with self.app.app_context():
                self.assertEqual(HomeDocumentVersion.query.count(), 10)
                for version in HomeDocumentVersion.query.all():
                    self.assertTrue(ex.valid_document(version))
                    self.assertEqual(s.document_content(version), self.objects[version.storage_key])
        self.assertEqual(self.upload_mock.call_count, 10)
        # Explicit storage-level authenticated readback precedes version insertion.
        self.assertGreaterEqual(self.read_mock.call_count, 10)

    def test_admin_failed_r2_replacements_preserve_current_and_history(self):
        self.assertEqual(self.post_pdf().status_code, 302)
        with self.app.app_context():
            original = s.latest_version('timetable')
            original_id, original_key = original.id, original.storage_key
        failures = (
            ('upload', 'upload_bytes_to_r2', OSError('upload failed'), 503),
            ('readback', 'read_file_from_r2', OSError('readback failed'), 503),
            ('hash', 'read_file_from_r2', b'%PDF-corrupt', 400),
        )
        for name, method, failure, status in failures:
            kwargs = {'side_effect': failure} if isinstance(failure, Exception) else {'return_value': failure}
            with self.subTest(failure=name), patch.object(storage.r2, method, **kwargs):
                self.assertEqual(self.post_pdf(content=pdf_bytes(300)).status_code, status)
            with self.app.app_context():
                self.assertEqual(HomeDocumentVersion.query.count(), 1)
                self.assertEqual(s.latest_version('timetable').id, original_id)
                self.assertEqual(s.document_content(db.session.get(HomeDocumentVersion, original_id)),
                                 self.objects[original_key])
        self.assertEqual(self.post_pdf(content=pdf_bytes(400)).status_code, 302)
        with self.app.app_context():
            self.assertEqual(HomeDocumentVersion.query.count(), 2)
            self.assertNotEqual(s.latest_version('timetable').id, original_id)

    def test_admin_db_failure_keeps_previous_version_and_uploaded_object(self):
        self.assertEqual(self.post_pdf().status_code, 302)
        with self.app.app_context():
            original_id = s.latest_version('timetable').id
        old_keys = set(self.objects)
        with patch.object(db.session, 'commit', side_effect=RuntimeError('database commit failed')):
            self.assertEqual(self.post_pdf(content=pdf_bytes(350)).status_code, 503)
        self.assertEqual(len(set(self.objects) - old_keys), 1)
        with self.app.app_context():
            self.assertEqual(HomeDocumentVersion.query.count(), 1)
            self.assertEqual(s.latest_version('timetable').id, original_id)
            previous = db.session.get(HomeDocumentVersion, original_id)
            self.assertEqual(s.document_content(previous), self.objects[previous.storage_key])


    def test_admin_all_slots_auditor_view_download_with_unusable_disk(self):
        original = self.app.config['SACE_HOME_REQUIREMENTS_VERSION']
        self.addCleanup(self.app.config.__setitem__, 'SACE_HOME_REQUIREMENTS_VERSION', original)
        self.app.config['SACE_HOME_REQUIREMENTS_VERSION'] = ex.REQUIREMENTS
        self.client.get('/logout')
        self.provision_home()
        auditor, aid = self.join_home(self.code_home())
        self.signin(self.uid)
        with patch.object(storage, 'disk_path', side_effect=PermissionError('disk unavailable')):
            for kind in pub.KINDS:
                self.assertEqual(self.post_pdf(kind).status_code, 302)
                with self.app.app_context():
                    version = s.latest_version(kind)
                    vid, key = version.id, version.storage_key
                target = f'/sace/home/documents/{vid}/content?assignment_id={aid}'
                for suffix in ('', '&download=1'):
                    self.read_mock.reset_mock()
                    response = auditor.get(target + suffix)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.data, self.objects[key])
                    self.assertEqual(response.headers['Cache-Control'], 'private, no-store')
                    self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
                    self.assertIn('attachment' if suffix else 'inline', response.headers['Content-Disposition'])
                    self.read_mock.assert_called_once_with(key, bucket='test-home-private')
                    response.close()



if __name__ == '__main__':
    suite = unittest.TestSuite(AdminDocuments(name) for name in dir(AdminDocuments) if name.startswith('test_admin_'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
