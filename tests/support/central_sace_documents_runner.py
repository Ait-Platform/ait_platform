"""Central SACE uploads in connection-local fixtures and fake R2; no migrations."""
import io
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import event
import home_sace_admin_documents_runner as h
from app.models.sace import SaceDocument
from werkzeug.datastructures import FileStorage

URL = '/admin/security/sace-management'
LEGACY = ['application_form', 'annexure_a', 'annexure_b', 'annexure_c', 'annexure_d', 'annexure_e']

class CentralDocuments(h.AdminDocuments):
    def assert_central_history(self, count):
        page = self.client.get(URL + '?program=home')
        self.assertEqual(page.status_code, 200)
        self.assertIn(f'Publication history ({count})'.encode(), page.data)
        self.assertEqual(page.data.count(b'&mdash; Current'), 1)
        self.assertIn(b'approved.pdf &mdash; Current', page.data)

    def post_pdf(self, kind='timetable', content=None, **extra):
        response = self.client.post(URL, data=dict(action='upload_document', slug='home',
            document_type=kind, file=(io.BytesIO(content if content is not None else h.pdf_bytes()), 'approved.pdf'), **extra))
        if response.status_code in (400, 503):
            with self.app.app_context():
                count = h.HomeDocumentVersion.query.count()
            if count == 1:
                self.assert_central_history(1)
        return response

    def test_central_legacy_forged_values_rejected_before_writes(self):
        with self.app.app_context():
            h.db.session.add(SaceDocument(slug='reading', document_type='application_form',
                file_name='existing.pdf', file_path='uploads/sace/existing.pdf'))
            h.db.session.commit()
            def snapshot():
                return [(row.id, row.slug, row.document_type, row.file_name, row.file_path, row.uploaded_at)
                    for row in SaceDocument.query.order_by(SaceDocument.id).all()]
            before = snapshot()
            engine = h.db.engine
        def forbid_mutation(conn, cursor, statement, parameters, context, executemany):
            if re.match(r'\s*(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:\w+\.)?"?sace_document"?\b', statement, re.I):
                raise AssertionError('legacy document mutation')
        event.listen(engine, 'before_cursor_execute', forbid_mutation)
        try:
            with patch('os.makedirs', side_effect=AssertionError('legacy directory')), \
                    patch.object(FileStorage, 'save', side_effect=AssertionError('legacy save')), \
                    patch.object(h.pub, 'publish_uploaded_pdf', side_effect=AssertionError('HOME publisher')):
                for program, kind in (('forged', 'application_form'), ('HOME', 'application_form'),
                        ('reading', 'forged'), ('reading', 'participant_manual'),
                        ('cultural_fire', 'forged'), ('loss', 'forged')):
                    with self.subTest(program=program, kind=kind):
                        response = self.client.post(URL, data={'action':'upload_document', 'slug':program,
                            'document_type':kind, 'file':(io.BytesIO(h.pdf_bytes()), 'existing.pdf')})
                        self.assertEqual(response.status_code, 400)
                        with self.app.app_context():
                            self.assertEqual(snapshot(), before)
                            self.assertEqual(h.HomeDocumentVersion.query.count(), 0)
        finally:
            event.remove(engine, 'before_cursor_execute', forbid_mutation)
        self.upload_mock.assert_not_called()

    def test_admin_failed_r2_replacements_preserve_current_and_history(self):
        with patch('os.makedirs', side_effect=AssertionError('legacy directory')), \
                patch.object(FileStorage, 'save', side_effect=AssertionError('legacy save')):
            super().test_admin_failed_r2_replacements_preserve_current_and_history()
        self.assert_central_history(2)
        with self.app.app_context():
            self.assertEqual(SaceDocument.query.count(), 0)

    def test_admin_db_failure_keeps_previous_version_and_uploaded_object(self):
        with patch('os.makedirs', side_effect=AssertionError('legacy directory')), \
                patch.object(FileStorage, 'save', side_effect=AssertionError('legacy save')):
            super().test_admin_db_failure_keeps_previous_version_and_uploaded_object()
        self.assert_central_history(1)
        with self.app.app_context():
            self.assertEqual(SaceDocument.query.count(), 0)

    def test_admin_replacement_preserves_existing_assignment_binding(self):
        super().test_admin_replacement_preserves_existing_assignment_binding()
        self.assert_central_history(2)

    def test_central_choices_and_no_legacy_home_writes(self):
        page = self.client.get(URL + '?program=home')
        self.assertEqual(page.status_code, 200)
        self.assertEqual(page.headers['Cache-Control'], 'private, no-store')
        options = re.search(rb'<select name="document_type"[^>]*>(.*?)</select>', page.data, re.S)[1]
        self.assertEqual(re.findall(rb'<option value="([^"]+)"', options), [k.encode() for k in h.pub.KINDS])
        with patch('os.makedirs', side_effect=AssertionError('legacy directory')), patch.object(FileStorage, 'save', side_effect=AssertionError('legacy save')):
            for kind in h.pub.KINDS:
                self.assertEqual(self.post_pdf(kind).status_code, 302)
            for kind in LEGACY + ['certificate', 'unknown']:
                self.assertEqual(self.post_pdf(kind).status_code, 400)
        with self.app.app_context():
            self.assertEqual(SaceDocument.query.count(), 0)
            self.assertEqual(h.HomeDocumentVersion.query.count(), len(h.pub.KINDS))
        page = self.client.get(URL + '?program=home')
        self.assertEqual(page.data.count(b'Publication history (1)'), 5)
        self.assertNotIn(b'endorsement/admin/', page.data)

    def test_central_legacy_programs_upload_and_replace(self):
        with tempfile.TemporaryDirectory() as root, patch.object(self.app, '_static_folder', root), patch.object(h.pub, 'publish_uploaded_pdf', side_effect=AssertionError('HOME publisher')):
            for program in ('reading', 'cultural_fire', 'loss'):
                page = self.client.get(URL + '?program=' + program)
                self.assertEqual(page.status_code, 200)
                options = re.search(rb'<select name="document_type"[^>]*>(.*?)</select>', page.data, re.S)[1]
                self.assertEqual(re.findall(rb'<option value="([^"]+)"', options), [k.encode() for k in LEGACY])
                for kind in LEGACY:
                    for width in (100, 200):
                        content = h.pdf_bytes(width)
                        response = self.client.post(URL, data={'action':'upload_document', 'slug':program,
                            'document_type':kind, 'file':(io.BytesIO(content), 'legacy.pdf')})
                        self.assertEqual(response.status_code, 200)
                        with self.app.app_context():
                            row = SaceDocument.query.filter_by(slug=program, document_type=kind).one()
                            self.assertEqual(row.file_path, f'uploads/sace/{program}_{kind}_legacy.pdf')
                            self.assertEqual((Path(root) / row.file_path).read_bytes(), content)
            with self.app.app_context():
                self.assertEqual(SaceDocument.query.count(), 18)
                self.assertEqual(h.HomeDocumentVersion.query.count(), 0)

    def test_central_database_authority_get_and_post(self):
        other = self.user('subject-only@example.test')
        with self.app.app_context():
            h.db.session.add(h.f.h.auth_models.AuthSubjectAdmin(email='subject-only@example.test', subject_id=901))
            h.db.session.add(h.HomeController(user_id=other))
            h.db.session.commit()
        for uid in (other, self.uid):
            self.signin(uid)
            if uid == self.uid:
                with self.app.app_context():
                    h.db.session.execute(h.text('UPDATE auth_approved_admin SET active=0'))
                    h.db.session.commit()
            for program in ('reading', 'home', 'cultural_fire', 'loss'):
                self.assertEqual(self.client.get(URL + '?program=' + program).status_code, 403)
                self.assertEqual(self.client.post(URL, data={'slug':program, 'action':'upload_document'}).status_code, 403)
        with self.app.app_context():
            h.db.session.execute(h.text('UPDATE auth_approved_admin SET active=1'))
            h.db.session.get(h.f.h.auth_models.User, self.uid).is_active = 0
            h.db.session.commit()
        self.assertIn(self.client.get(URL).status_code, (302, 403))
        self.assertIn(self.post_pdf().status_code, (302, 403))
        with self.client.session_transaction() as state:
            state.pop('_user_id', None)
        self.assertIn(self.client.get(URL).status_code, (302, 403))
        self.assertIn(self.post_pdf().status_code, (302, 403))
        self.upload_mock.assert_not_called()

    def test_central_sace_ra_cannot_enter(self):
        self.client.get('/logout')
        uid = self.provision_home('central-r@example.test')
        code = self.code_home()
        auditor, aid = self.join_home(code, 'central-a@example.test')
        for client in (self.client, auditor):
            with client.session_transaction() as state:
                state['is_admin'] = True
                state['role'] = 'admin'
            self.assertEqual(client.get(URL).status_code, 403)
            self.assertEqual(client.post(URL, data={'action':'upload_document', 'slug':'home'}).status_code, 403)
        self.upload_mock.assert_not_called()

    def test_central_pdf_and_csrf_validation(self):
        self.assertEqual(self.post_pdf(content=b'%PDF-invalid').status_code, 400)
        self.app.config['WTF_CSRF_ENABLED'] = True
        original = self.app.jinja_env.globals['csrf_token']
        self.app.jinja_env.globals['csrf_token'] = h.generate_csrf
        try:
            self.assertEqual(self.post_pdf().status_code, 400)
            page = self.client.get(URL + '?program=home')
            token = re.search(rb'name="csrf_token" value="([^"]+)"', page.data)[1].decode()
            self.assertEqual(self.post_pdf(csrf_token=token).status_code, 302)
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.app.jinja_env.globals['csrf_token'] = original

if __name__ == '__main__':
    names = [name for name in unittest.defaultTestLoader.getTestCaseNames(CentralDocuments)
        if name.startswith('test_central_') or name in (
            'test_admin_replacement_preserves_existing_assignment_binding',
            'test_admin_all_slots_publish_and_replace_without_disk',
            'test_admin_failed_r2_replacements_preserve_current_and_history',
            'test_admin_db_failure_keeps_previous_version_and_uploaded_object',
            'test_admin_all_slots_auditor_view_download_with_unusable_disk',
            'test_admin_storage_failure_missing_bucket_and_public_bucket_fail_closed')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(CentralDocuments(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
