"""Protected HOME Board document access in connection-local test fixtures.

Inherits the HOME examination regressions; never publishes workshop manuals.
"""
import hashlib
import unittest
from pathlib import Path
from home_sace_examination_runner import HomeExamination, f, db, HomeAssignment, HomeController, HomeEvidence
from app.models.sace_home import HomeDocument, HomeDocumentVersion


class HomeDocumentAccess(HomeExamination):
    kinds = ('application_form_1', 'application_form_2', 'timetable',
             'participant_manual', 'facilitator_manual')

    def fixture_document(self, kind, approved=True):
        with self.app.app_context():
            document = HomeDocument(kind=kind, title=kind)
            db.session.add(document)
            db.session.flush()
            path = Path(self.documents.name) / (kind + '.pdf')
            data = b'%PDF-1.4\nTemporary HOME access test fixture: ' + kind.encode()
            path.write_bytes(data)
            manifest = {'subject': f.s.SUBJECT, 'kind': kind}
            if approved:
                manifest['home_approval'] = {'approved_by': 'test fixture', 'reference': 'test-only'}
            version = HomeDocumentVersion(document_id=document.id, version='access-test-v1',
                storage_key=path.name, sha256=hashlib.sha256(data).hexdigest(),
                approved_by=HomeController.query.one().id, source_manifest=manifest)
            db.session.add(version)
            db.session.commit()
            return version.id, path, data

    def test_board_document_view_download_and_authorization(self):
        empty = self.auditor.get(self.base + '/board').data
        self.assertNotIn(b'>View</a>', empty)
        self.assertNotIn(b'>Download</a>', empty)
        ordinary = self.app.test_client()
        self.user('document-outsider@example.test')
        self.login(ordinary, 'document-outsider@example.test')
        foreign, foreign_id = self.join_home(self.code_home(), email='document-foreign@example.test')
        anonymous = self.app.test_client()
        for kind in self.kinds:
            with self.subTest(kind=kind):
                vid, path, data = self.fixture_document(kind)
                target = f'/sace/home/documents/{vid}/content?assignment_id={self.aid}'
                board = self.auditor.get(self.base + '/board').data
                self.assertIn(target.replace('&', '&amp;').encode(), board)
                self.assertIn((target + '&amp;download=1').encode(), board)
                self.assertNotIn(str(path).encode(), board)
                self.assertNotIn(b'/static/', board)
                for download in ('', '&download=1'):
                    with self.auditor.get(target + download) as response:
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.data, data)
                        disposition = 'attachment;' if download else 'inline;'
                        self.assertTrue(response.headers['Content-Disposition'].startswith(disposition))
                        self.assertIn('no-store', response.headers['Cache-Control'])
                    for client in (ordinary, foreign):
                        self.assertEqual(client.get(target + download).status_code, 403)
                        self.assertEqual(client.get(f'/sace/home/documents/{vid}/content' +
                            ('?download=1' if download else '')).status_code, 403)
                    self.assertIn(anonymous.get(target + download).status_code, (302, 401, 403))
                    foreign_target = f'/sace/home/documents/{vid}/content?assignment_id={foreign_id}'
                    self.assertEqual(self.auditor.get(foreign_target + download).status_code, 403)
                self.assertFalse(self.board()[kind]['examined'])
                self.assertEqual(self.auditor.get(self.base + '/materials/' + kind).status_code, 200)
                self.assertEqual(self.auditor.post(self.base + '/materials/' + kind,
                    data={'version_id': vid}).status_code, 302)
                self.assertTrue(self.board()[kind]['examined'])
                with self.app.app_context():
                    self.assertEqual(HomeEvidence.query.filter_by(assignment_id=self.aid,
                        item=kind, event='examined', document_version_id=vid).count(), 1)
                path.write_bytes(b'%PDF-tampered-test-fixture')
                for download in ('', '&download=1'):
                    self.assertEqual(self.auditor.get(target + download).status_code, 409)
                self.assertNotIn((target + '&amp;download=1').encode(),
                    self.auditor.get(self.base + '/board').data)
                path.write_bytes(data)
        board = self.auditor.get(self.base + '/board').data
        self.assertEqual(board.count(b'>View</a>'), 5)
        self.assertEqual(board.count(b'>Download</a>'), 5)
        summary = self.auditor.get(self.base + '/summary').data
        self.assertIn(b'<p>Auditor examination records evidence for HOME endorsement.</p>', summary)

    def test_unapproved_manuals_stay_unavailable(self):
        for kind in ('participant_manual', 'facilitator_manual'):
            vid, path, data = self.fixture_document(kind, approved=False)
            target = f'/sace/home/documents/{vid}/content?assignment_id={self.aid}'
            self.assertFalse(self.board()[kind]['available'])
            board = self.auditor.get(self.base + '/board').data
            self.assertNotIn(target.encode(), board)
            for suffix in ('', '&download=1'):
                self.assertEqual(self.auditor.get(target + suffix).status_code, 409)
            self.assertEqual(self.auditor.post(self.base + '/materials/' + kind,
                data={'version_id': vid}).status_code, 409)
            with self.app.app_context():
                with self.assertRaises(ValueError):
                    f.s.publish_document(HomeController.query.one(), kind, 'unapproved',
                        path.name, {'subject': f.s.SUBJECT, 'kind': kind})


if __name__ == '__main__':
    unittest.main(defaultTest='HomeDocumentAccess', verbosity=2)
