"""Protected HOME Board document access in connection-local test fixtures.

Inherits the HOME examination regressions; never publishes workshop manuals.
"""
import hashlib
import re
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

    def document_row(self, board, kind):
        target = (self.base + '/materials/' + kind).encode()
        title = self.board()[kind]['title'].encode()
        return next(row for row in re.findall(rb'<tr>.*?</tr>', board, re.S)
                    if target in row or title in row)

    def evidence_counts(self, kind):
        with self.app.app_context():
            return {event: HomeEvidence.query.filter_by(assignment_id=self.aid,
                    item=kind, event=event).count()
                    for event in ('examined', 'document_bound')}

    def test_board_document_view_inline_and_authorization(self):
        empty = self.auditor.get(self.base + '/board').data
        for kind in self.kinds:
            row = self.document_row(empty, kind)
            self.assertIn(b'>Outstanding</td>', row)
            self.assertIn(b'>View</a>', row)
        with self.app.app_context():
            self.assertEqual(HomeEvidence.query.filter_by(assignment_id=self.aid, event='examined').count(), 0)
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
                self.assertIn((self.base + '/materials/' + kind).encode(), board)
                self.assertIn(b'>View</a>', self.document_row(board, kind))
                self.assertNotIn(b'>Download</a>', board)
                self.assertNotIn(b'>Email</a>', board)
                material = self.auditor.get(self.base + '/materials/' + kind).data
                self.assertIn(target.encode(), material)
                self.assertIn(b'pdfjsLib.getDocument(', material)
                self.assertIn(b'<canvas id="document"', material)
                self.assertNotIn(b'Open document</a>', material)
                self.assertNotIn(b'<form method="post">', material)
                self.assertNotIn(b'>Examined</button>', material)
                self.assertTrue(self.board()[kind]['examined'])
                self.assertLess(material.index(b'>Back to Auditor Board</a>'), material.index(b'<canvas id="document"'))
                before_back = self.evidence_counts(kind)
                back = re.search(rb'href="([^"]+)">Back to Auditor Board</a>', material).group(1).decode()
                self.assertEqual(back, self.base + '/board')
                self.assertEqual(self.auditor.get(back).status_code, 200)
                self.assertEqual(self.evidence_counts(kind), before_back)
                self.assertNotIn(b'>Download</a>', material)
                self.assertNotIn(b'>Email</a>', material)
                self.assertEqual(self.auditor.post(self.base + '/materials/' + kind,
                    data={'version_id': vid}).status_code, 302)
                self.assertNotIn(str(path).encode(), board)
                self.assertNotIn(b'/static/', board)
                for download in ('', '&download=1', '&download=true', '&download=attachment',
                                 '&download=0&download=1', '&Download=1&attachment=1&as_attachment=true'):
                    with self.auditor.get(target + download) as response:
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.data, data)
                        self.assertTrue(response.headers['Content-Disposition'].startswith('inline;'))
                        self.assertIn('no-store', response.headers['Cache-Control'])
                    for client in (ordinary, foreign):
                        self.assertEqual(client.get(target + download).status_code, 403)
                        self.assertEqual(client.get(f'/sace/home/documents/{vid}/content' +
                            ('?download=1' if download else '')).status_code, 403)
                    self.assertIn(anonymous.get(target + download).status_code, (302, 401, 403))
                    foreign_target = f'/sace/home/documents/{vid}/content?assignment_id={foreign_id}'
                    self.assertEqual(self.auditor.get(foreign_target + download).status_code, 403)
                self.assertTrue(self.board()[kind]['examined'])
                self.assertEqual(self.auditor.get(self.base + '/materials/' + kind).status_code, 200)
                confirmation = self.auditor.post(self.base + '/materials/' + kind,
                    data={'version_id': vid})
                self.assertEqual(confirmation.status_code, 302)
                self.assertEqual(confirmation.location, self.base + '/board')
                returned = self.auditor.get(confirmation.location)
                self.assertEqual(returned.status_code, 200)
                examined_row = self.document_row(returned.data, kind)
                self.assertIn(b'>Examined</td>', examined_row)
                self.assertIn(b'>View</a>', examined_row)
                self.assertIn((self.base + '/materials/' + kind).encode(), examined_row)
                self.assertNotIn(b'>Download</a>', returned.data)
                self.assertNotIn(b'>Email</a>', returned.data)
                before_back = self.evidence_counts(kind)
                self.assertEqual(self.auditor.get(back).status_code, 200)
                self.assertEqual(self.evidence_counts(kind), before_back)
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
        for kind in self.kinds:
            row = self.document_row(board, kind)
            self.assertIn(b'>Examined</td>', row)
            self.assertIn(b'>View</a>', row)
        self.assertNotIn(b'>Download</a>', board)
        self.assertEqual(board.count(b'>Examined</td>'), 5)
        summary = self.auditor.get(self.base + '/summary').data
        self.assertIn(b'<p>Auditor examination records evidence for HOME endorsement.</p>', summary)

    def test_assignment_and_version_forgeries_and_csrf(self):
        vid, _, _ = self.fixture_document('application_form_1')
        target = f'/sace/home/documents/{vid}/content?assignment_id={self.aid}'
        self.assertEqual(self.auditor.get(self.base + '/materials/application_form_1').status_code, 200)
        for value in ('forged', '0', '-1', str(self.aid + 10000)):
            self.assertEqual(self.auditor.get(f'/sace/home/documents/{vid}/content?assignment_id={value}&download=1').status_code, 403)
        self.assertEqual(self.auditor.get(f'/sace/home/documents/{vid}/content?download=1').status_code, 403)
        self.assertEqual(self.auditor.post(self.base + '/materials/application_form_1',
            data={'version_id': vid + 10000}).status_code, 409)
        self.assertEqual(self.auditor.get(target).status_code, 200)
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            self.assertEqual(self.auditor.post(self.base + '/materials/application_form_1',
                data={'version_id': vid}).status_code, 400)
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
        with self.app.app_context():
            self.assertEqual(HomeEvidence.query.filter_by(assignment_id=self.aid,
                item='application_form_1', event='examined', document_version_id=vid).count(), 1)
            db.session.get(HomeAssignment, self.aid).status = 'revoked'
            db.session.commit()
        self.assertEqual(self.auditor.get(target + '&download=1').status_code, 403)

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
