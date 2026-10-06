"""Real approved PDFs in connection-local HOME tables; no migrations."""
import importlib.util
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('home_document_support', ROOT / 'tests/support/home_sace_postgres_runner.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.extensions import db
from app.models.sace_home import HomeController, HomeDocumentVersion, HomeAssignment
from app.program_sace_home import examination as ex
from app.program_sace_home.endorsement_documents import publish, SOURCES


class EndorsementDocuments(f.HomeFoundation):
    def test_real_pdfs_board_provider_and_access(self):
        self.app.config['SACE_HOME_REQUIREMENTS_VERSION'] = ex.REQUIREMENTS
        self.provision_home()
        auditor, assignment_id = self.join_home(self.code_home())
        other, other_id = self.join_home(self.code_home(), email='other-home-a@example.test')
        with self.app.app_context():
            owner = HomeController.query.one()
            rows = publish(owner, ROOT / 'output/pdf', 'HOME source approval', 'User-approved real endorsement PDFs 2026-10-05')
            db.session.commit()
            identifiers = {kind: row.id for kind, row in rows.items()}
            reused = publish(owner, ROOT / 'output/pdf', 'HOME source approval', 'User-approved real endorsement PDFs 2026-10-05')
            self.assertEqual(identifiers, {kind: row.id for kind, row in reused.items()})
            self.assertEqual(HomeDocumentVersion.query.count(), 3)
            with patch.object(ex, 'reading_artifact_hashes', return_value={rows['application_form_1'].sha256}):
                with self.assertRaises(ValueError):
                    publish(owner, ROOT / 'output/pdf', 'reviewer', 'Reading isolation rejection')
            db.session.commit()
            board = f.s.board_items(db.session.get(HomeAssignment, assignment_id))
            self.assertEqual([item['kind'] for item in board][1:4], list(SOURCES))
        page = auditor.get(f'/sace/home/assignments/{assignment_id}/board')
        self.assertEqual(page.status_code, 200)
        provider = self.client.get('/sace/home/control/documents')
        self.assertEqual(provider.status_code, 200)
        anonymous = self.app.test_client()
        ordinary = self.app.test_client()
        self.user('ordinary-documents@example.test')
        self.login(ordinary, 'ordinary-documents@example.test')
        for kind, version_id in identifiers.items():
            base = f'/sace/home/documents/{version_id}/content'
            url = base + f'?assignment_id={assignment_id}'
            self.assertIn(url.replace('&', '&amp;').encode(), page.data)
            source = (ROOT / 'output/pdf' / SOURCES[kind]).read_bytes()
            response = auditor.get(url)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, source)
            response.close()
            download = auditor.get(url + '&download=1')
            self.assertEqual(download.status_code, 200)
            self.assertTrue(download.headers['Content-Disposition'].startswith('attachment;'))
            self.assertEqual(download.data, source)
            download.close()
            self.assertEqual(other.get(url).status_code, 403)
            self.assertEqual(ordinary.get(url).status_code, 403)
            self.assertNotEqual(anonymous.get(url).status_code, 200)
            self.assertEqual(auditor.get(base).status_code, 403)
            if kind.startswith('application_form'):
                self.assertIn(base.encode(), provider.data)
                with self.client.get(base) as provider_pdf:
                    self.assertEqual(provider_pdf.data, source)
        self.app.config['SACE_HOME_REQUIREMENTS_VERSION'] = 'home-foundation-v1'


if __name__ == '__main__':
    unittest.main(defaultTest='EndorsementDocuments.test_real_pdfs_board_provider_and_access', verbosity=2)
