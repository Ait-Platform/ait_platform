"""HOME Phase 2B HTTP and content checks; verified localhost, no app factory."""
import base64
import copy
import importlib.util
import json
import re
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('home_foundation_support', ROOT / 'tests/support/home_sace_postgres_runner.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.program_sace_home import content_snapshot as c, examination as ex, manual_sources, lifecycle as lc
from app.extensions import db
from app.models.sace_home import HomeAssignment, HomeEvidence, HomeDocumentVersion, HomeDocument, HomeController


class HomeExamination(f.HomeFoundation):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.content_dir = tempfile.TemporaryDirectory(prefix='home-examination-')
        cls.app.config['SACE_HOME_EXAMINATION_ROOT'] = cls.content_dir.name
        cls.app.config['SACE_HOME_REQUIREMENTS_VERSION'] = ex.REQUIREMENTS
        url = make_url(dotenv_values(ROOT / '.env')['DATABASE_URL'])
        if url.host not in ('localhost', '127.0.0.1', '::1') or url.database != 'ait_local_db' or url.query:
            raise RuntimeError('Local content source only')
        engine = create_engine(url)
        try:
            with engine.connect().execution_options(isolation_level='REPEATABLE READ', postgresql_readonly=True) as conn:
                with conn.begin():
                    cls.inventory = manual_sources.source_snapshot(conn, ROOT)
            cls.bundle = c.build(cls.inventory, ROOT, 'test-fixture', 'local HOME content, not production approval')
        finally:
            engine.dispose()

    @classmethod
    def tearDownClass(cls):
        cls.content_dir.cleanup()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        c.write_bundle(self.bundle, self.content_dir.name)
        self.app.config.pop('SACE_HOME_EXAMINATION_VERSION', None)
        self.provision_home()
        self.auditor, self.aid = self.join_home(self.code_home())
        self.base = f'/sace/home/assignments/{self.aid}'

    def chapter(self, stage, number):
        return self.base + f'/experience/{stage}/{number}'

    def confirm(self, url, version=None):
        return self.auditor.post(url, data={'version': version or self.bundle['version']})

    def test_phase2b_board_order_and_missing_documents(self):
        response = self.auditor.get(self.base + '/board')
        self.assertEqual(response.status_code, 200)
        with self.app.test_request_context('/'):
            row = db.session.get(HomeAssignment, self.aid)
            items = f.s.board_items(row)
            self.assertEqual([i['kind'] for i in items], list(ex.ITEMS))
            self.assertEqual(row.requirements_version, ex.REQUIREMENTS)
            self.assertTrue(all(not i['available'] for i in items if i['kind'] in ex.DOCUMENTS))
        for kind in ex.DOCUMENTS:
            self.assertIn(b'awaiting an approved HOME document', self.auditor.get(self.base + '/materials/' + kind).data)
            self.assertEqual(self.auditor.post(self.base + '/materials/' + kind, data={'version_id': '1'}).status_code, 409)
        self.assertEqual(self.auditor.post(self.base + '/completion').status_code, 409)

    def test_phase2b_all_chapters_source_fidelity_and_inert_presentation(self):
        manifest = self.bundle['manifest']
        self.assertEqual([x['chapter_number'] for x in manifest['chapters']], list(range(1, 31)))
        for stage, numbers in c.STAGES.items():
            for number in numbers:
                response = self.auditor.get(self.chapter(stage, number))
                self.assertEqual(response.status_code, 200, (stage, number, response.data[:300]))
                html = response.data.decode()
                for forbidden in ('home_bp.', '/home/chapter/', '/teacher/score/', 'Submit Answers',
                                  'Submit to Teacher', 'MARK COMPETENT', 'onclick=', '<script', '<iframe'):
                    self.assertNotIn(forbidden, html)
                self.assertEqual(html.count('<form'), 1)  # Only the HOME evidence confirmation.
                chapter = manifest['chapters'][number - 1]
                original = next(ch for ch in self.inventory['source']['chapters'] if ch['chapter_number'] == number)
                self.assertEqual(chapter['id'], original['id'])
                if number <= 10:
                    self.assertIn('Expected Answer', html)
                    self.assertIn('Final Checklist', html)
                if number >= 21:
                    self.assertIn('Theory Review', html)
                    self.assertIn('Chapter Summary', html)
        self.assertEqual(len(manifest['assessment']), 10)
        self.assertEqual(sum(len(ch['questions']) for ch in manifest['assessment']), 50)

    def test_phase2b_complete_stage_coverage_and_idempotence(self):
        version = self.bundle['version']
        for stage, numbers in c.STAGES.items():
            for number in numbers:
                url = self.chapter(stage, number)
                self.assertEqual(self.auditor.get(url).status_code, 200)
                with self.app.app_context():
                    row = db.session.get(HomeAssignment, self.aid)
                    self.assertFalse(ex.journey_complete(row, version))
                self.assertEqual(self.confirm(url).status_code, 302)
        with self.app.app_context():
            row = db.session.get(HomeAssignment, self.aid)
            self.assertTrue(ex.journey_complete(row, version))
            self.assertEqual(HomeEvidence.query.filter_by(assignment_id=self.aid, event='examined').count(), 41)
        self.confirm(self.chapter('review', 30))
        with self.app.app_context():
            self.assertEqual(HomeEvidence.query.filter_by(assignment_id=self.aid, event='examined').count(), 41)
        # Educational completion does not falsely satisfy the six missing documents.
        self.assertEqual(self.auditor.post(self.base + '/completion').status_code, 409)

    def test_phase2b_fixed_assessment_and_safe_certificate_specimens(self):
        for kind in ('assessment', 'certificate'):
            url = self.base + '/materials/' + kind
            self.assertEqual(self.confirm(url).status_code, 409)  # Not opened.
            response = self.auditor.get(url)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'SPECIMEN', response.data)
            self.assertIn(b'NON-ISSUED', response.data)
            self.assertEqual(self.confirm(url, '0' * 64).status_code, 409)
            self.assertEqual(self.confirm(url).status_code, 302)
            if kind == 'certificate':
                self.assertIn(b'Sample HOME Learner', response.data)
                self.assertIn(b'HOME-SPECIMEN-NONISSUED', response.data)
                self.assertIn(b'not an academic qualification', response.data)
                self.assertNotIn(b'HOME A', response.data)
        with self.app.app_context():
            evidence = HomeEvidence.query.filter_by(item='assessment', event='examined').one()
            self.assertEqual(len(evidence.details['question_ids']), 50)
            self.assertEqual(evidence.details['version'], self.bundle['version'])

    def test_phase2b_stable_content_and_document_pinning(self):
        url = self.chapter('application', 11)
        before = self.auditor.get(url).data
        changed = copy.deepcopy(self.bundle)
        changed['manifest']['approval']['reference'] = 'next local version'
        changed['version'] = c.digest(changed['manifest'])
        c.write_bundle(changed, self.content_dir.name)
        self.assertEqual(self.auditor.get(url).data, before)
        self.assertEqual(self.confirm(url, changed['version']).status_code, 409)
        self.assertEqual(self.confirm(url).status_code, 302)
        with self.app.app_context():
            row = HomeEvidence.query.filter_by(item='content_manifest', event='bound').one()
            self.assertEqual(row.details['version'], self.bundle['version'])
            owner = HomeController.query.one()
            file = Path(self.documents.name) / 'fixture.pdf'
            file.write_bytes(b'%PDF-1.4\nHOME test document, not substantive provider content')
            manifest = {'subject': f.s.SUBJECT, 'kind': 'application_form_1',
                'home_approval': {'approved_by': 'test', 'reference': 'fixture'}}
            first = f.s.publish_document(owner, 'application_form_1', 'test-v1', file.name, manifest)
            first_id = first.id
            db.session.commit()
        document_url = self.base + '/materials/application_form_1'
        self.assertEqual(self.auditor.get(document_url).status_code, 200)
        self.assertEqual(self.auditor.get(f'/sace/home/documents/{first_id}/content?assignment_id={self.aid}').status_code, 200)
        with self.app.app_context():
            second = f.s.publish_document(HomeController.query.one(), 'application_form_1', 'test-v2', 'fixture.pdf', manifest)
            second_id = second.id
            db.session.commit()
        self.assertEqual(self.auditor.post(document_url, data={'version_id': second_id}).status_code, 409)
        self.assertEqual(self.auditor.post(document_url, data={'version_id': first_id}).status_code, 302)
        with self.app.app_context():
            first = db.session.get(HomeDocumentVersion, first_id)
            first.source_manifest = dict(manifest, altered='unexpected edit')
            db.session.commit()
        self.assertEqual(self.auditor.get(document_url).status_code, 409)
        self.assertEqual(self.auditor.post(document_url, data={'version_id': first_id}).status_code, 409)

    def test_phase2b_snapshot_determinism_mapping_and_tamper_rejection(self):
        again = c.build(self.inventory, ROOT, 'test-fixture', 'local HOME content, not production approval')
        self.assertEqual(again, self.bundle)
        changed = copy.deepcopy(self.inventory)
        mapping = {ch['id']: 1000 + ch['chapter_number'] * 7 for ch in changed['source']['chapters']}
        for ch in changed['source']['chapters']:
            ch['id'] = mapping[ch['id']]
        for q in changed['source']['questions']:
            q['chapter_id'] = mapping[q['chapter_id']]
        changed['source_sha256'] = c.digest(changed['source'])
        mapped = c.build(changed, ROOT, 'test', 'mapping fixture')
        self.assertEqual(mapped['manifest']['assessment'][0]['chapter_id'], 1147)
        self.assertEqual(mapped['manifest']['assessment'][0]['chapter_number'], 21)
        self.auditor.get(self.chapter('application', 11))
        target = Path(self.content_dir.name) / (self.bundle['version'] + '.json')
        damaged = copy.deepcopy(self.bundle)
        damaged['manifest']['chapters'][10]['questions'][0]['question'] = 'tampered'
        target.write_text(c.canonical(damaged), encoding='utf-8')
        try:
            self.assertEqual(self.auditor.get(self.chapter('application', 11)).status_code, 409)
        finally:
            target.write_text(c.canonical(self.bundle), encoding='utf-8')
        manifest = copy.deepcopy(self.bundle['manifest'])
        key = next(iter(manifest['assets']))
        manifest['assets'][key]['data'] = base64.b64encode(b'changed asset').decode()
        with self.assertRaises(ValueError): c.validate(manifest)

    def test_phase2b_ownership_csrf_deadline_and_revocation(self):
        url = self.chapter('practical', 1)
        self.assertEqual(self.client.get(url).status_code, 403)
        self.assertEqual(self.client.get(self.base + '/materials/certificate').status_code, 403)
        self.auditor.get(url)
        key = next(iter(self.bundle['manifest']['assets']))
        asset_url = f'/sace/home/assignments/{self.aid}/content-assets/{self.bundle["version"]}/{key}'
        self.assertEqual(self.auditor.get(asset_url).status_code, 200)
        self.assertEqual(self.client.get(asset_url).status_code, 403)
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            self.assertEqual(self.confirm(url).status_code, 400)
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
        with self.app.app_context():
            row = lc.Engagement.query.one()
            instant = lc.now()
            row.status = 'completion_pending'
            row.completion_requested_by_user_id = row.created_by_user_id
            row.completion_requested_at = instant
            row.completion_deadline = instant + timedelta(hours=48)
            deadline = row.completion_deadline
            db.session.commit()
        with patch.object(lc, 'now', return_value=deadline):
            for path in (url, asset_url, self.base + '/materials/assessment', self.base + '/materials/certificate'):
                self.assertEqual(self.auditor.get(path).status_code, 403)
            self.assertEqual(self.confirm(url).status_code, 403)
        with self.app.app_context():
            row = lc.Appointment.query.one()
            row.status, row.ended_at = 'revoked', lc.now()
            db.session.commit()
        self.assertEqual(self.auditor.get(url).status_code, 403)

    def test_phase2b_no_participant_or_reading_mutations_or_delivery(self):
        mutations = []
        def guard(conn, cursor, statement, parameters, context, many):
            match = re.match(r'\s*(?:INSERT INTO|UPDATE|DELETE FROM)\s+"?([a-z_]+)', statement, re.I)
            if match:
                mutations.append(match.group(1))
                self.assertTrue(match.group(1).startswith('sace_home_'), statement)
            self.assertNotRegex(statement.lower(), r'\b(home_progress|home_practical_submissions|home_final_assessments|qualifications|home_questions|home_chapters)\b')
        with self.app.app_context():
            engine = db.engine
            enrolled = [(r.id, r.status) for r in f.h.auth_models.UserEnrollment.query.all()]
            reading = f.h.Interaction.query.count()
        event.listen(engine, 'before_cursor_execute', guard)
        try:
            for stage, numbers in c.STAGES.items():
                url = self.chapter(stage, next(iter(numbers)))
                self.assertEqual(self.auditor.get(url).status_code, 200)
                self.assertEqual(self.confirm(url).status_code, 302)
            for kind in ('assessment', 'certificate'):
                url = self.base + '/materials/' + kind
                self.assertEqual(self.auditor.get(url).status_code, 200)
                self.assertEqual(self.confirm(url).status_code, 302)
        finally:
            event.remove(engine, 'before_cursor_execute', guard)
        self.assertTrue(mutations)
        with self.app.app_context():
            self.assertEqual([(r.id, r.status) for r in f.h.auth_models.UserEnrollment.query.all()], enrolled)
            self.assertEqual(f.h.Interaction.query.count(), reading)
        # No participant handlers or mailer are imported by the runtime wrapper.
        import sys
        self.assertNotIn('app.subject_home.routes', sys.modules)
        self.assertNotIn('app.utils.mailer', sys.modules)

    def test_phase2b_historical_requirements_and_unavailable_bundle(self):
        with self.app.app_context():
            row = db.session.get(HomeAssignment, self.aid)
            row.requirements_version = 'home-foundation-v1'
            db.session.commit()
        self.assertIn(b'not yet available', self.auditor.get(self.base + '/experience').data)
        self.assertEqual(self.auditor.get(self.chapter('practical', 1)).status_code, 409)
        with self.app.app_context():
            self.assertEqual(len(f.s.board_items(db.session.get(HomeAssignment, self.aid))), 8)
            row = db.session.get(HomeAssignment, self.aid)
            row.requirements_version = ex.REQUIREMENTS
            db.session.commit()
        self.app.config['SACE_HOME_EXAMINATION_VERSION'] = '0' * 64
        self.assertEqual(self.auditor.get(self.chapter('application', 11)).status_code, 409)
        self.assertEqual(self.confirm(self.chapter('application', 11)).status_code, 409)

    def test_phase2b_real_layout_and_successful_examination_completion(self):
        from jinja2 import FileSystemLoader
        loader = self.app.jinja_loader
        self.app.jinja_loader = FileSystemLoader(str(ROOT / 'templates'))
        self.app.jinja_env.cache.clear()
        try:
            for path in (self.base + '/board', self.base + '/experience', self.chapter('practical', 1),
                         self.chapter('theory', 21), self.base + '/materials/assessment', self.base + '/materials/certificate'):
                result = self.auditor.get(path)
                self.assertEqual(result.status_code, 200)
                self.assertIn(b'<!DOCTYPE html>', result.data)
                self.assertNotIn(b'/home/chapter/', result.data)
        finally:
            self.app.jinja_loader = loader
            self.app.jinja_env.cache.clear()
        self.auditor.post(self.base + '/summary')
        for stage, numbers in c.STAGES.items():
            for number in numbers:
                url = self.chapter(stage, number)
                self.auditor.get(url)
                self.assertEqual(self.confirm(url).status_code, 302)
        for kind in ('assessment', 'certificate'):
            url = self.base + '/materials/' + kind
            self.auditor.get(url)
            self.assertEqual(self.confirm(url).status_code, 302)
        for kind in sorted(ex.DOCUMENTS):
            with self.app.app_context():
                file = Path(self.documents.name) / (kind + '.pdf')
                file.write_bytes(b'%PDF-1.4\nIsolated test fixture, not a provider artifact')
                manifest = {'subject': f.s.SUBJECT, 'kind': kind,
                    'home_approval': {'approved_by': 'test', 'reference': 'test fixture'}}
                if kind in {'participant_manual', 'facilitator_manual'}:
                    manifest.update(self.inventory)
                    manifest['source_approval'] = {'production_parity_checked': True,
                        'approved_by': 'synthetic test approval only', 'approved_at': '2000-01-01'}
                doc = f.s.publish_document(HomeController.query.one(), kind, 'fixture-v1', file.name, manifest)
                vid = doc.id
                db.session.commit()
            url = self.base + '/materials/' + kind
            self.assertEqual(self.auditor.get(url).status_code, 200)
            self.assertEqual(self.auditor.get(f'/sace/home/documents/{vid}/content?assignment_id={self.aid}').status_code, 200)
            self.assertEqual(self.auditor.post(url, data={'version_id': vid}).status_code, 302)
        with self.app.app_context():
            self.assertEqual(f.s.missing(db.session.get(HomeAssignment, self.aid)), [])
        self.assertEqual(self.auditor.post(self.base + '/completion').status_code, 200)
        with self.app.app_context():
            self.assertEqual(db.session.get(HomeAssignment, self.aid).status, 'completed')
            self.assertEqual(lc.Engagement.query.one().status, 'active')
            self.assertEqual(f.h.auth_models.AuthSubjectAdmin.query.count(), 1)

    def test_phase2b_no_reading_document_substitution(self):
        with self.app.app_context():
            owner = HomeController.query.one()
            for name in ('App_Form_1.pdf', 'App_Form_2.pdf', 'P_Guide.pdf', 'F_Guide.pdf', 'Reading Timetable.pdf'):
                file = Path(self.documents.name) / name
                file.write_bytes((ROOT / 'app/static/pdf' / name).read_bytes())
                with self.assertRaisesRegex(ValueError, 'Reading'):
                    f.s.publish_document(owner, 'application_form_1', 'test', name,
                        {'subject': f.s.SUBJECT, 'kind': 'application_form_1',
                         'home_approval': {'approved_by': 'test', 'reference': 'relabel attempt'}})
            self.assertEqual(HomeDocumentVersion.query.count(), 0)
        self.assertEqual(self.auditor.get('/sace/home/examination.json').status_code, 404)


if __name__ == '__main__':
    names = [n for n in unittest.defaultTestLoader.getTestCaseNames(HomeExamination) if n.startswith('test_phase2b_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(HomeExamination(n) for n in names))
    raise SystemExit(not result.wasSuccessful())
