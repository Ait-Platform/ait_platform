"""Activity selection through real authentication and local temporary authority tables."""
import importlib.util
from pathlib import Path
import time
import unittest
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('activity_support', ROOT / 'tests/support/home_sace_postgres_runner.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.extensions import db
from app.program_sace_home import continuation as home_intent, service as home
from app.program_sace import access, endorsement as reading
from app.models.sace_home import HomeAssignment, HomeInvitation

EMAIL = 'official@example.test'


class ActivityRouting(f.HomeFoundation):
    def officials(self, reading_role, home_role):
        self.user(EMAIL)
        if home_role == 'R':
            self.provision_home(EMAIL)
        elif home_role == 'A':
            self.provision_home('home-provider@example.test')
            self.client, self.home_assignment = self.join_home(self.code_home(), EMAIL, existing=True)
        self.client.get('/logout')
        if reading_role == 'R':
            f.h.AccessJourneys.provision(self, self.client, EMAIL, existing=True)
        elif reading_role == 'A':
            self.reading_provider = self.app.test_client()
            self.user('reading-provider@example.test')
            f.h.AccessJourneys.provision(self, self.reading_provider, 'reading-provider@example.test', existing=True)
            code, self.reading_assignment = f.h.AccessJourneys.code(self, self.reading_provider)
            self.client.post('/sace/join', data={'code': code})
            self.client.post('/sace/auditor_pledge')
            result = self.login(self.client, EMAIL, '/sace/claim_code')
            self.assertEqual(self.client.get(result.location, follow_redirects=True).status_code, 200)
        self.client.get('/logout')

    def counts(self):
        with self.app.app_context():
            names = ('user_enrollment', 'auth_subject_admin', 'sace_workshop_interactions',
                'sace_reading_controller_appointment', 'sace_reading_assignment_context',
                'sace_home_controller_appointment', 'sace_home_assignment', 'sace_home_evidence',
                'sace_home_pledge', 'sace_home_audit_event')
            return tuple(db.session.execute(text('SELECT count(*) FROM ' + name)).scalar_one() for name in names)

    def matrix(self, reading_role, home_role):
        self.officials(reading_role, home_role)
        before = self.counts()
        page = self.login(self.client, EMAIL)
        self.assertEqual(page.status_code, 200)
        self.assertIn(b'Choose SACE Activity', page.data)
        self.assertIn(b'Hands-On Math Education', page.data)
        self.assertIn(b'href="/sace/dashboard"', page.data)
        self.assertIn(b'href="/sace/home/"', page.data)
        self.assertEqual(before, self.counts())
        for path in ('/dashboard', '/bridge'):
            self.assertIn(b'Choose SACE Activity', self.client.get(path).data)
        self.assertEqual(before, self.counts())
        for target, expected in (('/sace/home/', '/sace/home/control' if home_role == 'R' else '/sace/home/'),
                ('/sace/dashboard', '/sace/provisioning' if reading_role == 'R' else '/sace/reading')):
            self.client.get('/logout')
            result = self.login(self.client, EMAIL, target)
            self.assertEqual(result.location, expected)
            self.assertEqual(self.client.get(target, follow_redirects=True).status_code, 200)
        self.client.get('/logout')
        self.assertIn(b'Choose SACE Activity', self.login(self.client, EMAIL, '/dashboard').data)
        # Clicking the HOME entry after authority is revoked cannot restore it.
        with self.app.app_context():
            if home_role == 'A':
                db.session.get(HomeAssignment, self.home_assignment).status = 'revoked'
            else:
                db.session.execute(text("UPDATE auth_subject_admin SET email='revoked@example.test' WHERE subject_id=901"))
            db.session.commit()
        response = self.client.get('/sace/home/', follow_redirects=True)
        self.assertNotIn(b'HOME Control Centre', response.data)
        self.assertNotIn(b'HOME Auditor Board', response.data)
        self.assertEqual(self.client.get('/sace/dashboard', follow_redirects=True).status_code, 200)

    def test_routing_reading_r_home_r(self):
        self.matrix('R', 'R')

    def test_routing_reading_a_home_a(self):
        self.matrix('A', 'A')

    def test_routing_reading_r_home_a(self):
        self.matrix('R', 'A')

    def test_routing_reading_a_home_r(self):
        self.matrix('A', 'R')

    def test_routing_single_activity_authority(self):
        self.officials(None, 'R')
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/home/control')
        self.client.get('/logout')
        with self.app.app_context():
            db.session.execute(text("UPDATE auth_subject_admin SET email='revoked@example.test' WHERE subject_id=901"))
            db.session.commit()
        f.h.AccessJourneys.provision(self, self.client, EMAIL, existing=True)
        self.client.get('/logout')
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/provisioning')

    def test_routing_reading_continuation_and_explicit_home_override(self):
        self.officials(None, 'R')
        provider = self.app.test_client()
        self.user('reading-provider@example.test')
        f.h.AccessJourneys.provision(self, provider, 'reading-provider@example.test', existing=True)
        code, _ = f.h.AccessJourneys.code(self, provider)
        self.client.post('/sace/join', data={'code': code})
        self.client.post('/sace/auditor_pledge')
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/claim_code')
        self.client.get('/sace/claim_code', follow_redirects=True)
        self.client.get('/logout')
        code, _ = f.h.AccessJourneys.code(self, provider)
        self.client.post('/sace/join', data={'code': code})
        self.client.post('/sace/auditor_pledge')
        self.assertEqual(self.login(self.client, EMAIL, '/sace/home/').location, '/sace/home/control')

    def test_routing_reading_r_continuation_without_next(self):
        self.officials(None, 'R')
        f.h.AccessJourneys.pending_r(self, self.client)
        result = self.login(self.client, EMAIL)
        self.assertTrue(result.location.startswith('/sace/provisioning?journey='))
        self.assertEqual(self.client.get(result.location, follow_redirects=True).status_code, 200)
        self.client.get('/logout')
        self.assertIn(b'Choose SACE Activity', self.login(self.client, EMAIL).data)

    def test_routing_home_continuation_and_explicit_reading_override(self):
        self.officials('R', None)
        provider = self.app.test_client()
        saved = self.client
        self.client = provider
        self.provision_home('home-provider@example.test')
        code = self.code_home()
        self.client = saved
        self.client.post('/sace/home/join', data={'code': code})
        self.client.post('/sace/home/pledge', data={'accept': 'yes'})
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/home/claim')
        self.assertEqual(self.client.get('/sace/home/claim', follow_redirects=True).status_code, 200)
        self.client.get('/logout')
        self.assertIn(b'Choose SACE Activity', self.login(self.client, EMAIL).data)
        self.client.get('/logout')
        with self.client.session_transaction() as state:
            state[home_intent.KEY] = {'kind': 'join', 'expires_at': time.time()+900, 'pending_hash': home.digest(code)}
            state['sace_home_pending_code'] = code
        self.assertEqual(self.login(self.client, EMAIL, '/sace/dashboard').location, '/sace/provisioning')

    def test_routing_stale_invalid_continuations_choose_neither_activity(self):
        self.officials('R', 'R')
        for changes in ({'expires_at': 0}, {'pending_hash': 'wrong'}, {'kind': 'unknown'}, {}):
            self.client.get('/logout')
            with self.client.session_transaction() as state:
                state['sace_home_pending_code'] = 'HOME-' + 'F'*24
                state[home_intent.KEY] = dict(kind='join', expires_at=time.time()+900,
                    pending_hash=home.digest(state['sace_home_pending_code']))
                state[home_intent.KEY].update(changes)
                state['pending_sace_code'] = 'STALE-READING'
                state['sace_evaluator_pledged'] = True
                state['sace_reading_auditor_journey'] = {'expires_at': time.time()+900,
                    'code_hash': home.digest('STALE-READING'), 'user_id': None}
            self.assertIn(b'Choose SACE Activity', self.login(self.client, EMAIL).data)

    def test_routing_single_auditor_authority(self):
        self.officials('A', None)
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/reading')

    def test_routing_single_home_auditor_authority(self):
        self.officials(None, 'A')
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/home/')

    def test_routing_malformed_continuations(self):
        self.officials('R', 'R')
        with self.client.session_transaction() as state:
            state[access.PROVISIONING_KEY] = {'started_at': 'invalid', 'nonce': None}
            state['pending_sace_code'] = 123
            state['sace_reading_auditor_journey'] = {'expires_at': time.time()+900}
            state[home_intent.KEY] = {'kind': 'join', 'expires_at': time.time()+900}
            state['sace_home_pending_code'] = 123
        self.assertIn(b'Choose SACE Activity', self.login(self.client, EMAIL).data)

    def test_routing_existing_registration_returns_to_explicit_activity(self):
        self.officials('R', 'R')
        self.login(self.client, EMAIL)
        result = self.client.get('/register?subject=sace_endorsement&next=/sace/dashboard')
        self.assertEqual(result.location, '/sace/provisioning')
        result = self.client.get('/dashboard/info/sace_home_endorsement', follow_redirects=True)
        self.assertIn(b'HOME Control Centre', result.data)


if __name__ == '__main__':
    suite = unittest.TestSuite(ActivityRouting(name) for name in dir(ActivityRouting) if name.startswith('test_routing_'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
