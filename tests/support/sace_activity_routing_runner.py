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

from flask import abort, current_app, url_for
from flask_login import current_user, login_required
from app.program_sace.activity import ReadingActivity
from app.program_sace_home.activity import HomeActivity
from app.sace_activity import registry, subject_slugs

EMAIL = 'official@example.test'


class ThirdActivity:
    """Test-only activity: temporary fixture authority, independent of R/A role."""
    identifier = 'third'
    display_name = 'Third SACE Activity'

    def recognizes(self, path, subject):
        return subject == 'sace_test_third' or bool(path and path.startswith('/sace/test-third'))

    def has_authority(self):
        return current_user.id in current_app.config.get('TEST_THIRD_AUTHORITIES', {})

    def entry(self, target=None, *, selected=False):
        return target or url_for('test_third_entry')

    def continuation(self, target=None, *, selected=False):
        from flask import session
        context = session.get('test_third_continuation')
        if (isinstance(context, dict) and context.get('user_id') == current_user.id
                and context.get('expires_at', 0) > time.time() and self.has_authority()):
            return url_for('test_third_entry')
        return None


class ActivityRouting(f.HomeFoundation):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        @cls.app.get('/sace/test-third')
        @login_required
        def test_third_entry():
            if not ThirdActivity().has_authority():
                abort(403)
            return 'Third SACE guarded entry'

        @cls.app.get('/sace/test-third/evidence')
        @login_required
        def test_third_evidence():
            if not ThirdActivity().has_authority():
                abort(403)
            abort(409, description='Genuine evidence conflict')

    def setUp(self):
        super().setUp()
        self.app.config.pop('SACE_ACTIVITY_REGISTRY', None)
        self.app.config['TEST_THIRD_AUTHORITIES'] = {}

    def third(self, role='R'):
        with self.app.app_context():
            from app.models.auth import User
            user = User.query.filter_by(email=EMAIL).one()
            self.app.config['TEST_THIRD_AUTHORITIES'][user.id] = role
        with self.app.app_context():
            self.app.config['SACE_ACTIVITY_REGISTRY'] = dict(registry(), sace_test_third=ThirdActivity())

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
        self.assertIn(b'data-activity="reading"', page.data)
        self.assertIn(b'data-activity="home"', page.data)
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

    def test_routing_third_activity_all_role_combinations(self):
        # Real Reading/HOME authority across the full R/A matrix; extension is
        # registry-only, with no changes to either production adapter.
        for reading_role, home_role in (('R', 'R'), ('R', 'A'), ('A', 'R'), ('A', 'A')):
            for third_role in ('R', 'A'):
                with self.subTest(reading=reading_role, home=home_role, third=third_role):
                    self.setUp()
                    self.officials(reading_role, home_role)
                    self.third(third_role)
                    before = self.counts()
                    response = self.login(self.client, EMAIL)
                    for identifier in ('reading', 'home', 'third'):
                        self.assertIn(('data-activity="' + identifier + '"').encode(), response.data)
                    self.assertEqual(response.data.count(b'data-activity='), 3)
                    self.assertEqual(response.headers['Cache-Control'], 'private, no-store')
                    for path in ('/dashboard', '/bridge', '/register'):
                        self.assertEqual(self.client.get(path).data.count(b'data-activity='), 3)
                    self.assertEqual(self.counts(), before)
                    self.assertEqual(self.client.get('/sace/test-third').status_code, 200)
                    self.app.config['TEST_THIRD_AUTHORITIES'].clear()
                    self.assertEqual(self.client.get('/sace/test-third').status_code, 403)
                    self.assertNotIn(b'data-activity="third"', self.client.get('/dashboard').data)
                    self.assertEqual(self.counts(), before)

    def test_routing_third_single_authority_origins_and_conflict(self):
        self.officials(None, None)
        self.third('A')
        before = self.counts()
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/test-third')
        self.assertEqual(self.client.get('/sace/test-third/evidence').status_code, 409)
        self.assertEqual(self.counts(), before)
        self.client.get('/logout')
        self.assertEqual(self.login(self.client, EMAIL, '/sace/test-third').location, '/sace/test-third')
        self.client.get('/logout')
        self.assertEqual(self.client.post('/login?subject=sace_test_third', data={
            'email': EMAIL, 'password': 'test-password'}).location, '/sace/test-third')
        self.assertEqual(self.client.get('/register?subject=sace_test_third').location, '/sace/test-third')
        self.assertEqual(self.client.get('/dashboard/info/sace_test_third').location, '/sace/test-third')
        with self.app.test_request_context():
            self.assertIn('sace_test_third', subject_slugs())

    def test_routing_third_valid_stale_continuation_and_non_sace_origin(self):
        self.officials('R', 'A')
        self.third()
        with self.app.app_context():
            from app.models.auth import User
            uid = User.query.filter_by(email=EMAIL).one().id
        for expiry, expected in ((time.time()+900, 302), (0, 200)):
            self.client.get('/logout')
            with self.client.session_transaction() as state:
                state['test_third_continuation'] = {'user_id': uid, 'expires_at': expiry}
            response = self.login(self.client, EMAIL)
            self.assertEqual(response.status_code, expected)
            if expected == 302:
                self.assertEqual(response.location, '/sace/test-third')
        self.client.get('/logout')
        self.assertEqual(self.login(self.client, EMAIL, '/ordinary').location, '/ordinary')

    def test_routing_competing_real_continuations_ignore_registry_order(self):
        self.officials('R', 'A')
        # Start Reading first, then HOME: navigation retains independent intent.
        f.h.AccessJourneys.pending_r(self, self.client)
        provider = self.app.test_client()
        saved = self.client
        self.client = provider
        self.login(provider, 'home-provider@example.test', '/sace/home/')
        code = self.code_home()
        self.client = saved
        self.client.post('/sace/home/join', data={'code': code})
        self.client.post('/sace/home/pledge', data={'accept': 'yes'})
        before = self.counts()
        response = self.login(self.client, EMAIL)
        self.assertIn(b'Choose SACE Activity', response.data)
        self.assertEqual(before, self.counts())
        with self.app.app_context():
            self.app.config['SACE_ACTIVITY_REGISTRY'] = dict(reversed(tuple(registry().items())))
        self.assertIn(b'Choose SACE Activity', self.login(self.client, EMAIL).data)
        self.assertEqual(before, self.counts())
        with self.client.session_transaction() as state:
            self.assertIn(home_intent.KEY, state)
            self.assertIn(access.PROVISIONING_KEY, state)
        self.assertEqual(self.client.get('/register?subject=sace_home_endorsement&next=/sace/home/claim').location,
                         '/sace/home/claim')

    def test_routing_zero_authority_fallback(self):
        self.officials(None, None)
        before = self.counts()
        response = self.login(self.client, EMAIL)
        self.assertEqual(response.location, '/dashboard')
        self.assertNotIn(b'Choose SACE Activity', response.data)
        self.assertEqual(self.counts(), before)


    def test_routing_registry_admin_exclusions_preserve_global_admin(self):
        import ast
        from flask import session
        from sqlalchemy import bindparam
        self.officials('R', 'R')
        self.third()
        self.app.config['TEST_THIRD_AUTHORITIES'].clear()
        with self.app.app_context():
            models = f.h.auth_models
            db.session.add(models.AuthSubject(id=902, slug='sace_test_third', name='Third', is_active=1))
            db.session.add(models.AuthSubjectAdmin(subject_id=902, email=EMAIL))
            db.session.add(models.AuthSubjectAdmin(subject_id=44, email=EMAIL))
            db.session.commit()
        before = self.counts()
        page = self.login(self.client, EMAIL)
        self.assertNotIn(b'data-activity="third"', page.data)
        with self.client.session_transaction() as state:
            self.assertEqual(state['admin_subjects'], ['sace_participant'])
        # Execute the real public session builder, with only its app bootstrap
        # omitted, against the same connection-local PostgreSQL fixtures.
        tree = ast.parse((ROOT / 'app/public/routes.py').read_text(encoding='utf-8'))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'refresh_bridge_session')
        env = dict(db=db, text=text, bindparam=bindparam, session=session)
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<public-session>', 'exec'), env)
        with self.app.test_request_context():
            user = models.User.query.filter_by(email=EMAIL).one()
            env['refresh_bridge_session'](user)
            self.assertEqual(session['admin_subjects'], ['sace_participant'])
            self.assertEqual(session['subjects_access']['sace_test_third'], 'locked')
            self.assertEqual(session['subjects_access']['sace_participant'], 'admin')
            self.assertFalse(session['is_admin'])
            self.assertEqual(self.counts(), before)
            db.session.execute(text('INSERT INTO auth_approved_admin (email, active) VALUES (:email, 1)'),
                               {'email': EMAIL})
            db.session.commit()
            env['refresh_bridge_session'](user)
            self.assertTrue(session['is_admin'])
            self.assertTrue(all(v == 'admin' for v in session['subjects_access'].values()))
            self.assertEqual(self.counts(), before)

    def test_routing_conflicting_and_untrusted_explicit_origins(self):
        self.officials('R', 'R')
        self.third()
        before = self.counts()
        response = self.client.post('/login', query_string={
            'subject': 'sace_test_third', 'next': '/sace/home/'},
            data={'email': EMAIL, 'password': 'test-password'})
        self.assertEqual(response.data.count(b'data-activity='), 3)
        self.assertEqual(self.counts(), before)
        # Subject context never authorises an external next URL.
        response = self.client.get('/register', query_string={
            'subject': 'sace_test_third', 'next': 'https://external.example/sace/test-third'})
        self.assertEqual(response.location, '/sace/test-third')



if __name__ == '__main__':
    suite = unittest.TestSuite(ActivityRouting(name) for name in dir(ActivityRouting) if name.startswith('test_routing_'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
