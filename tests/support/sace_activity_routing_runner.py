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

        # URL-building targets for the real platform Bridge admin branch.
        for path, endpoint in (('/admin/', 'admin_bp.index'),
                ('/general/', 'general_bp.index')):
            if endpoint not in cls.app.view_functions:
                cls.app.add_url_rule(path, endpoint=endpoint, view_func=lambda **kwargs: 'Admin destination')

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

    def approve_platform_admin(self):
        with self.app.app_context():
            db.session.execute(text("INSERT INTO auth_approved_admin (email, active) VALUES (:email, 1)"),
                               {'email': EMAIL})
            db.session.commit()

    def authority_snapshot(self):
        with self.app.app_context():
            return tuple(tuple(db.session.execute(text(
                'SELECT row_to_json(t)::text FROM ' + name + ' t ORDER BY 1')).scalars())
                for name in ('auth_subject_admin', 'sace_reading_engagement',
                    'sace_reading_controller_appointment', 'sace_reading_assignment_context',
                    'sace_workshop_interactions', 'sace_home_controller_appointment',
                    'sace_home_assignment', 'sace_home_evidence'))

    def assert_platform_bridge(self, path='/bridge'):
        from flask import template_rendered
        rendered = []
        def capture(sender, template, context, **kwargs):
            rendered.append((template.name, context))
        with template_rendered.connected_to(capture, self.app):
            page = self.client.get(path, follow_redirects=True)
        self.assertEqual(page.status_code, 200)
        self.assertEqual(page.request.path, '/bridge')
        self.assertLessEqual(len(page.history), 1)
        self.assertNotIn(b'Choose SACE Activity', page.data)
        name, context = rendered[-1]
        self.assertEqual(name, 'auth/bridge_dashboard.html')
        self.assertTrue(context['platform_admin_bridge'])
        with self.app.app_context():
            expected = {s.slug for s in f.h.auth_models.AuthSubject.query.filter_by(is_active=1)
                        if not s.is_hidden_on_bridge}
        self.assertEqual({s['slug'] for s in context['subjects']}, expected | {'spv'})
        spv_tiles = [s for s in context['subjects'] if s['slug'] == 'spv']
        self.assertEqual(len(spv_tiles), 1)
        self.assertEqual(spv_tiles[0]['name'], 'SPV')
        self.assertEqual(spv_tiles[0]['subject']['description'], 'Special Purpose Vehicle')
        self.assertEqual(spv_tiles[0]['href'], '/admin/spv/')
        for tile in context['subjects']:
            self.assertEqual(tile['access_level'], 'admin')
            if tile['slug'] not in ('admin_general', 'staff'):
                self.assertIn(tile['name'].encode(), page.data)
        return page

    def test_routing_spv_uses_real_subject_placeholder(self):
        from flask import url_for
        from sqlalchemy import event
        self.officials('R', None)
        self.approve_platform_admin()
        self.assert_platform_bridge(self.login(self.client, EMAIL).location)
        before = self.authority_snapshot()
        counts = self.counts()
        self.assertNotIn('spv_admin_bp.spv_dashboard', self.app.view_functions)
        with self.app.test_request_context():
            self.assertEqual(url_for('admin_bp.subject_dashboard', subject='spv'), '/admin/spv/')
        # Legacy SPV metadata must neither duplicate the tile nor choose its destination.
        for visibility in (None, False, True):
            with self.subTest(hidden=visibility):
                with self.app.app_context():
                    if visibility is not None:
                        subject = f.h.auth_models.AuthSubject.query.filter_by(slug='spv').first()
                        if subject is None:
                            subject = f.h.auth_models.AuthSubject(id=906, slug='spv', name='Legacy SPV',
                                is_active=1, start_endpoint='spv_bp.about')
                            db.session.add(subject)
                        subject.is_hidden_on_bridge = visibility
                        db.session.commit()
                page = self.assert_platform_bridge()
                self.assertEqual(page.data.count(b'Special Purpose Vehicle'), 1)
                self.assertIn(b'href="/admin/spv/"', page.data)
                # The real handler must not inspect the generic subject/start endpoint.
                def reject_subject_query(conn, cursor, statement, parameters, context, executemany):
                    if 'auth_subject' in statement.lower():
                        raise AssertionError('SPV placeholder queried generic subject metadata')
                with self.app.app_context():
                    engine = db.engine
                event.listen(engine, 'before_cursor_execute', reject_subject_query)
                try:
                    result = self.client.get('/admin/spv/')
                finally:
                    event.remove(engine, 'before_cursor_execute', reject_subject_query)
                self.assertEqual(result.status_code, 200)
                for text in (b'SPV', b'Special Purpose Vehicle',
                        b'This programme is being prepared for reintroduction.', b'Back to Bridge'):
                    self.assertIn(text, result.data)
                self.assertIn(b'href="/bridge"', result.data)
                self.assertNotIn(b'<form', result.data)
                self.assertNotRegex(result.data.lower(), rb'investment|checkout|paystack|payment|pledge|spv_bp|spv_admin_bp')
                self.assertEqual(before, self.authority_snapshot())
                self.assertEqual(counts, self.counts())

    def test_routing_platform_reading_r_login_exit_and_reentry(self):
        import re
        from app.program_sace.lifecycle import controller
        self.officials('R', None)
        self.approve_platform_admin()
        with self.app.app_context():
            models = f.h.auth_models
            db.session.add(models.AuthSubject(id=903, slug='budget', name='Budget', is_active=1))
            db.session.add(models.AuthSubject(id=904, slug='hidden-test', name='Hidden test',
                                             is_active=1, is_hidden_on_bridge=True))
            db.session.add(models.AuthSubject(id=905, slug='inactive-test', name='Inactive test', is_active=0))
            db.session.commit()
            appointment_id = controller(models.User.query.filter_by(email=EMAIL).one()).id
        before = self.authority_snapshot()
        counts = self.counts()
        login = self.login(self.client, EMAIL)
        self.assertEqual(login.location, '/dashboard')
        self.assert_platform_bridge(login.location)
        self.assert_platform_bridge('/dashboard')
        self.assert_platform_bridge()
        self.assertEqual(counts, self.counts())
        result = self.client.get('/sace/dashboard', follow_redirects=True)
        self.assertEqual(result.request.path, '/sace/provisioning')
        self.assertIn(b'SACE Control Centre', result.data)
        exit_link = re.search(rb'<a href="([^"]+)"[^>]*>\s*<i[^>]*></i> Exit', result.data)
        self.assertIsNotNone(exit_link)
        self.assertEqual(exit_link.group(1), b'/bridge')
        self.assert_platform_bridge(exit_link.group(1).decode())
        result = self.client.get('/sace/dashboard', follow_redirects=True)
        self.assertIn(b'SACE Control Centre', result.data)
        with self.app.app_context():
            appointment = controller(models.User.query.filter_by(email=EMAIL).one())
            self.assertEqual(appointment.id, appointment_id)
            self.assertEqual(appointment.status, 'active')
        self.assertEqual(before, self.authority_snapshot())
        self.assertEqual(counts, self.counts())
        self.client.get('/logout')
        self.assertEqual(self.login(self.client, EMAIL, '/sace/dashboard').location, '/sace/provisioning')

    def test_routing_platform_residual_continuations_do_not_capture(self):
        self.officials('R', 'R')
        self.approve_platform_admin()
        self.third()
        with self.app.app_context():
            uid = f.h.auth_models.User.query.filter_by(email=EMAIL).one().id
        before = self.authority_snapshot()
        for target in (None, '/dashboard', '/bridge'):
            self.client.get('/logout')
            # A genuine accepted Reading provisioning continuation, plus a
            # validated third activity continuation and residual HOME state.
            f.h.AccessJourneys.pending_r(self, self.client)
            with self.client.session_transaction() as state:
                state['test_third_continuation'] = {'user_id': uid, 'expires_at': time.time()+900}
                state[home_intent.KEY] = {'kind': 'join', 'expires_at': time.time()+900,
                                         'pending_hash': home.digest('residual')}
                state['sace_home_pending_code'] = 'residual'
            response = self.login(self.client, EMAIL, target)
            self.assertEqual(response.location, target or '/dashboard')
            self.assert_platform_bridge(response.location)
            self.assert_platform_bridge('/dashboard')
            self.assert_platform_bridge()
        self.assertEqual(before, self.authority_snapshot())
        self.assertEqual(self.client.get('/sace/home/', follow_redirects=True).request.path, '/sace/home/control')

    def test_routing_platform_valid_home_continuation_does_not_capture(self):
        self.officials('R', None)
        self.approve_platform_admin()
        saved = self.client
        self.client = self.app.test_client()
        try:
            self.provision_home('home-provider@example.test')
            code = self.code_home()
        finally:
            self.client = saved
        for target in (None, '/dashboard', '/bridge'):
            self.client.get('/logout')
            self.assertEqual(self.client.post('/sace/home/join', data={'code': code}).location,
                             '/sace/home/pledge')
            self.client.post('/sace/home/pledge', data={'accept': 'yes'})
            # Genuine, current consent-backed HOME intent. Explicit platform
            # next URLs may discard it under HOME's existing abandonment rule.
            with self.client.session_transaction() as state:
                self.assertIn(home_intent.KEY, state)
                self.assertIn('sace_home_pledge_auditor', state)
            before = self.authority_snapshot()
            response = self.login(self.client, EMAIL, target)
            self.assertEqual(response.location, target or '/dashboard')
            if target is None:
                with self.client.session_transaction() as state:
                    self.assertIn(home_intent.KEY, state)
            self.assert_platform_bridge(response.location)
            self.assertEqual(before, self.authority_snapshot())

    def test_routing_platform_reading_a_keeps_operational_authority(self):
        self.officials('A', 'R')
        self.approve_platform_admin()
        before = self.authority_snapshot()
        self.assert_platform_bridge(self.login(self.client, EMAIL).location)
        self.assertEqual(self.client.get('/sace/dashboard', follow_redirects=True).request.path, '/sace/reading')
        self.assertEqual(self.client.get('/sace/home/', follow_redirects=True).request.path, '/sace/home/control')
        self.assertEqual(before, self.authority_snapshot())

    def test_routing_subject_admin_and_session_flags_are_not_platform_identity(self):
        self.officials('R', None)
        with self.app.app_context():
            db.session.add(f.h.auth_models.AuthSubjectAdmin(subject_id=44, email=EMAIL))
            db.session.commit()
        self.assertEqual(self.login(self.client, EMAIL).location, '/sace/provisioning')
        with self.client.session_transaction() as state:
            state['is_admin'] = True
            state['role'] = 'admin'
        for path in ('/dashboard', '/bridge'):
            self.assertEqual(self.client.get(path).location, '/sace/provisioning')
        import re
        result = self.client.get('/sace/provisioning')
        self.assertRegex(result.data, rb'<a href="/"[^>]*>\s*<i[^>]*></i> Exit')

    def test_routing_central_management_does_not_begin_operational_onboarding(self):
        self.officials(None, None)
        self.approve_platform_admin()
        self.login(self.client, EMAIL)
        before = self.authority_snapshot()
        counts = self.counts()
        for path in ('/admin/security/sace-management', '/admin/security/sace-management?program=home'):
            result = self.client.get(path)
            self.assertEqual(result.status_code, 200)
            self.assertNotIn(b'Open SACE Provisioning', result.data)
            self.assertNotIn(b'href="/sace/provisioning', result.data)
            with self.client.session_transaction() as state:
                self.assertNotIn(access.PROVISIONING_KEY, state)
                self.assertNotIn(home_intent.KEY, state)
            self.assertEqual(before, self.authority_snapshot())
            self.assertEqual(counts, self.counts())
        self.assertEqual(self.client.get('/sace/provisioning').status_code, 200)
        with self.client.session_transaction() as state:
            self.assertIn(access.PROVISIONING_KEY, state)
        # Database-approved platform identity does not prohibit deliberate
        # legitimate operational provisioning through its dedicated entry.
        self.client.get('/logout')
        f.h.AccessJourneys.provision(self, self.client, EMAIL, existing=True)
        self.assertIn(b'SACE Control Centre', self.client.get('/sace/provisioning').data)

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
