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
        with self.app.app_context():
            identity = f.h.auth_models.User.query.filter_by(email=EMAIL).one()
            identity.name = 'Appointed Controller Example'
            db.session.commit()
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
        self.assertIn(b'Appointed Controller Example', page.data)
        if reading_role == home_role == 'R':
            self.assertIn(b'SACE controller: Appointed Controller Example', page.data)
            authority = self.authority_snapshot()
            for control in ('/sace/provisioning', '/sace/home/control'):
                centre = self.client.get(control)
                self.assertEqual(centre.status_code, 200)
                self.assertIn(b'href="/sace/activities"', centre.data)
                self.assertIn(b'Back to Choose SACE Activity', centre.data)
                chooser = self.client.get('/sace/activities')
                self.assertEqual(chooser.status_code, 200)
                self.assertIn(b'SACE controller: Appointed Controller Example', chooser.data)
                self.assertIn(b'href="/sace/provisioning"', chooser.data)
                self.assertIn(b'href="/sace/home/control"', chooser.data)
            self.assertEqual(authority, self.authority_snapshot())
        elif reading_role == home_role == 'A':
            self.assertNotIn(b'SACE controller:', page.data)
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

    def test_routing_submitted_auditors_remain_until_finalized(self):
        from unittest.mock import patch
        self.officials('A','A')
        self.login(self.client,EMAIL)
        with patch.object(reading,'completion_requirements',return_value=[]), patch.object(home,'missing',return_value=[]):
            self.assertEqual(self.client.post('/sace/reading/finish-evaluation').status_code,200)
            self.assertEqual(self.client.post(f'/sace/home/assignments/{self.home_assignment}/completion').status_code,200)
        for entry in ('/dashboard','/bridge','/sace/activities'):
            response=self.client.get(entry,follow_redirects=True)
            self.assertEqual(response.status_code,200)
            self.assertIn(b'Choose SACE Activity',response.data)
            self.assertIn(b'href="/sace/reading"',response.data)
            self.assertIn(b'href="/sace/home/"',response.data)
        self.client.get('/logout')
        self.assertIn(b'Choose SACE Activity',self.login(self.client,EMAIL).data)
        self.assertEqual(self.reading_provider.post(f'/sace/provisioning/assignments/{self.reading_assignment}/finalize').status_code,302)
        page=self.client.get('/sace/activities')
        self.assertNotIn(b'href="/sace/reading"',page.data)
        self.assertIn(b'href="/sace/home/"',page.data)

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
        self.assertIn(b'Admin Hub', page.data)
        for label, href in ((b'General', b'/admin/settings'),
                (b'Programs', b'/admin/programs/'), (b'Security', b'/admin/security'),
                (b'API', b'/admin/api/')):
            self.assertIn(label, page.data)
            self.assertIn(b'href="' + href + b'"', page.data)
        self.assertNotIn(b'SACE Control Centre', page.data)
        self.assertNotIn(b'Auditor Board', page.data)
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
                self.assertNotIn(b'href="' + tile['href'].encode() + b'"', page.data)
        return page

    def test_routing_retire_san_only_preserves_platform_and_r_a(self):
        import json
        import re
        from unittest.mock import patch
        from flask_wtf.csrf import generate_csrf
        from app.program_sace import lifecycle as lc
        models = f.h.auth_models
        with self.app.app_context():
            # Match the reviewed production identities using temporary tables only.
            db.session.execute(text('DELETE FROM auth_subject WHERE id = 900'))
            db.session.execute(text("UPDATE auth_subject SET slug='sace_endorsement' WHERE id=44"))
            db.session.execute(text('UPDATE auth_subject SET id=48 WHERE id=901'))
            for uid, email, name in ((1, 'san@gmail.com', 'San'),
                    (631, 'ren@gmail.com', 'R'), (632, 'nan@gmail.com', 'A')):
                user = models.User(id=uid, email=email, name=name, is_active=1)
                user.set_password('test-password')
                db.session.add(user)
            db.session.execute(text("INSERT INTO auth_approved_admin (id, email, active) VALUES (1, 'san@gmail.com', 1)"))
            db.session.commit()

        def next_id(table, value):
            with self.app.app_context():
                # Never reset public sequences: all fixtures are connection-local.
                db.session.execute(text("SELECT setval(pg_get_serial_sequence(:table, 'id'), :value, false)"),
                                   {'table': 'pg_temp.' + table, 'value': value})
                db.session.commit()

        r_client = self.client
        next_id('auth_subject_admin', 5)
        self.provision_home('ren@gmail.com')
        r_client.get('/logout')
        next_id('auth_subject_admin', 4)
        next_id('sace_reading_engagement', 1)
        next_id('sace_reading_controller_appointment', 1)
        next_id('sace_workshop_interactions', 306)
        f.h.AccessJourneys.provision(self, r_client, 'ren@gmail.com', existing=True)
        next_id('sace_workshop_interactions', 311)
        code, invitation_id = f.h.AccessJourneys.code(self, r_client)
        self.assertEqual(invitation_id, 311)
        auditor = self.app.test_client()
        auditor.post('/sace/join', data={'code': code})
        auditor.post('/sace/auditor_pledge')
        login = self.login(auditor, 'nan@gmail.com', '/sace/claim_code')
        self.assertEqual(auditor.get(login.location, follow_redirects=True).status_code, 200)

        san = self.app.test_client()
        next_id('auth_subject_admin', 6)
        next_id('sace_reading_engagement', 2)
        next_id('sace_reading_controller_appointment', 2)
        next_id('sace_workshop_interactions', 432)
        f.h.AccessJourneys.provision(self, san, 'san@gmail.com', existing=True)
        self.client = san
        self.assert_platform_bridge('/dashboard')
        with self.app.app_context():
            self.assertEqual(lc.controller(db.session.get(models.User, 1)).id, 2)
            self.assertEqual(db.session.get(lc.Appointment, 2).operational_grant_id, 6)
            self.assertEqual(db.session.get(lc.Appointment, 1).operational_grant_id, 4)
            last_event = db.session.execute(text('SELECT max(id) FROM sace_workshop_interactions')).scalar_one()

        def protected_snapshot():
            filters = {
                'auth_subject_admin': ' WHERE id <> 6',
                'sace_reading_controller_appointment': ' WHERE id <> 2',
                'sace_reading_engagement': ' WHERE id <> 2',
                'sace_workshop_interactions': ' WHERE id <= :last_event',
            }
            with self.app.app_context():
                return {table: tuple(db.session.execute(text(
                    'SELECT row_to_json(t)::text FROM pg_temp."' + table + '" t' +
                    filters.get(table, '') + ' ORDER BY 1'), {'last_event': last_event}).scalars())
                    for table in self.tables}

        before = protected_snapshot()
        reason = 'Correct mistaken platform-admin Reading provisioning; San is not a SACE official.'
        # Exercise the existing HTTP revocation with real CSRF protection.
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            with patch.dict(self.app.jinja_env.globals, csrf_token=generate_csrf):
                page = san.get('/sace/provisioning/lifecycle')
                self.assertEqual(page.status_code, 200)
                token = re.search(rb'name="csrf_token" value="([^"]+)"', page.data).group(1).decode()
                self.assertEqual(san.post('/sace/provisioning/engagement/end',
                    data={'status': 'revoked', 'reason': reason}).status_code, 400)
                self.assertEqual(before, protected_snapshot())
                response = san.post('/sace/provisioning/engagement/end',
                    data={'csrf_token': token, 'status': 'revoked', 'reason': reason})
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/dashboard')
        self.assertEqual(before, protected_snapshot())
        with self.app.app_context():
            appointment = db.session.get(lc.Appointment, 2)
            engagement = db.session.get(lc.Engagement, 2)
            for row in (appointment, engagement):
                self.assertEqual(row.status, 'revoked')
                self.assertEqual(row.ended_by_user_id, 1)
                self.assertEqual(row.end_reason, reason)
                self.assertIsNotNone(row.revoked_at)
            self.assertIsNone(appointment.operational_grant_id)
            self.assertEqual(appointment.grant_id_at_issue, 6)
            self.assertIsNone(db.session.get(models.AuthSubjectAdmin, 6))
            self.assertEqual(db.session.get(models.AuthSubjectAdmin, 4).email, 'ren@gmail.com')
            self.assertEqual(db.session.get(models.AuthSubjectAdmin, 5).email, 'ren@gmail.com')
            self.assertEqual(db.session.get(models.ApprovedAdmin, 1).active, 1)
            user = db.session.get(models.User, 1)
            self.assertIsNone(lc.controller(user))
            self.assertEqual(reading.assignments(user.id, active_only=True), [])
            audits = f.h.Interaction.query.filter(f.h.Interaction.id > last_event).all()
            self.assertEqual({r.activity_slug for r in audits},
                             {'controller_appointment_ended', 'reading_engagement_ended'})
            self.assertTrue(all(r.user_id == 1 and r.workshop_session_id == 'reading-engagement-2' for r in audits))
            retired = next(r for r in audits if r.activity_slug == 'controller_appointment_ended')
            self.assertEqual(json.loads(retired.response_data)['retired_grant_id'], 6)

        self.assert_platform_bridge(response.location)
        san.get('/logout')
        self.assert_platform_bridge(self.login(san, 'san@gmail.com').location)
        self.assertEqual(san.get('/sace/reading').status_code, 403)
        self.assertEqual(san.post('/sace/provisioning/generate_code').status_code, 403)
        self.assertEqual(san.post('/sace/provisioning/engagement/end',
                                 data={'status': 'revoked', 'reason': reason}).status_code, 403)
        self.assertIn(b'SACE Control Centre', r_client.get('/sace/provisioning').data)
        self.assertIn(b'Auditor Board', auditor.get('/sace/reading').data)
        self.assertEqual(auditor.get('/sace/provisioning/lifecycle').status_code, 403)
        self.assertEqual(r_client.post('/sace/provisioning/appointments/2/end',
            data={'status': 'revoked', 'reason': 'Wrong engagement'}).status_code, 404)
        self.assertEqual(before, protected_snapshot())

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
                self.assertEqual(page.data.count(b'Special Purpose Vehicle'), 0)
                self.assertNotIn(b'href="/admin/spv/"', page.data)
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

    def test_routing_explicit_activity_bridge_requires_existing_authority(self):
        self.assertEqual(self.client.get('/sace/activities').status_code,302)
        self.user(EMAIL)
        self.login(self.client,EMAIL)
        before=self.counts()
        self.assertEqual(self.client.get('/sace/activities').status_code,403)
        self.assertEqual(before,self.counts())

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

    def test_routing_production_632_auditor_ignores_stale_controller_context(self):
        import json
        from app.program_sace import lifecycle as lc
        models = f.h.auth_models
        with self.app.app_context():
            db.session.execute(text('DELETE FROM auth_subject WHERE id=900'))
            db.session.execute(text("UPDATE auth_subject SET slug='sace_endorsement' WHERE id=44"))
            for uid, email in ((631, 'ren@gmail.com'), (632, 'nan@gmail.com')):
                user = models.User(id=uid, email=email, name='R' if uid==631 else 'A', is_active=1)
                user.set_password('test-password'); db.session.add(user)
            db.session.commit()
        with self.app.app_context():
            for table, value in (('auth_subject_admin',4), ('sace_reading_engagement',1), ('sace_reading_controller_appointment',1)):
                db.session.execute(text("SELECT setval(pg_get_serial_sequence(:table,'id'),:value,false)"),
                    {'table':'pg_temp.'+table,'value':value})
            db.session.commit()
        provider=self.app.test_client()
        f.h.AccessJourneys.provision(self,provider,'ren@gmail.com',existing=True)
        with self.app.app_context():
            db.session.get(lc.Engagement,1).reference='LITRE-AA6A3C3EA79027FF'
            db.session.commit()
        with self.app.app_context():
            db.session.execute(text("SELECT setval(pg_get_serial_sequence('pg_temp.sace_workshop_interactions','id'),311,false)"))
            db.session.commit()
        code, aid=f.h.AccessJourneys.code(self,provider)
        self.assertEqual(aid,311)
        self.client.post('/sace/join',data={'code':code}); self.client.post('/sace/auditor_pledge')
        result=self.login(self.client,'nan@gmail.com','/sace/claim_code')
        self.client.get(result.location,follow_redirects=True)
        with self.app.app_context():
            enrollment=models.UserEnrollment(id=1031,user_id=632,subject_id=44,status='active',country_code='ZA',local_currency='ZAR',local_amount_cents=0,zar_amount_cents=0)
            db.session.add(enrollment)
            db.session.commit()
            self.assertEqual(enrollment.status,'active')
            self.assertIsNone(lc.controller(db.session.get(models.User,632)))
            self.assertEqual(models.AuthSubjectAdmin.query.filter_by(email='nan@gmail.com').count(),0)
            self.assertEqual(lc.Appointment.query.filter_by(user_id=632).count(),0)
            self.assertEqual(reading.payload(db.session.get(f.h.Interaction,311))['claimed_by_user_id'],632)
        self.client.get('/logout')
        with self.client.session_transaction() as state:
            state[access.PROVISIONING_KEY]={'nonce':'stale-provider-context','started_at':time.time(),'accepted_at':time.time(),'user_id':None}
            state['sace_admin_provisioning']=True; state['sace_admin_pledged']=True
            state['admin_subjects']=['sace_endorsement']; state['role']='admin'
        self.assertEqual(self.login(self.client,'nan@gmail.com').location,'/sace/reading')
        for path in ('/dashboard','/bridge','/sace/dashboard'):
            self.assertEqual(self.client.get(path).location,'/sace/reading')
        selector=self.client.get('/sace/activities')
        self.assertNotIn(b'SACE controller:',selector.data)
        self.assertNotIn(b'/sace/provisioning',selector.data)
        self.assertIn(b'Auditor Board',self.client.get('/sace/reading').data)
        self.assertIn(b'href="/sace/activities">Back</a>',self.client.get('/sace/reading').data)
        self.assertEqual(self.client.get('/sace/provisioning').location,'/sace/reading')
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code,403)
        with self.app.app_context():
            self.assertEqual(lc.Appointment.query.filter_by(user_id=632).count(),0)
            self.assertEqual(models.AuthSubjectAdmin.query.filter_by(email='nan@gmail.com').count(),0)
        with self.client.session_transaction() as state:
            self.assertNotIn(access.PROVISIONING_KEY,state)
        self.client.get('/logout')
        self.assertEqual(self.login(self.client,'ren@gmail.com').location,'/sace/provisioning')
        self.assertIn(b'SACE Control Centre',self.client.get('/sace/provisioning').data)

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
