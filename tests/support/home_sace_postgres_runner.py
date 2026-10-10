"""Focused HOME HTTP checks in temporary PostgreSQL tables; no application startup.
Migration upgrade/downgrade is exercised in a transactionally rolled-back private schema.
"""
import importlib.util
import re
import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from sqlalchemy import text, create_engine, inspect
from sqlalchemy.schema import CreateTable, CreateIndex, AddConstraint
from alembic.migration import MigrationContext
from alembic.operations import Operations

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("sace_test_support", ROOT / "tests/support/sace_access_postgres_runner.py")
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
from app.program_sace_home import home_sace_bp, service as s
from app.models.sace_home import (HomeController, HomeProvisioning, HomeInvitation, HomePledge,
    HomeAssignment, HomeDocument, HomeDocumentVersion, HomeEvidence, now)
from app.extensions import db

HOME_TABLES = [t for t in db.metadata.sorted_tables if t.name.startswith("sace_home_")]


class HomeFoundation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h.AccessJourneys.setUpClass.__func__(cls)
        cls.app.register_blueprint(home_sace_bp)
        with cls.app.app_context():
            with db.engine.begin() as conn:
                conn.execute(text('ALTER TABLE pg_temp."user" ADD PRIMARY KEY (id)'))
                conn.execute(text('ALTER TABLE pg_temp."user" ADD UNIQUE (email)'))
                for table in HOME_TABLES:
                    ddl = str(CreateTable(table).compile(dialect=conn.dialect))
                    conn.execute(text(ddl.replace("CREATE TABLE", "CREATE TEMPORARY TABLE", 1)))
                    for index in table.indexes:
                        conn.execute(CreateIndex(index))
                for table in HOME_TABLES:
                    for constraint in table.foreign_key_constraints:
                        if constraint.use_alter:
                            conn.execute(AddConstraint(constraint))
        cls.tables += [t.name for t in HOME_TABLES]
        cls.documents = tempfile.TemporaryDirectory(prefix="home-sace-documents-")
        cls.app.config["SACE_HOME_DOCUMENT_ROOT"] = cls.documents.name
        # These frozen foundation/Phase 2A journeys exercise historical requirements.
        # Phase 2B has its own suite with the new requirements explicitly selected.
        cls.app.config['SACE_HOME_REQUIREMENTS_VERSION'] = 'home-foundation-v1'

    @classmethod
    def tearDownClass(cls):
        cls.documents.cleanup()
        h.AccessJourneys.tearDownClass.__func__(cls)

    def setUp(self):
        # The invitation/pledge/appointment cycle is intentional; clear only our
        # connection-local HOME fixtures together before the shared harness reset.
        with self.app.app_context():
            db.session.execute(text('TRUNCATE ' + ', '.join('pg_temp.' + t.name for t in HOME_TABLES)))
            db.session.commit()
        h.AccessJourneys.setUp(self)
        with self.app.app_context():
            db.session.execute(text("INSERT INTO auth_subject (id, slug, name, is_active) VALUES (901, 'sace_home_endorsement', 'HOME SACE Endorsement', 1)"))
            db.session.commit()
    user = h.AccessJourneys.user
    login = h.AccessJourneys.login
    pledge = h.AccessJourneys.pledge

    def provision_home(self, email="home-r@example.test"):
        with self.app.app_context():
            token = s.issue_provisioning(email, "local-test")
            db.session.commit()
        self.assertEqual(self.client.get("/sace/home/provisioning?token=" + token, follow_redirects=True).status_code, 200)
        self.assertEqual(self.client.post("/sace/home/provisioning", data={}).status_code, 302)
        result = self.client.post("/register", data={"subject": s.SUBJECT, "full_name": "HOME R",
            "email": email, "password": "test-password"}, follow_redirects=True)
        self.assertEqual(result.status_code, 200, result.data[:500])
        self.assertIn(b"HOME Control Centre", result.data)
        with self.app.app_context():
            return HomeController.query.one().user_id

    def code_home(self):
        result = self.client.post("/sace/home/control/codes")
        self.assertEqual(result.status_code, 200)
        return re.search(rb"HOME-[A-F0-9]{24}", result.data).group().decode()

    def join_home(self, code, email="home-a@example.test", existing=False):
        client = self.app.test_client()
        self.assertEqual(client.post("/sace/home/join", data={"code": code}).location, "/sace/home/pledge")
        self.assertEqual(client.get("/sace/home/pledge").status_code, 200)
        self.assertEqual(client.post("/sace/home/pledge", data={"accept": "yes"}).status_code, 302)
        if existing:
            result = self.login(client, email, "/sace/home/claim")
            self.assertEqual(result.location, "/sace/home/claim")
            result = client.get(result.location, follow_redirects=True)
        else:
            result = client.post("/register", data={"subject": s.SUBJECT, "full_name": "HOME A",
                "email": email, "password": "test-password"}, follow_redirects=True)
        self.assertEqual(result.status_code, 200, result.data[:500])
        self.assertIn(b"HOME Auditor Board", result.data)
        with self.app.app_context():
            row = HomeAssignment.query.order_by(HomeAssignment.id.desc()).first()
            return client, row.id

    def direct_home_entry(self, client=None):
        client = client or self.client
        response = client.get('/sace/home/provisioning')
        self.assertEqual(response.status_code,302)
        with client.session_transaction() as session:
            nonce=session['sace_home_provisioning_context']['nonce']
        page=client.get(response.location)
        self.assertIn(b'HOME Controller IP Pledge',page.data)
        return nonce

    def test_simplified_controller_form_with_real_csrf(self):
        from flask_wtf.csrf import generate_csrf
        original = self.app.jinja_env.globals['csrf_token']
        self.app.jinja_env.globals['csrf_token'] = generate_csrf
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            nonce = self.direct_home_entry()
            url = '/sace/home/provisioning?journey=' + nonce
            response = self.client.get(url)
            self.assertNotIn(b'name="signature"', response.data)
            self.assertNotIn(b'name="accept"', response.data)
            self.assertNotIn(b'This HOME provisioning invitation is for', response.data)
            self.assertIn(b'Accept and Continue', response.data)
            self.assertNotIn(b'href="/sace/home/"', response.data)
            self.assertEqual(response.data.count(b'Accept and Continue'), 1)
            self.assertLess(response.data.index(b'Accept and Continue'),
                response.data.index(s.PLEDGE_TEXT.encode()))
            fields = dict((a.decode(), b.decode()) for a, b in re.findall(
                rb'<input type="hidden" name="([^"]+)" value="([^"]*)">', response.data))
            self.assertEqual(fields['journey'], nonce)
            self.assertEqual(self.client.post(url, data={'journey': nonce}).status_code, 400)
            forged = dict(fields, journey='forged')
            self.assertEqual(self.client.post(url, data=forged).status_code, 403)
            response = self.client.post(url, data=fields)
            self.assertEqual(response.location, '/sace/home/authenticate')
            with self.client.session_transaction() as state:
                consent = state['sace_home_pledge_controller']
                self.assertEqual(consent['context_id'], nonce)
                self.assertEqual(consent['acceptance_method'], 'accept_and_continue')
                self.assertEqual(consent['version'], s.PLEDGE_VERSION)
                self.assertTrue(consent['accepted_at'])
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.app.jinja_env.globals['csrf_token'] = original

    def test_https_provisioning_missing_referrer_diagnostic(self):
        from flask_wtf.csrf import generate_csrf
        original = self.app.jinja_env.globals['csrf_token']
        self.app.jinja_env.globals['csrf_token'] = generate_csrf
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            origin = 'https://localhost'
            response = self.client.get('/sace/home/provisioning', base_url=origin)
            response = self.client.get(response.location, base_url=origin)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['Referrer-Policy'], 'same-origin')
            fields = dict((a.decode(), b.decode()) for a, b in re.findall(
                rb'<input type="hidden" name="([^"]+)" value="([^"]*)">', response.data))
            url = '/sace/home/provisioning?journey=' + fields['journey']
            with self.assertLogs(self.app.logger, level='INFO') as logs:
                rejected = self.client.post(url, data=fields, base_url=origin)
            self.assertEqual(rejected.status_code, 400)
            self.assertEqual(len(rejected.data), 122)
            self.assertIn(b'The referrer header is missing.', rejected.data)
            messages = '\n'.join(logs.output)
            self.assertIn('HOME provisioning rejected: CSRF (referrer missing)', messages)
            self.assertNotIn('HOME provisioning POST entered', messages)
            for value in fields.values():
                self.assertNotIn(value, messages)
            with self.assertLogs(self.app.logger, level='INFO') as logs:
                rejected = self.client.post(url, data=fields, base_url=origin,
                    headers={'Referer': 'https://other.example/'})
            self.assertEqual(rejected.status_code, 400)
            self.assertIn(b'The referrer does not match the host.', rejected.data)
            messages = '\n'.join(logs.output)
            self.assertIn('HOME provisioning rejected: CSRF (referrer/host mismatch)', messages)
            self.assertNotIn('HOME provisioning POST entered', messages)
            for value in fields.values():
                self.assertNotIn(value, messages)
            with self.assertLogs(self.app.logger, level='INFO') as logs:
                accepted = self.client.post(url, data=fields, base_url=origin,
                    headers={'Referer': origin + url})
            self.assertEqual(accepted.status_code, 302)
            self.assertEqual(accepted.location, '/sace/home/authenticate')
            self.assertIn('HOME provisioning POST entered', '\n'.join(logs.output))
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.app.jinja_env.globals['csrf_token'] = original

    def test_https_control_code_generation_referrer_policy(self):
        from flask_wtf.csrf import generate_csrf
        self.provision_home()
        original = self.app.jinja_env.globals['csrf_token']
        self.app.jinja_env.globals['csrf_token'] = generate_csrf
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            origin = 'https://localhost'
            page = self.client.get('/sace/home/control', base_url=origin)
            self.assertEqual(page.status_code, 200)
            self.assertEqual(page.headers['Referrer-Policy'], 'same-origin')
            token = re.search(rb'name="csrf_token" value="([^"]+)"', page.data).group(1).decode()
            target = '/sace/home/control/codes'
            for headers, reason in (({}, b'The referrer header is missing.'),
                    ({'Referer': 'https://foreign.example/'}, b'The referrer does not match the host.')):
                rejected = self.client.post(target, base_url=origin,
                    data={'csrf_token': token}, headers=headers)
                self.assertEqual(rejected.status_code, 400)
                self.assertIn(reason, rejected.data)
            with self.app.app_context():
                self.assertEqual(HomeInvitation.query.count(), 0)
            accepted = self.client.post(target, base_url=origin,
                data={'csrf_token': token}, headers={'Referer': origin + '/sace/home/control'})
            self.assertEqual(accepted.status_code, 200)
            self.assertRegex(accepted.data, rb'HOME-[A-F0-9]{24}')
            with self.app.app_context():
                self.assertEqual(HomeInvitation.query.count(), 1)
            for path in ('/sace/home/control/documents', '/sace/home/control/completion',
                         '/sace/home/ip-pledge'):
                response = self.client.get(path, base_url=origin)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers['Referrer-Policy'], 'same-origin')
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.app.jinja_env.globals['csrf_token'] = original
        code = re.search(rb'HOME-[A-F0-9]{24}', accepted.data).group().decode()
        auditor, aid = self.join_home(code)
        for path in ('/sace/home/join',
                     f'/sace/home/assignments/{aid}/board',
                     f'/sace/home/assignments/{aid}/summary',
                     f'/sace/home/assignments/{aid}/completion'):
            response = auditor.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['Referrer-Policy'], 'same-origin')

    def test_standard_url_new_r_ordinary_login_and_home_auditor(self):
        nonce=self.direct_home_entry()
        with self.app.app_context():
            self.assertEqual(HomeProvisioning.query.count(),0)
            self.assertEqual(HomeController.query.count(),0)
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.count(),0)
        self.assertEqual(self.client.post('/sace/home/provisioning',data={'signature':'R','accept':'yes'}).status_code,403)
        self.assertEqual(self.client.post('/sace/home/provisioning?journey='+nonce,data={'journey':nonce}).location,'/sace/home/authenticate')
        result=self.client.post('/register',data={'subject':s.SUBJECT,'full_name':'HOME R','email':'direct-r@example.test','password':'test-password'},follow_redirects=True)
        self.assertEqual(result.status_code,200,result.data[:500])
        self.assertIn(b'HOME Control Centre',result.data)
        with self.app.app_context():
            row=HomeProvisioning.query.one()
            self.assertEqual(row.email,'direct-r@example.test')
            self.assertIsNotNone(row.claimed_at)
            self.assertEqual(HomePledge.query.one().provisioning_id,row.id)
            self.assertEqual(HomePledge.query.one().signature, 'HOME R')
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.filter_by(subject_id=901).count(),1)
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.filter_by(subject_id=900).count(),0)
            self.assertEqual(h.Interaction.query.count(),0)
        self.client.get('/logout')
        self.assertEqual(self.login(self.client,'direct-r@example.test').location,'/sace/home/control')
        self.assertEqual(self.client.get('/sace/home/provisioning').location,'/sace/home/control')
        auditor,aid=self.join_home(self.code_home())
        self.assertEqual(auditor.get('/sace/home/provisioning').status_code,403)
        self.assertEqual(auditor.get('/sace/home/control').status_code,403)
        with self.app.app_context():
            user=db.session.get(HomeAssignment,aid).auditor_id
            self.assertIsNone(HomeController.query.filter_by(user_id=user).first())
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.filter_by(email='home-a@example.test').count(),0)

    def test_session_provisioning_rejects_forgery_expiry_replay_and_abandonment(self):
        nonce=self.direct_home_entry()
        fresh=self.app.test_client()
        self.assertEqual(fresh.get('/sace/home/provisioning?journey='+nonce).status_code,403)
        self.assertEqual(fresh.post('/sace/home/provisioning',data={'journey':nonce}).status_code,403)
        self.assertEqual(self.client.post('/sace/home/provisioning',data={'journey':'wrong'}).status_code,403)
        self.assertEqual(self.client.get('/register?subject='+s.SUBJECT).status_code,400)
        with self.client.session_transaction() as session:
            context=dict(session[s.PROVISIONING_CONTEXT]); context['expires_at']=0
            session[s.PROVISIONING_CONTEXT]=context
        self.assertEqual(self.client.post('/sace/home/provisioning',data={'journey':nonce}).status_code,403)
        self.client.get('/login?next=/unrelated')
        with self.client.session_transaction() as session:
            self.assertNotIn(s.PROVISIONING_CONTEXT,session)
        nonce=self.direct_home_entry()
        with self.client.session_transaction() as session:
            saved_context=dict(session[s.PROVISIONING_CONTEXT])
        self.client.post('/sace/home/provisioning',data={'journey':nonce})
        with self.client.session_transaction() as session:
            saved_pledge=dict(session['sace_home_pledge_controller'])
        self.client.post('/register',data={'subject':s.SUBJECT,'full_name':'HOME R','email':'replay-r@example.test','password':'test-password'},follow_redirects=True)
        self.user('replay-other@example.test')
        replay=self.app.test_client()
        self.login(replay,'replay-other@example.test')
        with replay.session_transaction() as session:
            session[s.PROVISIONING_CONTEXT]=saved_context
            session['sace_home_provisioning_token']=nonce
            session['sace_home_pledge_controller']=saved_pledge
        self.assertEqual(replay.get('/sace/home/provisioning?journey='+nonce).status_code,403)
        with self.app.app_context():
            self.assertEqual(HomeController.query.count(),1)
            self.assertEqual(HomeProvisioning.query.count(),1)
            self.assertEqual(h.Interaction.query.count(),0)

    def test_authenticated_nonce_cannot_transfer_between_identities(self):
        self.user('bound-r@example.test'); self.user('other-r@example.test')
        self.login(self.client,'bound-r@example.test')
        nonce=self.direct_home_entry()
        with self.client.session_transaction() as session:
            context=dict(session[s.PROVISIONING_CONTEXT])
        other=self.app.test_client(); self.login(other,'other-r@example.test')
        with other.session_transaction() as session:
            session[s.PROVISIONING_CONTEXT]=context
            session['sace_home_provisioning_token']=nonce
        self.assertEqual(other.get('/sace/home/provisioning?journey='+nonce).status_code,403)
        with self.app.app_context():
            self.assertEqual(HomeController.query.count(),0)

    def test_r_control_final_completion_and_provider_documents(self):
        from unittest.mock import patch
        self.provision_home()
        first = self.code_home()
        second = self.code_home()
        self.assertNotEqual(first, second)
        response = self.client.get('/sace/home/control')
        html = response.data.decode()
        self.assertGreater(html.index('Complete Activity Endorsement'), html.index('HOME examination assignments'))
        response = self.client.get('/sace/home/control/documents')
        html = response.data.decode()
        for title in ('Application Form 1', 'Application Form 2', 'Facilitator CVs &amp; Compliance'):
            self.assertIn(title, html)
        self.assertIn('The primary SACE application form.', html)
        self.assertIn('The secondary SACE application form.', html)
        self.assertEqual(html.count('Awaiting an approved HOME document version.'), 3)
        for title in ('HOME programme / timetable', 'HOME Workshop / Participant Manual',
                      'HOME Facilitator Manual', 'Assessment tools / evidence',
                      'Monitoring / evaluation evidence', 'Applicable HOME certificate evidence'):
            self.assertNotIn(title, html)
        with self.app.app_context():
            path = Path(self.documents.name) / 'application.pdf'
            path.write_bytes(b'%PDF-1.4\nHOME provider test fixture only')
            version = s.publish_document(HomeController.query.one(), 'application_form_1',
                'provider-test', path.name, {'subject': s.SUBJECT, 'kind': 'application_form_1',
                'home_approval': {'approved_by': 'local test', 'reference': 'fixture'}})
            db.session.commit()
            vid = version.id
        response = self.client.get('/sace/home/control/documents')
        self.assertIn(('/sace/home/documents/' + str(vid) + '/content').encode(), response.data)
        target = f'/sace/home/control/documents/{vid}/email'
        with patch('app.utils.mailer.send_pdf_email') as send:
            self.assertEqual(self.client.post(target, data={'recipient_email': 'r@example.com'}).status_code, 302)
            send.assert_called_once()
            self.assertEqual(send.call_args.args[3], path.read_bytes())
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            with patch('app.utils.mailer.send_pdf_email') as send:
                self.assertEqual(self.client.post(target,
                    data={'recipient_email': 'r@example.com'}).status_code, 400)
                send.assert_not_called()
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
        auditor, aid = self.join_home(first)
        with self.app.app_context():
            kinds = {item['kind'] for item in s.board_items(db.session.get(HomeAssignment, aid))}
            self.assertTrue({'timetable', 'participant_manual', 'facilitator_manual',
                'assessment', 'monitoring', 'certificate'} <= kinds)
        self.assertEqual(auditor.post(target, data={'recipient_email': 'r@example.com'}).status_code, 403)
        self.assertEqual(self.app.test_client().post(target).status_code, 302)
        self.assertEqual(self.client.get('/sace/home/control/completion').status_code, 200)
        self.assertEqual(self.client.post('/sace/home/control/completion', data={'decision': 'yes'}).status_code, 302)
        html = self.client.get('/sace/home/control').data.decode()
        self.assertGreater(html.index('Cancel Completion'), html.index('HOME examination assignments'))

    def test_new_and_returning_home_controller(self):
        uid = self.provision_home()
        self.code_home()
        self.assertEqual(self.client.get("/sace/home/control/documents").status_code, 200)
        self.assertIn(b"Application Form 1", self.client.get("/sace/home/control/documents").data)
        with self.app.app_context():
            self.assertEqual(HomePledge.query.filter_by(role="controller").count(), 1)
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.filter_by(subject_id=901).count(), 1)
            self.assertEqual(h.Interaction.query.count(), 0)
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "home-r@example.test", "/sace/home/").location, "/sace/home/control")
        self.assertEqual(self.client.get("/sace/home/", follow_redirects=True).status_code, 200)
        self.assertEqual(self.client.get("/sace/reading").status_code, 403)

    def test_new_auditor_summary_and_returning_assignment(self):
        self.provision_home()
        client, aid = self.join_home(self.code_home())
        base = f"/sace/home/assignments/{aid}"
        self.assertEqual(client.get(base + "/summary").status_code, 200)
        self.assertEqual(client.post(base + "/summary").location, base + "/board")
        self.assertIn(b"Examined", client.get(base + "/board").data)
        with self.app.app_context():
            self.assertEqual(HomePledge.query.filter_by(role="auditor").count(), 1)
            self.assertEqual(HomeEvidence.query.filter_by(item="summary", event="examined").count(), 1)
            self.assertEqual(h.Interaction.query.count(), 0)
            self.assertEqual(h.endorsement.assignments(db.session.get(HomeAssignment, aid).auditor_id), [])
        self.assertEqual(client.get(base + "/experience").status_code, 200)
        self.assertEqual(client.post(base + "/completion").status_code, 409)
        self.assertEqual(client.get("/sace/reading").status_code, 403)
        client.get("/logout")
        self.login(client, "home-a@example.test", "/sace/home/")
        self.assertEqual(client.get("/sace/home/").location, base + "/board")

    def test_existing_account_join_and_assignment_ownership(self):
        self.provision_home()
        uid = self.user("existing-home-a@example.test")
        client, aid = self.join_home(self.code_home(), "existing-home-a@example.test", existing=True)
        with self.app.app_context():
            self.assertEqual(db.session.get(HomeAssignment, aid).auditor_id, uid)
            self.assertEqual(h.auth_models.User.query.count(), 2)
        self.assertEqual(self.client.get(f"/sace/home/assignments/{aid}/board").status_code, 403)
        self.assertEqual(client.get("/sace/home/control").status_code, 403)

    def test_existing_registration_branch(self):
        self.provision_home()
        uid = self.user("home-a@example.test")
        client, aid = self.join_home(self.code_home())
        with self.app.app_context():
            self.assertEqual(db.session.get(HomeAssignment, aid).auditor_id, uid)

    def test_invalid_expired_claimed_codes(self):
        self.provision_home()
        other = self.app.test_client()
        self.assertEqual(other.post("/sace/home/join", data={"code": "NOT-HOME"}).status_code, 400)
        expired = self.code_home()
        with self.app.app_context():
            row = HomeInvitation.query.filter_by(code_hash=s.digest(expired)).one()
            row.expires_at = now() - timedelta(seconds=1)
            db.session.commit()
        self.assertEqual(other.post("/sace/home/join", data={"code": expired}).status_code, 400)
        code = self.code_home()
        self.join_home(code)
        self.assertEqual(other.post("/sace/home/join", data={"code": code}).status_code, 400)

    def test_evaluator_accept_without_signature_with_real_csrf(self):
        from datetime import datetime
        from flask_wtf.csrf import generate_csrf
        self.provision_home()
        code = self.code_home()
        client = self.app.test_client()
        client.post('/sace/home/join', data={'code': code})
        original = self.app.jinja_env.globals['csrf_token']
        self.app.jinja_env.globals['csrf_token'] = generate_csrf
        self.app.config['WTF_CSRF_ENABLED'] = True
        try:
            origin = 'https://localhost'
            page = client.get('/sace/home/pledge', base_url=origin)
            self.assertEqual(page.status_code, 200)
            self.assertEqual(page.headers['Referrer-Policy'], 'same-origin')
            self.assertNotIn(b'Full name / signature', page.data)
            self.assertNotIn(b'name="signature"', page.data)
            self.assertNotIn(b'type="checkbox"', page.data)
            self.assertIn(b'name="accept" value="yes">Accept and Continue', page.data)
            token = re.search(rb'name="csrf_token" value="([^"]+)"', page.data).group(1).decode()
            target = origin + '/sace/home/pledge'
            for data, headers in (({'accept': 'yes'}, {'Referer': target}),
                    ({'accept': 'yes', 'csrf_token': token}, {}),
                    ({'accept': 'yes', 'csrf_token': token}, {'Referer': 'https://foreign.example/'}),
                    ({'csrf_token': token}, {'Referer': target})):
                self.assertEqual(client.post('/sace/home/pledge', base_url=origin,
                    data=data, headers=headers).status_code, 400)
            response = client.post('/sace/home/pledge', base_url=origin,
                data={'accept': 'yes', 'csrf_token': token, 'signature': 'Forged Name'},
                headers={'Referer': target})
            self.assertEqual(response.status_code, 302)
            with client.session_transaction() as state:
                accepted_at = datetime.fromisoformat(state['sace_home_pledge_auditor']['accepted_at'])
            with self.app.app_context():
                self.assertEqual(HomePledge.query.filter_by(role='auditor').count(), 0)
        finally:
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.app.jinja_env.globals['csrf_token'] = original
        # Anonymous consent becomes durable only against the authenticated claimant.
        response = client.post('/register', data={'subject': s.SUBJECT,
            'full_name': 'Actual Evaluator', 'email': 'evaluator@example.test',
            'password': 'test-password'}, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            assignment = HomeAssignment.query.one()
            pledge = HomePledge.query.filter_by(role='auditor').one()
            self.assertEqual(pledge.user_id, assignment.auditor_id)
            self.assertEqual(pledge.signature, 'Actual Evaluator')
            self.assertEqual(pledge.invitation_id, assignment.invitation_id)
            self.assertIsNone(pledge.provisioning_id)
            self.assertEqual(pledge.version, s.PLEDGE_VERSION)
            self.assertEqual(pledge.text_hash, s.digest(s.PLEDGE_TEXT))
            self.assertEqual(pledge.accepted_at, accepted_at)
            self.assertIsNotNone(pledge.recorded_at)
            evidence = HomeEvidence.query.filter_by(assignment_id=assignment.id,
                item='pledge', event='accepted').one()
            self.assertEqual(evidence.actor_id, assignment.auditor_id)
            self.assertEqual(evidence.details['version'], s.PLEDGE_VERSION)

    def test_pledge_and_provisioning_protection(self):
        self.assertEqual(self.client.get("/sace/home/provisioning").status_code, 302)
        self.provision_home()
        code = self.code_home()
        client = self.app.test_client()
        client.post("/sace/home/join", data={"code": code})
        self.assertEqual(client.post("/sace/home/pledge", data={"signature": "Test"}).status_code, 400)
        self.assertEqual(client.get("/register?subject=" + s.SUBJECT).status_code, 400)
        self.assertEqual(self.client.post("/sace/home/join", data={"code": code}).status_code, 302)
        self.client.post("/sace/home/pledge", data={"signature": "R", "accept": "yes"})
        self.assertEqual(self.client.get("/sace/home/claim").status_code, 403)

    def test_litre_authority_code_and_evidence_do_not_grant_home(self):
        uid = self.user("litre@example.test")
        h.AccessJourneys.provision(self, self.client, "litre@example.test", existing=True)
        with self.app.app_context():
            row = h.Interaction(user_id=uid, activity_slug="auditor_provisioned",
                response_data=h.json.dumps({"code": "LITRE-TEST", "status": "Claimed", "claimed_by_user_id": uid}))
            db.session.add(row)
            db.session.flush()
            db.session.add(h.Interaction(user_id=uid, workshop_session_id=h.endorsement.room(row),
                activity_slug="map_reviewed", response_data="{}"))
            db.session.commit()
        self.login(self.client, "litre@example.test", "/sace/home/")
        self.assertEqual(self.client.get("/sace/home/control").status_code, 403)
        self.assertEqual(self.client.post("/sace/home/join", data={"code": "LITRE-TEST"}).status_code, 400)
        with self.app.app_context():
            self.assertEqual(HomeAssignment.query.count(), 0)
            self.assertEqual(HomeEvidence.query.count(), 0)
        self.assertEqual(self.client.get("/sace/provisioning").status_code, 200)
        self.client.get("/logout")
        self.assertEqual(self.login(self.client, "litre@example.test", "/sace/reading").location, "/sace/provisioning")

    def test_home_code_rejected_by_litre(self):
        self.provision_home()
        code = self.code_home()
        client = self.app.test_client()
        result = client.post("/sace/join", data={"code": code}, follow_redirects=True)
        self.assertNotEqual(result.request.path, "/sace/auditor_pledge")
        with client.session_transaction() as state:
            self.assertFalse(state.get("pending_sace_code"))
        with self.app.app_context():
            self.assertEqual(h.Interaction.query.count(), 0)
            self.assertEqual(HomeInvitation.query.one().status, "unclaimed")

    def test_document_version_examination_and_unavailable_manuals(self):
        self.provision_home()
        client, aid = self.join_home(self.code_home())
        base = f"/sace/home/assignments/{aid}"
        self.assertEqual(client.post(base + "/materials/participant_manual").status_code, 409)
        with self.app.app_context():
            path = Path(self.documents.name) / "test-evidence.pdf"
            path.write_bytes(b"%PDF-1.4\nHOME test fixture only")
            version = s.publish_document(HomeController.query.one(), "assessment", "test-v1",
                path.name, {"fixture": True})
            db.session.commit()
            vid = version.id
        target = base + "/materials/assessment"
        self.assertEqual(client.post(target, data={"version_id": vid}).status_code, 409)
        with client.get(f"/sace/home/documents/{vid}/content?assignment_id={aid}") as response:
            self.assertEqual(response.status_code, 200)
        self.assertEqual(client.post(target, data={"version_id": vid}).status_code, 302)
        with self.app.app_context():
            self.assertTrue(s.examined(db.session.get(HomeAssignment, aid), "assessment", vid))
            version = db.session.get(HomeDocumentVersion, vid)
            version2 = HomeDocumentVersion(document_id=version.document_id, version="test-v2", storage_key=version.storage_key,
                sha256=version.sha256, source_manifest={"fixture": True}, approved_by=version.approved_by)
            db.session.add(version2)
            db.session.commit()
            self.assertFalse(next(x for x in s.board_items(db.session.get(HomeAssignment, aid)) if x["kind"] == "assessment")["examined"])
        self.assertEqual(self.app.test_client().get(f"/sace/home/documents/{vid}/content").status_code, 302)

    def test_same_identity_evidence_stays_separate(self):
        self.provision_home()
        uid = self.user("dual-a@example.test")
        with self.app.app_context():
            row = h.Interaction(user_id=uid, activity_slug="auditor_provisioned",
                response_data=h.json.dumps({"code": "LITRE-ONLY", "status": "Claimed", "claimed_by_user_id": uid}))
            db.session.add(row)
            db.session.flush()
            room = h.endorsement.room(row)
            db.session.add(h.Interaction(user_id=uid, workshop_session_id=room,
                activity_slug="map_reviewed", response_data="{}"))
            db.session.commit()
        client, aid = self.join_home(self.code_home(), "dual-a@example.test", existing=True)
        with self.app.app_context():
            self.assertFalse(s.examined(db.session.get(HomeAssignment, aid), "summary"))
            before = h.Interaction.query.count()
        client.post(f"/sace/home/assignments/{aid}/summary")
        client.post(f"/sace/home/assignments/{aid}/summary")
        with self.app.app_context():
            self.assertEqual(h.Interaction.query.count(), before)
            self.assertEqual(HomeEvidence.query.filter_by(item="summary", event="examined").count(), 1)
            self.assertEqual(len(h.endorsement.assignments(uid)), 1)

    def test_provisioning_email_binding_and_expiry_after_pledge(self):
        self.user("wrong@example.test")
        with self.app.app_context():
            token = s.issue_provisioning("intended@example.test", "local-test")
            db.session.commit()
        client = self.app.test_client()
        client.get("/sace/home/provisioning?token=" + token)
        client.post("/sace/home/provisioning", data={"signature": "Test", "accept": "yes"})
        self.login(client, "wrong@example.test", "/sace/home/provisioning")
        self.assertEqual(client.get("/sace/home/provisioning").status_code, 403)
        self.provision_home()
        code = self.code_home()
        client.post("/sace/home/join", data={"code": code})
        client.post("/sace/home/pledge", data={"signature": "A", "accept": "yes"})
        with self.app.app_context():
            invitation = HomeInvitation.query.filter_by(code_hash=s.digest(code)).one()
            invitation.expires_at = now() - timedelta(seconds=1)
            db.session.commit()
        self.assertEqual(client.get("/sace/home/claim").status_code, 400)
        with self.app.app_context():
            self.assertEqual(HomeAssignment.query.count(), 0)

    def test_manual_publication_requires_approved_source(self):
        self.provision_home()
        with self.app.app_context():
            with self.assertRaises(ValueError):
                s.publish_document(HomeController.query.one(), "participant_manual", "v1", "missing.pdf", {})
            self.assertEqual(HomeDocumentVersion.query.count(), 0)
            db.session.rollback()

    def test_real_layout_and_csrf(self):
        from jinja2 import FileSystemLoader
        loader = self.app.jinja_loader
        self.app.jinja_loader = FileSystemLoader(str(ROOT / "templates"))
        self.app.jinja_env.cache.clear()
        try:
            response = self.client.get("/sace/home/join")
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"<!DOCTYPE html>", response.data)
        finally:
            self.app.jinja_loader = loader
            self.app.jinja_env.cache.clear()
        self.app.config["WTF_CSRF_ENABLED"] = True
        try:
            self.assertEqual(self.client.post("/sace/home/join", data={"code": "HOME-TEST"}).status_code, 400)
        finally:
            self.app.config["WTF_CSRF_ENABLED"] = False

    def test_migration_roundtrip_is_additive_and_isolated(self):
        from dotenv import dotenv_values
        engine = create_engine(dotenv_values(ROOT / ".env")["DATABASE_URL"])
        spec = importlib.util.spec_from_file_location("home_migration", ROOT / "migrations/versions/home_sace_001_foundation.py")
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        schema = "pg_temp"
        try:
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    conn.execute(text('SET LOCAL search_path TO "' + schema + '"'))
                    conn.execute(text('CREATE TEMP TABLE "user" (id INTEGER PRIMARY KEY)'))
                    conn.execute(text('CREATE TEMP TABLE sace_workshop_interactions (id INTEGER PRIMARY KEY, response_data TEXT)'))
                    conn.execute(text("INSERT INTO sace_workshop_interactions VALUES (1, 'unchanged LITRE sentinel')"))
                    with Operations.context(MigrationContext.configure(conn)):
                        migration.upgrade()
                        names = conn.execute(text("SELECT relname FROM pg_class WHERE relnamespace = pg_my_temp_schema() AND relkind = 'r'")).scalars().all()
                        self.assertEqual(len([n for n in names if n.startswith("sace_home_")]), 8)
                        migration.downgrade()
                    self.assertEqual(sorted(conn.execute(text("SELECT relname FROM pg_class WHERE relnamespace = pg_my_temp_schema() AND relkind = 'r'")).scalars().all()), ["sace_workshop_interactions", "user"])
                    self.assertEqual(conn.execute(text('SELECT response_data FROM sace_workshop_interactions')).scalar(), 'unchanged LITRE sentinel')
                finally:
                    transaction.rollback()
        finally:
            engine.dispose()


if __name__ == "__main__":
    unittest.main(verbosity=2)
