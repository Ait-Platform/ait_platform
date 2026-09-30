"""Reading lifecycle tests. Only localhost ait_local_db, isolated test tables/schemas."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit, parse_qs
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('reading_lifecycle_support', ROOT / 'tests/support/sace_access_postgres_runner.py')
h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
from app.program_sace import lifecycle as lc, access, endorsement as flow
from app.extensions import db
from sqlalchemy import text
from flask_login import login_user


class ReadingLifecycle(h.AccessJourneys):
    # Execute this subclass's lifecycle checks only; existing security tests run separately.
    def active_r(self):
        self.provision(self.client, 'r@example.test')
        with self.app.app_context():
            row = lc.Appointment.query.one()
            return row.id, row.engagement_id, row.user_id

    def auditor(self):
        code, invitation = self.code(self.client)
        self.user('a@example.test')
        client = self.app.test_client()
        client.post('/sace/join', data={'code':code})
        client.post('/sace/auditor_pledge')
        result = self.login(client, 'a@example.test')
        self.assertEqual(client.get(result.location, follow_redirects=True).status_code, 200)
        return client, invitation

    def finish(self, path, status='completed'):
        return self.client.post(path, data={'status':status, 'reason':'Reviewed local test completion'})

    def test_lifecycle_grant_alone_and_global_metadata_denied(self):
        uid = self.user('orphan@example.test')
        with self.app.app_context():
            db.session.add(h.auth_models.AuthSubjectAdmin(subject_id=900, email='orphan@example.test'))
            db.session.add(h.auth_models.UserEnrollment(user_id=uid, subject_id=900, status='active'))
            db.session.execute(text("INSERT INTO auth_approved_admin (email, active) VALUES ('orphan@example.test', 1)"))
            lc.event(uid, 'admin_patent_pledge', {})
            db.session.commit()
        result = self.login(self.client, 'orphan@example.test')
        self.assertNotEqual(result.location, '/sace/provisioning')
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code, 403)
        self.client.get('/sace/provisioning')
        self.assertEqual(self.pledge(self.client).status_code, 409)
        with self.app.app_context():
            self.assertEqual(lc.Appointment.query.count(), 0)

    def test_lifecycle_exact_grant_subject_email_and_parent_required(self):
        aid, eid, uid = self.active_r()
        with self.app.app_context():
            grant = h.auth_models.AuthSubjectAdmin.query.one()
            grant.email = 'changed@example.test'; db.session.commit()
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code, 403)
        with self.app.app_context():
            grant = h.auth_models.AuthSubjectAdmin.query.one()
            grant.email = 'r@example.test'; grant.subject_id = 44; db.session.commit()
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code, 403)
        with self.app.app_context():
            grant = h.auth_models.AuthSubjectAdmin.query.one(); grant.subject_id = 900
            engagement = db.session.get(lc.Engagement, eid)
            engagement.status='completed'; engagement.completed_at=lc.utcnow(); engagement.ended_by_user_id=uid
            db.session.commit()
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code, 403)

    def test_lifecycle_end_appointment_and_subsequent_endorsement(self):
        aid,eid,uid = self.active_r()
        with self.app.app_context():
            old_events = h.Interaction.query.count()
            db.session.add(h.auth_models.AuthSubjectAdmin(subject_id=44,email='r@example.test'))
            db.session.commit()
        self.finish(f'/sace/provisioning/appointments/{aid}/end')
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code,403)
        self.client.get('/logout')
        self.assertNotEqual(self.login(self.client,'r@example.test').location,'/sace/provisioning')
        with self.app.app_context():
            row=db.session.get(lc.Appointment,aid)
            self.assertEqual(row.status,'completed');self.assertIsNone(row.operational_grant_id)
            self.assertIsNotNone(db.session.get(h.auth_models.User,uid))
            self.assertEqual(h.auth_models.UserEnrollment.query.count(),1)
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.one().subject_id,44)
            self.assertGreater(h.Interaction.query.count(),old_events)
        self.client.get('/sace/provisioning');self.pledge(self.client)
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.count(),2)
            self.assertEqual(lc.Appointment.query.count(),2)
            self.assertNotEqual(lc.Appointment.query.filter_by(status='active').one().engagement_id,eid)

    def test_lifecycle_whole_closure_kills_stale_r_a_and_codes(self):
        aid,eid,uid=self.active_r();auditor,inv=self.auditor();code,unused=self.code(self.client)
        self.assertEqual(self.finish('/sace/provisioning/engagement/end','revoked').status_code,302)
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code,403)
        self.assertEqual(auditor.get('/sace/reading').status_code,403)
        self.assertEqual(auditor.get('/sace/reading/finish-evaluation').status_code,403)
        other=self.app.test_client();other.post('/sace/join',data={'code':code})
        with other.session_transaction() as state:self.assertNotIn('pending_sace_code',state)
        with self.app.app_context():
            self.assertEqual(db.session.get(lc.Engagement,eid).status,'revoked')
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.count(),0)
            self.assertEqual(lc.payload(db.session.get(h.Interaction,inv))['status'],'Revoked')
            self.assertEqual(lc.payload(db.session.get(h.Interaction,unused))['status'],'Revoked')

    def test_lifecycle_handover_preserves_issuer_and_active_auditor(self):
        aid,eid,uid=self.active_r();auditor,inv=self.auditor();code,unused=self.code(self.client)
        response=self.client.post('/sace/provisioning/handover',data={'email':'successor@example.test'})
        import re,html
        url=html.unescape(re.search(r'value="(http[^"]+)"',response.data.decode()).group(1))
        successor=self.app.test_client();self.user('successor@example.test')
        successor.get(urlsplit(url).path+'?'+urlsplit(url).query)
        self.pledge(successor)
        with successor.session_transaction() as state:next_url='/sace/provisioning?journey='+state[access.PROVISIONING_KEY]['nonce']
        result=self.login(successor,'successor@example.test',next_url)
        self.assertEqual(successor.get(result.location,follow_redirects=True).status_code,200)
        self.finish(f'/sace/provisioning/appointments/{aid}/end')
        self.assertEqual(auditor.get('/sace/reading').status_code,200)
        self.assertEqual(successor.get('/sace/provisioning/feed').status_code,200)
        with self.app.app_context():
            self.assertEqual(lc.Engagement.query.count(),1)
            self.assertEqual(lc.Appointment.query.count(),2)
            self.assertEqual(db.session.get(lc.AssignmentContext,inv).issuing_appointment_id,aid)
            self.assertEqual(lc.payload(db.session.get(h.Interaction,unused))['status'],'Revoked')
        self.assertEqual(successor.get(urlsplit(url).path+'?'+urlsplit(url).query).status_code,200) # existing R; no new appointment
        with self.app.app_context():self.assertEqual(lc.Appointment.query.count(),2)

    def test_lifecycle_auditor_completion_no_login_restoration(self):
        self.active_r();auditor,inv=self.auditor()
        # Eligibility is tested in the existing Reading suite; isolate the closure operation.
        with patch.object(flow,'completion_requirements',return_value=[]):
            result=auditor.post('/sace/reading/finish-evaluation')
        self.assertEqual(result.status_code,200)
        self.assertEqual(auditor.get('/sace/reading').status_code,403)
        auditor.get('/logout')
        self.assertNotEqual(self.login(auditor,'a@example.test').location,'/sace/reading')
        with self.app.app_context():
            data=lc.payload(db.session.get(h.Interaction,inv))
            self.assertEqual(data['status'],'Completed');self.assertIn('completed_at',data)
            self.assertEqual(h.auth_models.AuthSubjectAdmin.query.count(),1)

    def test_lifecycle_close_rollback_is_atomic(self):
        aid,eid,uid=self.active_r();auditor,inv=self.auditor()
        with patch.object(db.session,'commit',side_effect=RuntimeError('rollback')):
            with self.assertRaises(RuntimeError):self.finish('/sace/provisioning/engagement/end')
        self.assertEqual(auditor.get('/sace/reading').status_code,200)
        self.assertEqual(self.client.post('/sace/provisioning/generate_code').status_code,302)
        with self.app.app_context():self.assertEqual(db.session.get(lc.Engagement,eid).status,'active')

    def seed_cutover(self):
        with self.app.app_context():
            for uid,email in [(622,'r622@example.test'),(623,'a623@example.test'),(630,'test630@example.test'),(640,'unreviewed@example.test')]:
                user=h.auth_models.User(id=uid,email=email,name='Fixture',is_active=1);user.set_password('test-password');db.session.add(user)
            db.session.flush()
            for gid,uid in [(2,622),(3,630),(4,640)]:
                db.session.add(h.auth_models.AuthSubjectAdmin(id=gid,subject_id=900,email=db.session.get(h.auth_models.User,uid).email))
            for event_id,uid,slug in [(72,622,'controller_provisioned'),(73,622,'admin_patent_pledge'),(304,630,'controller_provisioned'),(305,630,'admin_patent_pledge')]:
                db.session.add(h.Interaction(id=event_id,user_id=uid,activity_slug=slug,response_data='{}',timestamp=datetime(2026,9,24 if uid==622 else 30,5,46,25)))
            db.session.add(h.Interaction(id=74,user_id=622,activity_slug='auditor_provisioned',response_data=json.dumps({'status':'Claimed','claimed_by_user_id':623,'code':'FIXTURE'})))
            db.session.add(h.Interaction(id=218,user_id=622,activity_slug='auditor_provisioned',response_data=json.dumps({'status':'Claimed','claimed_by_user_id':630,'code':'FIXTURE630'})))
            for event_id,slug in [(220,'pledge'),(221,'assignment_claimed')]:
                db.session.add(h.Interaction(id=event_id,user_id=630,workshop_session_id='endorsement-218',activity_slug=slug,response_data='{}'))
            db.session.add(h.Interaction(id=219,user_id=622,activity_slug='auditor_provisioned',response_data=json.dumps({'status':'Claimed','claimed_by_user_id':640})))
            db.session.add(h.Interaction(id=307,user_id=623,workshop_session_id='endorsement-74',activity_slug='map_reviewed',response_data='{}'))
            db.session.commit()
        return dict(reference='reviewed-622-reading',r_provisioning_event_id=72,r_pledge_event_id=73,auditor_assignments=[{'invitation_id':74,'auditor_user_id':623,'status':'Claimed'},{'invitation_id':218,'auditor_user_id':630,'status':'Claimed'}],reason='Reviewed test-created grant disposition')

    def test_lifecycle_reviewed_622_623_630_cutover_idempotent(self):
        manifest=self.seed_cutover()
        with self.app.app_context():
            before={i:(db.session.get(h.Interaction,i).response_data,db.session.get(h.Interaction,i).timestamp) for i in [72,73,74,218,219,220,221,304,305,307]}
            engagement=lc.reviewed_cutover(manifest,622);eid=engagement.id;db.session.commit()
            self.assertEqual(lc.reviewed_cutover(manifest,622).id,eid);db.session.commit()
            self.assertIsNotNone(db.session.get(h.auth_models.AuthSubjectAdmin,2));self.assertIsNone(db.session.get(h.auth_models.AuthSubjectAdmin,3))
            self.assertIsNotNone(db.session.get(h.auth_models.AuthSubjectAdmin,4))
            self.assertEqual(lc.Appointment.query.one().user_id,622)
            self.assertEqual(db.session.get(lc.AssignmentContext,74).engagement_id,eid)
            self.assertEqual(db.session.get(lc.AssignmentContext,218).engagement_id,eid)
            self.assertIsNone(db.session.get(lc.AssignmentContext,219))
            self.assertEqual(lc.Engagement.query.count(),1)
            self.assertEqual(lc.AssignmentContext.query.count(),2)
            for i,values in before.items():self.assertEqual((db.session.get(h.Interaction,i).response_data,db.session.get(h.Interaction,i).timestamp),values)
            self.assertIsNotNone(db.session.get(h.auth_models.User,630))
        self.assertEqual(self.login(self.client,'r622@example.test').location,'/sace/provisioning')
        client=self.app.test_client();self.assertEqual(self.login(client,'a623@example.test').location,'/sace/reading')
        client=self.app.test_client();self.assertEqual(self.login(client,'test630@example.test').location,'/sace/reading')
        client=self.app.test_client();self.assertEqual(self.login(client,'unreviewed@example.test').location,'/dashboard')

    def test_lifecycle_cutover_wrong_manifest_rolls_back(self):
        manifest=self.seed_cutover();manifest['r_pledge_event_id']=305
        from werkzeug.exceptions import HTTPException
        with self.app.app_context():
            with self.assertRaises(HTTPException):lc.reviewed_cutover(manifest,622)
            db.session.rollback()
            self.assertIsNotNone(db.session.get(h.auth_models.AuthSubjectAdmin,3))
            self.assertEqual(lc.Engagement.query.count(),0)

    def test_lifecycle_cutover_reviewed_assignment_validation(self):
        import copy
        from werkzeug.exceptions import HTTPException
        manifest=self.seed_cutover()
        variants=[]
        for field,value in [('auditor_user_id',623),('status','Completed')]:
            bad=copy.deepcopy(manifest);bad['auditor_assignments'][1][field]=value;variants.append(bad)
        bad=copy.deepcopy(manifest);bad['auditor_assignments'].append(bad['auditor_assignments'][0]);variants.append(bad)
        with self.app.app_context():
            for bad in variants:
                with self.assertRaises(HTTPException):lc.reviewed_cutover(bad,622)
                db.session.rollback()
                self.assertEqual(lc.Engagement.query.count(),0)
                self.assertIsNotNone(db.session.get(h.auth_models.AuthSubjectAdmin,3))
            lc.reviewed_cutover(manifest,622);db.session.commit()
            bad=copy.deepcopy(manifest);bad['auditor_assignments'].pop()
            with self.assertRaises(HTTPException):lc.reviewed_cutover(bad,622)
            db.session.rollback()

    def test_lifecycle_cutover_preserves_terminal_assignment_states(self):
        manifest=self.seed_cutover()
        with self.app.app_context():
            for item,status in zip(manifest['auditor_assignments'],['Completed','Revoked']):
                row=db.session.get(h.Interaction,item['invitation_id'])
                state=json.loads(row.response_data);state['status']=status;row.response_data=json.dumps(state)
                item['status']=status
            db.session.commit()
            lc.reviewed_cutover(manifest,622);db.session.commit()
            for item in manifest['auditor_assignments']:
                self.assertEqual(json.loads(db.session.get(h.Interaction,item['invitation_id']).response_data)['status'],item['status'])
                self.assertIsNotNone(db.session.get(lc.AssignmentContext,item['invitation_id']))
        for email in ['a623@example.test','test630@example.test']:
            self.assertEqual(self.login(self.app.test_client(),email).location,'/dashboard')

    def test_lifecycle_migration_additive_and_downgrade_scoped(self):
        from alembic.migration import MigrationContext
        from alembic.operations import Operations
        from sqlalchemy import create_engine, inspect
        import uuid
        spec=importlib.util.spec_from_file_location('reading_revision', ROOT/'migrations/versions/reading_sace_001_engagement.py')
        revision=importlib.util.module_from_spec(spec);spec.loader.exec_module(revision)
        with self.app.app_context():url=db.engine.url
        engine=create_engine(url)
        schema='reading_migration_test_'+uuid.uuid4().hex
        try:
            with engine.connect() as conn:
                tx=conn.begin()
                try:
                    before=set(conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'")).scalars())
                    for name in ('user','auth_subject','auth_subject_admin','sace_workshop_interactions'):
                        conn.execute(text('CREATE TEMP TABLE "'+name+'" (id integer PRIMARY KEY)'))
                    schema=conn.execute(text('SELECT pg_my_temp_schema()::regnamespace::text')).scalar_one()
                    conn=conn.execution_options(schema_translate_map={None:'pg_temp'})
                    revision.op=Operations(MigrationContext.configure(conn))
                    revision.upgrade()
                    self.assertTrue(set(revision.TABLE_NAMES).issubset(inspect(conn).get_table_names(schema=schema)))
                    for model in (lc.Engagement,lc.Appointment,lc.AssignmentContext):
                        table=revision.Base.metadata.tables[model.__tablename__]
                        self.assertEqual(set(table.columns.keys()),set(model.__table__.columns.keys()))
                        self.assertEqual({i.name for i in table.indexes},{i.name for i in model.__table__.indexes})
                    revision.downgrade()
                    self.assertEqual(set(inspect(conn).get_table_names(schema=schema)),{'user','auth_subject','auth_subject_admin','sace_workshop_interactions'})
                    self.assertEqual(before,set(conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'")).scalars()))
                finally:tx.rollback()
        finally:engine.dispose()

    def test_lifecycle_concurrent_close_blocks_late_controller_operation(self):
        import threading,uuid,time
        from flask import Flask
        from sqlalchemy import create_engine
        from werkzeug.exceptions import HTTPException
        aid,eid,uid=self.active_r()
        schema='rlc_'+uuid.uuid4().hex[:12]+'_'
        assert len(schema) == 17
        with self.app.app_context():
            url=db.engine.url
            with db.engine.begin() as conn:
                for table in self.tables:
                    conn.execute(text('CREATE TABLE public."'+schema+table+'" (LIKE pg_temp."'+table+'" INCLUDING ALL)'))
                    if table != 'sace_reading_assignment_context':
                        conn.execute(text('ALTER TABLE public."'+schema+table+'" ALTER COLUMN id DROP IDENTITY IF EXISTS'))
                        conn.execute(text('ALTER TABLE public."'+schema+table+'" ALTER COLUMN id DROP DEFAULT'))
                        conn.execute(text('ALTER TABLE public."'+schema+table+'" ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY'))
                    conn.execute(text('INSERT INTO public."'+schema+table+'" SELECT * FROM pg_temp."'+table+'"'))
                    if table != 'sace_reading_assignment_context':
                        conn.execute(text("SELECT setval(pg_get_serial_sequence(:table,'id'), COALESCE((SELECT max(id) FROM public.\""+schema+table+"\"),0)+1,false)"),{'table':'public.'+schema+table})
        app=Flask('reading_concurrency');app.secret_key='local-concurrency-test'
        app.config.update(SQLALCHEMY_DATABASE_URI=url,SQLALCHEMY_ENGINE_OPTIONS={'connect_args':{'options':'-csearch_path=pg_temp -clock_timeout=5000'}})
        db.init_app(app);h.login_manager.init_app(app)
        # Each worker's temporary views expose only uniquely prefixed fixture tables.
        from sqlalchemy import event as sql_event
        with app.app_context():
            @sql_event.listens_for(db.engine, 'connect')
            def fixture_views(connection, record):
                cursor=connection.cursor()
                for table in self.tables:
                    cursor.execute('CREATE TEMP VIEW "'+table+'" AS SELECT * FROM public."'+schema+table+'"')
                connection.commit();cursor.close()
        locked=threading.Event();release=threading.Event();attempted=threading.Event();finished=threading.Event()
        outcomes=[];errors=[]
        def close():
            try:
                with app.test_request_context('/'):
                    login_user(db.session.get(h.auth_models.User,uid));lc.subject_lock();locked.set()
                    if not release.wait(5):raise RuntimeError('test release timeout')
                    lc.end_engagement('completed','Concurrent closure');db.session.commit()
            except Exception as exc:errors.append(exc);locked.set()
        def late():
            try:
                with app.test_request_context('/'):
                    login_user(db.session.get(h.auth_models.User,uid));attempted.set()
                    try:lc.require_controller();outcomes.append('unexpected authority')
                    except HTTPException as exc:outcomes.append(exc.code)
                    finally:db.session.rollback()
            except Exception as exc:errors.append(exc)
            finally:finished.set()
        threads=[threading.Thread(target=close),threading.Thread(target=late)]
        try:
            threads[0].start();self.assertTrue(locked.wait(3));threads[1].start();self.assertTrue(attempted.wait(3))
            self.assertFalse(finished.wait(0.2),'Second transaction should wait for the lifecycle lock')
            release.set()
            for thread in threads:thread.join(7)
            self.assertFalse(any(t.is_alive() for t in threads));self.assertEqual(errors,[]);self.assertEqual(outcomes,[403])
        finally:
            release.set()
            for thread in threads:
                if thread.ident:thread.join(7)
            with app.app_context():db.session.remove();db.engine.dispose()
            engine=create_engine(url)
            try:
                with engine.begin() as conn:
                    for table in reversed(self.tables):
                        conn.execute(text('DROP TABLE public."'+schema+table+'"'))
            finally:engine.dispose()

    def test_lifecycle_history_requires_explicit_platform_audit_authority(self):
        aid,eid,uid=self.active_r()
        self.assertEqual(self.client.get(f'/admin/security/sace-engagement/{eid}').status_code,302)
        self.finish('/sace/provisioning/engagement/end')
        self.assertEqual(self.client.get('/sace/provisioning/feed').status_code,403)
        self.user('reviewer@example.test')
        with self.app.app_context():
            db.session.execute(text("INSERT INTO auth_approved_admin (email,active) VALUES ('reviewer@example.test',1)"));db.session.commit()
        reviewer=self.app.test_client();self.login(reviewer,'reviewer@example.test')
        result=reviewer.get(f'/admin/security/sace-engagement/{eid}')
        self.assertEqual(result.status_code,200);self.assertIn(b'completed',result.data)

    def test_lifecycle_invalid_handover_and_cross_engagement_end_denied(self):
        aid,eid,uid=self.active_r()
        self.assertEqual(self.client.post('/sace/provisioning/appointments/99999/end',data={'status':'revoked','reason':'test'}).status_code,404)
        self.assertEqual(self.client.get('/sace/provisioning/lifecycle').status_code,200)
        response=self.client.post('/sace/provisioning/handover',data={'email':'successor@example.test'})
        import re,html
        url=html.unescape(re.search(r'value="(http[^"]+)"',response.data.decode()).group(1))
        self.user('wrong@example.test');other=self.app.test_client()
        other.get(urlsplit(url).path+'?'+urlsplit(url).query);self.pledge(other)
        with other.session_transaction() as state:next_url='/sace/provisioning?journey='+state[access.PROVISIONING_KEY]['nonce']
        result=self.login(other,'wrong@example.test',next_url)
        self.assertEqual(other.get(result.location).status_code,403)
        with self.app.app_context():self.assertEqual(lc.Appointment.query.count(),1)


if __name__=='__main__':
    names=[name for name in unittest.defaultTestLoader.getTestCaseNames(ReadingLifecycle) if name.startswith('test_lifecycle_')]
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(ReadingLifecycle(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
