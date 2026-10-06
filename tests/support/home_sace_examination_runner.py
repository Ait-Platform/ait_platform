"""Original HOME participant handlers in isolated connection-local PostgreSQL fixtures."""
import importlib.util
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import text
from sqlalchemy.schema import CreateTable, CreateIndex
from jinja2 import ChoiceLoader, DictLoader
ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('home_foundation_support', ROOT / 'tests/support/home_sace_postgres_runner.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.extensions import db
import app
app.db = db
from app.models.home import (HomeChapter, HomeQuestion, HomeQuestionOption, HomeProgress,
    HomePracticalSubmission, HomeTeacherLink, HomeFinalAssessment)
from app.models.sace_home import HomeAssignment, HomeEvidence, HomeController, HomeEngagement
from app.program_sace_home import examination as ex, participant_context as context, workshop_interactions as instrument
from app.subject_home import routes as participant
HOME_PARTICIPANT_TABLES = [t for t in db.metadata.sorted_tables if t.name.startswith('home_')]


class HomeExamination(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        f.HomeFoundation.setUpClass.__func__(cls)
        cls.app.register_blueprint(participant.home_bp)
        cls.app.config['SACE_HOME_REQUIREMENTS_VERSION'] = ex.REQUIREMENTS
        cls.app.config['SACE_HOME_EXAMINATION_VERSION'] = None
        cls.app.jinja_loader = ChoiceLoader([DictLoader({'layout.html':
            "{% if home_auditor %}{% include 'subject_home/auditor_controls.html' %}{% endif %}{% block content %}{% endblock %}"}), cls.app.jinja_loader])
        with cls.app.app_context():
            with db.engine.begin() as conn:
                for table in HOME_PARTICIPANT_TABLES:
                    conn.execute(text(str(CreateTable(table).compile(dialect=conn.dialect)).replace('CREATE TABLE', 'CREATE TEMPORARY TABLE', 1)))
                    for index in table.indexes:
                        conn.execute(CreateIndex(index))
        cls.tables += [t.name for t in HOME_PARTICIPANT_TABLES]
        cls.app.add_url_rule('/payments/home', endpoint='paystack_bp.paystack_start', view_func=lambda: 'subscription')

    @classmethod
    def tearDownClass(cls):
        f.HomeFoundation.tearDownClass.__func__(cls)

    provision_home = f.HomeFoundation.provision_home
    code_home = f.HomeFoundation.code_home
    join_home = f.HomeFoundation.join_home
    user = f.HomeFoundation.user
    login = f.HomeFoundation.login

    def setUp(self):
        f.HomeFoundation.setUp(self)
        self.app.config['MAIL_SUPPRESS_SEND'] = False
        self.provision_home()
        self.auditor, self.aid = self.join_home(self.code_home())
        self.base = f'/sace/home/assignments/{self.aid}'
        with self.app.app_context():
            db.session.add(f.h.auth_models.AuthSubject(id=902, slug='home', name='HOME', is_active=1))
            for n in range(1, 31):
                db.session.add(HomeChapter(id=n, chapter_number=n, title=f'Original HOME Chapter {n}', pass_mark=100))
            db.session.flush()
            for n in range(11, 31):
                db.session.add(HomeQuestion(id=n, chapter_id=n, question=f'Original question {n}', question_type='single', correct_answer='Correct'))
                db.session.add(HomeQuestionOption(question_id=n, option_text='Correct', sort_order=1))
                db.session.add(HomeQuestionOption(question_id=n, option_text='Wrong', sort_order=2))
            db.session.commit()

    def url(self, path, assignment=None):
        return path + ('&' if '?' in path else '?') + f'home_assignment_id={self.aid if assignment is None else assignment}'

    def chapter(self, n):
        return self.url(f'/home/chapter/{n}')

    def advance(self, n, action='skip'):
        self.assertEqual(self.auditor.get(self.chapter(n)).status_code, 200)
        response = self.auditor.post(self.url(f'/home/auditor/advance/{n}'), data={'action': action})
        self.assertEqual(response.status_code, 302, response.data)
        return response

    def journey(self, passed=True):
        for n in range(1, 31):
            self.advance(n, 'teacher_examined' if n <= 10 else 'skip')
        target = self.url('/final_exam')
        self.assertEqual(self.auditor.get(target).status_code, 200)
        result = self.auditor.post(target, data={f'q{n}': 'Correct' if passed else 'Wrong' for n in range(21,31)})
        self.assertEqual(result.status_code, 302)
        with self.app.app_context():
            assessment = context.assessment(db.session.get(HomeAssignment, self.aid))
            self.assertEqual(assessment.passed, passed)
            return assessment.id

    def board(self):
        with self.app.app_context():
            return {x['kind']: x for x in f.s.board_items(db.session.get(HomeAssignment,self.aid))}

    def test_summary_notice_requires_authorized_home_auditor(self):
        notice = b'SACE Auditor Examination'
        response = self.auditor.get(self.base + '/summary')
        self.assertEqual(response.status_code, 200)
        self.assertIn(notice, response.data)
        self.assertIn(b'Completing these tests is optional', response.data)
        self.assertNotIn(notice, self.auditor.get(self.url('/dashboard/learner')).data)
        ordinary = self.app.test_client()
        self.user('ordinary-notice@example.test')
        self.login(ordinary, 'ordinary-notice@example.test')
        self.assertEqual(ordinary.get(self.base + '/summary').status_code, 403)
        self.assertNotIn(notice, ordinary.get('/dashboard/learner').data)
        self.assertEqual(self.client.get(self.base + '/summary').status_code, 403)
        with self.app.app_context():
            db.session.get(HomeAssignment, self.aid).status = 'revoked'
            db.session.commit()
        self.assertEqual(self.auditor.get(self.base + '/summary').status_code, 403)

    def test_original_journey_without_bundle_and_order(self):
        result = self.auditor.get(self.base+'/experience')
        self.assertEqual(result.location, self.url('/dashboard/learner'))
        dashboard = self.auditor.get(result.location)
        self.assertEqual(dashboard.status_code, 200)
        self.assertNotIn(b'My Linked Teacher',dashboard.data)
        self.assertEqual(self.auditor.get(self.chapter(2)).status_code,409)
        self.assertEqual(self.auditor.post(self.url('/home/auditor/advance/1'),data={'action':'skip'}).status_code,409)
        response = self.auditor.get(self.chapter(1))
        self.assertIn(b'Teacher Approved / Examined',response.data)
        self.assertIn(b'Learner identified',response.data)
        self.advance(1,'teacher_examined')
        self.advance(2)
        with self.app.app_context():
            self.assertEqual(HomeProgress.query.count(),0)
            self.assertEqual(HomePracticalSubmission.query.count(),0)
            self.assertEqual(HomeTeacherLink.query.count(),0)
            self.assertEqual(f.h.auth_models.UserEnrollment.query.count(),0)
            row=db.session.get(HomeAssignment,self.aid)
            self.assertFalse(f.h.auth_models.AuthSubjectAdmin.query.filter_by(email='home-a@example.test').first())
            self.assertEqual(context.latest(row,'participant_chapter:1','examined').details['action'],'teacher_examined')
        with self.auditor.session_transaction() as state:
            self.assertNotIn('chapter_1_done',state)

    def test_all_original_chapters_and_real_assessment(self):
        aid=self.journey()
        self.assertTrue(self.board()['experience']['examined'])
        self.assertFalse(self.board()['certificate']['examined'])
        response=self.auditor.get(self.url(f'/home/report/exit?assessment_id={aid}'))
        self.assertIn(b'Email Certificate',response.data)
        self.assertIn(f'name="home_assignment_id" value="{self.aid}"'.encode(),response.data)
        with self.app.app_context():
            self.assertEqual(HomeProgress.query.count(),0)
            self.assertEqual(HomePracticalSubmission.query.count(),0)
            self.assertEqual(f.h.auth_models.UserEnrollment.query.count(),0)
            self.assertEqual(f.h.Interaction.query.count(),0)
        self.assertEqual(self.auditor.get(self.base+'/experience/practical/1').status_code,410)

    def test_context_ownership_terminal_states_and_forgeries(self):
        self.assertEqual(self.client.get(self.chapter(1)).status_code,403)
        for value in ('forged','0','-1'):
            self.assertEqual(self.auditor.get('/home/chapter/1?home_assignment_id='+value).status_code,403)
        self.assertEqual(self.auditor.post(self.chapter(1),data={'home_assignment_id':self.aid+1}).status_code,403)
        for status in ('revoked','completed'):
            with self.app.app_context():
                row=db.session.get(HomeAssignment,self.aid);row.status=status
                row.completed_at=f.now() if status=='completed' else None;db.session.commit()
            self.assertEqual(self.auditor.get(self.chapter(1)).status_code,403)
        with self.app.app_context():
            row=db.session.get(HomeAssignment,self.aid);row.status='active';row.completed_at=None
            controller_id=HomeController.query.one().user_id
            engagement=HomeEngagement.query.one();engagement.status='completion_pending'
            engagement.completion_requested_at=f.now()-timedelta(hours=49)
            engagement.completion_deadline=engagement.completion_requested_at+timedelta(hours=48)
            engagement.completion_requested_by_user_id=controller_id
            db.session.commit()
        self.assertEqual(self.auditor.post(self.url('/home/auditor/advance/1'),data={'action':'skip'}).status_code,403)

    def test_auditor_cannot_call_teacher_enrollment_or_test_controls(self):
        for path in ('/home/create_tutor','/home/link_teacher','/home/advance/1','/home/send_certificate'):
            self.assertEqual(self.auditor.post(self.url(path)).status_code,403)
        for path in ('/test_passed_certificate','/home/bypass_chapters','/teacher/dashboard'):
            self.assertEqual(self.auditor.get(self.url(path)).status_code,403)
        self.assertEqual(self.auditor.post('/home/auditor/advance/1',data={'action':'skip'}).status_code,403)

    def test_csrf_required_for_controls_and_responses(self):
        self.app.config['WTF_CSRF_ENABLED']=True
        try:
            self.assertEqual(self.auditor.post(self.url('/home/auditor/advance/1'),data={'action':'skip'}).status_code,400)
            self.assertEqual(self.auditor.post(self.base+'/responses/longitudinal_survey',data={'intention_to_use':'Yes'}).status_code,400)
        finally:
            self.app.config['WTF_CSRF_ENABLED']=False

    def test_ordinary_participant_teacher_gate_unchanged(self):
        client=self.app.test_client();self.user('participant@example.test');self.login(client,'participant@example.test')
        self.assertEqual(client.post('/home/chapter/1').status_code,302)
        self.assertEqual(client.get('/home/chapter/11').status_code,302)
        with self.app.app_context():
            sub=HomePracticalSubmission.query.one();self.assertEqual(sub.status,'pending');sid=sub.id
            self.assertEqual(HomeProgress.query.count(),0)
        self.assertEqual(self.client.post(f'/teacher/score/{sid}',data={'decision':'not_yet_competent'}).status_code,302)
        with self.app.app_context():self.assertEqual(HomeProgress.query.count(),0)
        self.assertEqual(self.client.post(f'/teacher/score/{sid}',data={'decision':'competent'}).status_code,302)
        with self.app.app_context():self.assertEqual(HomeProgress.query.one().chapter_number,1)
        self.assertEqual(client.get('/home/chapter/11').status_code,302)
        self.assertEqual(client.post('/home/auditor/advance/1',data={'action':'skip'}).status_code,403)

    def test_evaluations_survey_independent_and_old_evidence_ignored(self):
        kinds=['experience','facilitator_evaluation','participant_evaluation','certificate','longitudinal_survey']
        self.assertEqual(list(self.board())[-5:],kinds)
        with self.app.app_context():
            row=db.session.get(HomeAssignment,self.aid)
            for kind in ('assessment','monitoring','certificate'):
                db.session.add(HomeEvidence(assignment_id=row.id,actor_id=row.auditor_id,item=kind,event='examined',details={'version':'old-bundle'}))
            db.session.commit()
        self.assertTrue(all(not self.board()[k]['examined'] for k in kinds))
        for kind,questions in (('facilitator_evaluation',instrument.FACILITATOR_QUESTIONS),('participant_evaluation',instrument.PARTICIPANT_QUESTIONS)):
            path=self.base+'/responses/'+kind
            self.assertEqual(self.auditor.get(path).status_code,200)
            self.assertEqual(self.auditor.post(path,data={}).status_code,400)
            data={key:'Yes' for key,_ in questions}
            self.assertEqual(self.auditor.post(path,data=dict(data,**{questions[0][0]:'yes'})).status_code,400)
            self.assertEqual(self.auditor.post(path,data=data).status_code,302)
            self.assertTrue(self.board()[kind]['examined'])
            if kind=='facilitator_evaluation':self.assertFalse(self.board()['participant_evaluation']['examined'])
        path=self.base+'/responses/longitudinal_survey'
        for answer in ('','Unsure','yes','false'):
            self.assertEqual(self.auditor.post(path,data={'intention_to_use':answer}).status_code,400)
        for answer in ('Yes','No'):
            self.assertEqual(self.auditor.post(path,data={'intention_to_use':answer}).status_code,302)
            self.assertTrue(self.board()['longitudinal_survey']['examined'])
        with self.app.app_context():self.assertEqual(f.h.Interaction.query.count(),0)

    def test_certificate_failure_suppression_retry_and_automatic_board_credit(self):
        aid=self.journey();target=self.url('/home/report/finish');data={'assessment_id':aid,'email':'home-a@example.test','doc_type':'certificate'}
        self.assertEqual(self.auditor.post(self.base+'/materials/certificate',data={'version':'old'}).status_code,409)
        self.assertFalse(self.board()['certificate']['examined'])
        with patch.object(participant,'_generate_home_certificate_pdf',return_value=b'%PDF-genuine-fixture') as generate, patch('app.utils.mailer.send_pdf_email',return_value=False) as send:
            self.app.config['MAIL_SUPPRESS_SEND']=True
            self.assertEqual(self.auditor.post(target,data=data).status_code,503);send.assert_not_called()
            self.app.config['MAIL_SUPPRESS_SEND']=False
            generate.return_value=None
            self.assertEqual(self.auditor.post(target,data=data).status_code,503);send.assert_not_called()
            generate.return_value=b'%PDF-genuine-fixture'
            self.assertEqual(self.auditor.post(target,data=data).status_code,503)
            self.assertFalse(self.board()['certificate']['examined'])
            send.return_value=True
            self.assertEqual(self.auditor.post(target,data=data).location,self.base+'/board')
            self.assertTrue(self.board()['certificate']['examined'])
            self.assertEqual(self.auditor.post(target,data=data).status_code,302)
        with self.app.app_context():
            event=context.latest(db.session.get(HomeAssignment,self.aid),'participant_certificate','sent')
            self.assertEqual(event.details['assessment_id'],aid)
            self.assertEqual(event.details['recipient'],'home-a@example.test')
            self.assertEqual(len(event.details['pdf_sha256']),64)
            self.assertEqual(f.h.Interaction.query.count(),0)
            self.assertEqual(f.h.auth_models.UserEnrollment.query.count(),0)

    def test_failed_final_assessment_and_unassociated_certificate_cannot_count(self):
        aid=self.journey(False)
        with patch.object(participant,'_generate_home_certificate_pdf',return_value=b'%PDF-report'),patch('app.utils.mailer.send_pdf_email',return_value=True):
            self.assertEqual(self.auditor.post(self.url('/home/report/finish'),data={'assessment_id':aid,'email':'a@example.test','doc_type':'certificate'}).status_code,302)
        self.assertFalse(self.board()['certificate']['examined'])
        with self.app.app_context():
            row=db.session.get(HomeAssignment,self.aid)
            other=HomeFinalAssessment(user_id=row.auditor_id,overall_score=100,passed=True);db.session.add(other);db.session.commit();oid=other.id
        self.assertEqual(self.auditor.post(self.url('/home/report/finish'),data={'assessment_id':oid,'email':'a@example.test'}).status_code,404)
        self.assertEqual(self.auditor.get(self.url(f'/home/report/exit?assessment_id={oid}')).status_code,404)

    def test_real_certificate_generator_and_renderer_failure(self):
        aid=self.journey()
        from flask_login import login_user
        with self.app.test_request_context('/'):
            result=db.session.get(HomeFinalAssessment,aid);login_user(db.session.get(f.h.auth_models.User,result.user_id))
            pdf=participant._generate_home_certificate_pdf(result)
            self.assertTrue(pdf.startswith(b'%PDF-'))
            with patch('xhtml2pdf.pisa.CreatePDF',return_value=type('Failed',(),{'err':1})()):
                with self.assertRaises(RuntimeError):participant._generate_home_certificate_pdf(result)


    def test_dual_authorised_identity_cannot_cross_satisfy_evidence(self):
        from flask_login import login_user
        from app.program_sace import endorsement as reading, lifecycle as reading_lc
        with self.app.app_context():
            home_row=db.session.get(HomeAssignment,self.aid)
            uid=home_row.auditor_id
            controller=HomeController.query.one().user_id
            grant=f.h.auth_models.AuthSubjectAdmin(subject_id=900,email='home-r@example.test')
            db.session.add(grant);db.session.flush()
            prov=reading_lc.event(controller,'controller_provisioned',{'fixture':True})
            pledge=reading_lc.event(controller,'admin_patent_pledge',{'fixture':True})
            engagement=reading_lc.Engagement(reference='dual-programme-test',created_by_user_id=controller,provenance_event_id=prov.id)
            db.session.add(engagement);db.session.flush()
            appointment=reading_lc.Appointment(engagement_id=engagement.id,user_id=controller,
                operational_grant_id=grant.id,grant_id_at_issue=grant.id,grant_subject_id=900,
                grant_email_at_issue='home-r@example.test',pledge_event_id=pledge.id,provisioning_event_id=prov.id)
            db.session.add(appointment);db.session.flush()
            row=f.h.Interaction(user_id=controller,activity_slug='auditor_provisioned',
                response_data=f.h.json.dumps({'status':'Claimed','claimed_by_user_id':uid,'demo_step':0}))
            db.session.add(row);db.session.flush();reading_lc.link_assignment(row,appointment,prov.id)
            rid=row.id;db.session.commit()
        self.assertEqual(self.auditor.get('/sace/reading').status_code,200)
        with self.app.test_request_context('/'):
            login_user(db.session.get(f.h.auth_models.User,uid))
            row=reading.assignment()
            for key in ('workshop_certificate','reading_certificate'):
                reading.record(row,key,{'outcome':'accepted_by_mail_sender'})
            reading.record(row,'workshop_survey',{'intention_to_use':'Yes'})
            reading.record(row,'step31',{'instrument':'facilitator-evaluation-v1'})
            reading.record(row,'step32',{'instrument':'participant-experience-v1'})
            # Even similarly named events in Reading cannot complete HOME.
            reading.record(row,'participant_certificate',{'outcome':'accepted_by_mail_sender'})
            db.session.commit()
            count=f.h.Interaction.query.count()
        self.assertTrue(all(not self.board()[k]['examined'] for k in ex.FUNCTIONAL_ITEMS))
        self.advance(1,'teacher_examined')
        self.auditor.post(self.base+'/responses/facilitator_evaluation',data={key:'Yes' for key,_ in instrument.FACILITATOR_QUESTIONS})
        self.auditor.post(self.base+'/responses/participant_evaluation',data={key:'No' for key,_ in instrument.PARTICIPANT_QUESTIONS})
        self.auditor.post(self.base+'/responses/longitudinal_survey',data={'intention_to_use':'No'})
        assessment_id=self.journey()
        with patch.object(participant,'_generate_home_certificate_pdf',return_value=b'%PDF-genuine'),patch('app.utils.mailer.send_pdf_email',return_value=True):
            self.assertEqual(self.auditor.post(self.url('/home/report/finish'),data={'assessment_id':assessment_id,'email':'a@example.test'}).status_code,302)
        self.assertTrue(self.board()['certificate']['examined'])
        with self.app.app_context():
            self.assertEqual(f.h.Interaction.query.count(),count)
            row=db.session.get(f.h.Interaction,rid)
            self.assertEqual(reading.payload(reading.latest(row,'workshop_survey'))['intention_to_use'],'Yes')
            self.assertFalse(reading.course_complete(row))
            # Removing Reading certificate events cannot expose HOME evidence as a substitute.
            f.h.Interaction.query.filter_by(workshop_session_id=reading.room(row)).filter(f.h.Interaction.activity_slug.in_(['workshop_certificate','reading_certificate'])).delete(synchronize_session=False)
            db.session.commit()
            self.assertFalse(reading.certificate_delivered(row,'reading_certificate'))
            self.assertFalse(reading.certificate_delivered(row,'workshop_certificate'))
            self.assertFalse(f.h.auth_models.AuthSubjectAdmin.query.filter_by(email='home-a@example.test').first())

    def test_fresh_assignment_and_session_context_do_not_reuse_progress(self):
        self.advance(1)
        with self.auditor.session_transaction() as state:
            state['chapter_2_done']=True
            state['home_assignment_id']=self.aid
        self.assertEqual(self.auditor.get(self.chapter(3)).status_code,409)
        other,other_id=self.join_home(self.code_home(),email='foreign-a@example.test')
        self.assertEqual(self.auditor.get(self.url('/home/chapter/1',other_id)).status_code,403)
        self.assertEqual(other.get(self.chapter(1)).status_code,403)
        self.assertEqual(other.get(self.url('/home/chapter/2',other_id)).status_code,409)
        self.assertEqual(other.post(self.url('/home/auditor/advance/1',other_id),data={'action':'skip'}).status_code,409)
        with other.session_transaction() as state:
            state['home_assignment_id']=self.aid
        self.assertEqual(other.post('/home/auditor/advance/1',data={'action':'skip'}).status_code,403)

    def test_new_completion_history_retains_new_evidence_requirements(self):
        import hashlib
        from app.models.sace_home import HomeDocument,HomeDocumentVersion
        aid=self.journey()
        with patch.object(participant,'_generate_home_certificate_pdf',return_value=b'%PDF-genuine'),patch('app.utils.mailer.send_pdf_email',return_value=True):
            self.auditor.post(self.url('/home/report/finish'),data={'assessment_id':aid,'email':'a@example.test'})
        self.auditor.post(self.base+'/summary')
        for kind,qs in [('facilitator_evaluation',instrument.FACILITATOR_QUESTIONS),('participant_evaluation',instrument.PARTICIPANT_QUESTIONS)]:
            self.auditor.post(self.base+'/responses/'+kind,data={k:'Yes' for k,_ in qs})
        self.auditor.post(self.base+'/responses/longitudinal_survey',data={'intention_to_use':'Yes'})
        for kind in ('application_form_1','application_form_2','timetable','participant_manual','facilitator_manual'):
            with self.app.app_context():
                doc=HomeDocument(kind=kind,title=kind);db.session.add(doc);db.session.flush()
                path=Path(self.documents.name)/(kind+'.pdf');path.write_bytes(b'%PDF-approved-document-fixture')
                version=HomeDocumentVersion(document_id=doc.id,version='fixture-v1',storage_key=path.name,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),approved_by=HomeController.query.one().id,
                    source_manifest={'subject':f.s.SUBJECT,'kind':kind,'home_approval':{'approved_by':'reviewer','reference':'fixture'}})
                db.session.add(version);db.session.commit();vid=version.id
            self.assertEqual(self.auditor.get(self.base+'/materials/'+kind).status_code,200)
            with self.auditor.get(f'/sace/home/documents/{vid}/content?assignment_id={self.aid}') as response:
                self.assertEqual(response.status_code,200)
            self.assertEqual(self.auditor.post(self.base+'/materials/'+kind,data={'version_id':vid}).status_code,302)
        self.assertTrue(all(x['examined'] for x in self.board().values()))
        self.assertEqual(self.auditor.post(self.base+'/completion').status_code,200)
        with self.app.app_context():
            row=db.session.get(HomeAssignment,self.aid)
            self.assertEqual(row.status,'completed')
            self.assertTrue(all(x['examined'] for x in f.s.board_items(row)))
        self.assertEqual(self.auditor.post(self.url('/home/auditor/advance/1'),data={'action':'skip'}).status_code,403)


if __name__=='__main__':unittest.main(verbosity=2)
