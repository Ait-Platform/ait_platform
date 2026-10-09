"""Consolidated Reading repairs: real routes and temporary local PostgreSQL evidence."""
import importlib.util
from pathlib import Path
import unittest
import re
from html import unescape
from unittest.mock import patch
from html.parser import HTMLParser


class Anchors(HTMLParser):
    def __init__(self, data, table_only=False):
        super().__init__()
        self.items, self.current, self.in_table, self.table_only = [], None, False, table_only
        self.feed(data.decode() if isinstance(data, bytes) else data)

    def handle_starttag(self, tag, attrs):
        if tag == "tbody": self.in_table = True
        if tag == "a" and (self.in_table or not self.table_only):
            self.current = {"href": dict(attrs).get("href"), "text": ""}

    def handle_data(self, data):
        if self.current is not None: self.current["text"] += data

    def handle_endtag(self, tag):
        if tag == "a" and self.current is not None:
            self.current["text"] = self.current["text"].strip()
            self.items.append(self.current)
            self.current = None
        if tag == "tbody": self.in_table = False


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("sace_repair_support", ROOT / "tests/support/sace_access_postgres_runner.py")
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
from app.extensions import db
from app.program_sace import endorsement as flow, endorsement_routes as r


class ReadingRepair(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h.AccessJourneys.setUpClass.__func__(cls)
        cls.app.static_folder = str(ROOT / "app/static")

    @classmethod
    def tearDownClass(cls):
        h.AccessJourneys.tearDownClass.__func__(cls)

    user = h.AccessJourneys.user
    login = h.AccessJourneys.login

    def setUp(self):
        h.AccessJourneys.setUp(self)
        controller = self.user("r@example.test")
        auditor = self.user("a@example.test")
        with self.app.app_context():
            row = h.Interaction(user_id=controller, activity_slug="auditor_provisioned",
                response_data=h.json.dumps(dict(status="Claimed", claimed_by_user_id=auditor, demo_step=0)))
            db.session.add(row)
            db.session.commit()
            self.assignment_id = row.id
            from app.program_sace import lifecycle as lc
            grant = h.auth_models.AuthSubjectAdmin(subject_id=900, email='r@example.test')
            db.session.add(grant)
            prov = lc.event(controller, 'controller_provisioned', {'fixture': True})
            pledge = lc.event(controller, 'admin_patent_pledge', {'fixture': True})
            engagement = lc.Engagement(reference='reading-test', created_by_user_id=controller, provenance_event_id=prov.id)
            db.session.add(engagement); db.session.flush()
            appointment = lc.Appointment(engagement_id=engagement.id, user_id=controller,
                operational_grant_id=grant.id, grant_id_at_issue=grant.id, grant_subject_id=900,
                grant_email_at_issue='r@example.test', pledge_event_id=pledge.id, provisioning_event_id=prov.id)
            db.session.add(appointment); db.session.flush()
            lc.link_assignment(row, appointment, prov.id)
            db.session.commit()
        self.login(self.client, "a@example.test", "/sace/reading")

    def event(self, slug):
        with self.app.app_context():
            row = db.session.get(h.Interaction, self.assignment_id)
            event = flow.latest(row, slug)
            return flow.payload(event) if event else None

    def certificate_html(self, page, kind):
        from app.utils.branding import get_logo_data_uri, get_seal_data_uri
        self.assertEqual(page.status_code,200)
        self.assertIn(b' sandbox srcdoc=',page.data)
        self.assertNotIn(b'src="/sace/reading/certificate-evidence/',page.data)
        html=unescape(re.search(rb'srcdoc="([^"]+)"',page.data).group(1).decode())
        with self.app.app_context(), patch.object(self.app,'root_path',str(ROOT/'app')):
            self.assertIn('src="'+get_logo_data_uri()+'"',html)
            self.assertIn('src="'+get_seal_data_uri()+'"',html)
            state=flow.payload(db.session.get(h.Interaction,self.assignment_id))
        self.assertIn(state[kind+'_certificate_id'],html)
        self.assertIn(h.datetime.fromisoformat(state[kind+'_completed_at']).strftime('%d %B %Y'),html)
        for marker in ('Archoney Institute of Technology','auth-signature','AIT Official Seal'):
            self.assertIn(marker,html)
        import os
        if os.environ.get('AIT_CERTIFICATE_VISUAL_DIR'):
            target=Path(os.environ['AIT_CERTIFICATE_VISUAL_DIR'])
            target.mkdir(parents=True,exist_ok=True)
            (target/(kind+'.html')).write_text(html,encoding='utf-8')
        return html

    def step(self, number, **values):
        response = self.client.post("/sace/reading/demo/advance", json=dict(step=number, **values))
        self.assertEqual(response.status_code, 200, response.data)
        return response.json["next"]

    def slides(self):
        response = self.client.get("/sace/reading/simulator")
        self.assertIn(b"Step 32", response.data)
        self.assertNotIn(b'id="demo-slide"', response.data)
        self.assertEqual(self.client.post("/sace/reading/demo/advance", json={"step": 31}).status_code, 409)
        self.assertIsNone(self.event("demo_slide_1"))
        self.assertIsNone(self.event("step31"))

    def workshop(self):
        self.slides()
        self.step(32, answers=dict(method_clear='Yes', activities_clear='Unsure', helpful_guidance='No'))
        self.step(33, answers=dict(reading_problem='Yes',practical_activities='Yes',oral_activities='Yes',able_to_use='Yes'))
        self.assertEqual(self.step(34, intention_to_use='Yes'), "/sace/reading/step35")
        response = self.client.get("/sace/reading/step35")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Workshop Post-Test", response.data)
        self.assertNotIn(b"Back to Auditor Board", response.data)
        response = self.client.post("/sace/reading/step35", data=dict(q1="B",q2="B",q3="C",q4="A"))
        self.assertEqual(response.location, "/sace/reading/post_test/results")

    def test_exact_board_order_and_controlled_materials(self):
        response = self.client.get("/sace/reading")
        items = [(a["text"],a["href"]) for a in Anchors(response.data, table_only=True).items]
        self.assertEqual(items, [
            ("Activity Summary", "/sace/reading/auditor-map"),
            ("Application Form 1", "/sace/secure_view/app_form"),
            ("Application Form 2", "/sace/secure_view/app_form_2"),
            ("Facilitator Manual", "/sace/secure_view/f_guide"),
            ("Workshop Manual", "/sace/secure_view/p_guide"),
            ("AIT IP Pledge (reference)", "/sace/secure_view/ip_pledge"),
            ("Reading Timetable (T/T)", "/sace/secure_view/timetable"),
            ("Workshop Online Interaction & Assessment — Steps 32–35", "/sace/reading/simulator"),
            ("18-video Reading Course", "/sace/reading/course")])
        from app.program_sace import certification as cert
        rows=re.findall(r'<tr class="border-t[^>]*>(.*?)</tr>',response.data.decode(),re.S)
        self.assertEqual(len(rows),10)
        for number, row in enumerate(rows,1):
            text=unescape(re.sub(r'<[^>]+>','',row))
            self.assertTrue(text.startswith(str(number)+'. '),text)
            self.assertIn(cert.BOARD_ITEMS[number-1][1] if number<10 else 'Certification',text)
        self.assertIn(b'aria-disabled="true">Certification',response.data)
        self.assertNotIn(b"Slides 1-31 are the workshop presentation.",response.data)
        for kind in ("f_guide","p_guide","timetable"):
            self.assertEqual(self.client.get("/sace/secure_view/"+kind).status_code,200)
            with self.client.get("/sace/material/"+kind+"/content") as pdf:
                self.assertEqual(pdf.status_code,200)
                self.assertTrue(pdf.data.startswith(b"%PDF-"))

    def recorded_board_statuses(self):
        from app.program_sace import certification as cert
        ids=[]
        with self.app.app_context():
            row=db.session.get(h.Interaction,self.assignment_id)
            for slug, *_ in cert.BOARD_ITEMS:
                event=h.Interaction(user_id=flow.payload(row)['claimed_by_user_id'],
                    workshop_session_id=flow.room(row),activity_slug=slug,
                    response_data=h.json.dumps({'evidence':'existing Board status fixture'}))
                db.session.add(event);db.session.flush();ids.append(event.id)
            db.session.commit()
        return ids

    def test_reading_certification_requires_each_existing_board_status(self):
        from app.program_sace import certification as cert
        ids=self.recorded_board_statuses()
        for eid,(slug,*_) in zip(ids,cert.BOARD_ITEMS):
            with self.app.app_context():
                db.session.get(h.Interaction,eid).activity_slug='pending_'+slug
                db.session.commit()
            self.assertEqual(self.client.get('/sace/reading/certification').status_code,409,slug)
            self.assertIn(b'aria-disabled="true">Certification',self.client.get('/sace/reading').data)
            with self.app.app_context():
                self.assertIsNone(cert.saved(db.session.get(h.Interaction,self.assignment_id)))
                db.session.get(h.Interaction,eid).activity_slug=slug
                db.session.commit()

    def test_reading_certification_freezes_board_facts_without_changing_journey(self):
        import copy
        import hashlib
        from contextlib import ExitStack
        from sqlalchemy import text
        from app.program_sace import certification as cert
        from app.utils.branding import get_logo_data_uri,get_seal_data_uri
        ids=self.recorded_board_statuses()
        target='/sace/reading/certification'
        with self.app.app_context():
            before=db.session.execute(text('SELECT id,row_to_json(e)::text FROM sace_workshop_interactions e ORDER BY id')).all()
            state_before=db.session.get(h.Interaction,self.assignment_id).response_data
        with ExitStack() as stack:
            for name in ('workshop_passed','course_complete','step35_passed','completion_requirements','refresh_progress','record'):
                stack.enter_context(patch.object(flow,name,side_effect=AssertionError('Existing flow must not be invoked')))
            stack.enter_context(patch.object(self.app,'root_path',str(ROOT/'app')))
            response=self.client.get(target)
        self.assertEqual(response.status_code,200)
        displayed=unescape(re.search(rb'srcdoc="([^"]+)"',response.data).group(1).decode())
        self.assertEqual(displayed.count('<div class="document-title">'),1)
        self.assertIn('<th>Provider</th><td colspan="3">SACE</td>',displayed)
        with self.app.app_context(),patch.object(self.app,'root_path',str(ROOT/'app')):
            evidence=cert.saved(db.session.get(h.Interaction,self.assignment_id))
            eid=evidence.id;frozen=copy.deepcopy(flow.payload(evidence));raw=evidence.response_data
            self.assertEqual(frozen['snapshot_sha256'],cert.digest(frozen['snapshot']))
            self.assertEqual(frozen['html_sha256'],hashlib.sha256(frozen['html'].encode()).hexdigest())
            self.assertEqual([item['evidence_ids'][0] for item in frozen['snapshot']['items']],ids)
            self.assertEqual([item['kind'] for item in frozen['snapshot']['items']],[item[0] for item in cert.BOARD_ITEMS])
            for item in frozen['snapshot']['items']:
                self.assertTrue(item['title'] in unescape(displayed),item['title'])
                self.assertTrue(item['examined_at'] in displayed,item['examined_at'])
                self.assertIn(item['status'],displayed)
                self.assertEqual(item['recorded_facts'],{'evidence':'existing Board status fixture'})
            for helper in (get_logo_data_uri,get_seal_data_uri):self.assertIn('src="'+helper()+'"',displayed)
            after=db.session.execute(text('SELECT id,row_to_json(e)::text FROM sace_workshop_interactions e ORDER BY id')).all()
            self.assertEqual([record for record in after if record[0]!=eid],before)
            self.assertEqual(db.session.get(h.Interaction,self.assignment_id).response_data,state_before)
        with patch.object(self.app,'root_path',str(ROOT/'app')):
            self.assertEqual(self.client.get(target).data,response.data)
        with self.app.app_context():
            self.assertEqual(db.session.get(h.Interaction,eid).response_data,raw)
            self.assertEqual(h.Interaction.query.filter_by(activity_slug=cert.SLUG).count(),1)
        board=self.client.get('/sace/reading')
        anchors=Anchors(board.data,table_only=True).items
        self.assertEqual(len(anchors),10)
        self.assertEqual(anchors[-1],{'href':target,'text':'Certification'})
        self.assertNotIn(b'Workshop Certificate evidence',board.data)
        self.assertNotIn(b'Reading Course Certificate evidence',board.data)
        self.assertEqual(self.client.post(target).status_code,405)
        import os
        if os.environ.get('AIT_CERTIFICATE_VISUAL_DIR'):
            directory=Path(os.environ['AIT_CERTIFICATE_VISUAL_DIR']);directory.mkdir(parents=True,exist_ok=True)
            (directory/'reading-certification.html').write_text(displayed,encoding='utf-8')
            (directory/'reading-board.html').write_bytes(board.data)

    def test_reading_certification_rejects_tampering_and_conflicting_snapshots(self):
        import copy
        from app.program_sace import certification as cert
        self.recorded_board_statuses();target='/sace/reading/certification'
        with patch.object(self.app,'root_path',str(ROOT/'app')):
            self.assertEqual(self.client.get(target).status_code,200)
        with self.app.app_context():
            evidence=cert.saved(db.session.get(h.Interaction,self.assignment_id))
            eid=evidence.id;raw=evidence.response_data;frozen=flow.payload(evidence)
        for field in ('html','snapshot','assignment_id','auditor_id'):
            changed=copy.deepcopy(frozen)
            if field=='html':changed['html']+='tampered'
            elif field=='snapshot':changed['snapshot']['certified_at']='tampered'
            else:
                if field=='assignment_id':changed['snapshot']['assignment_id']+=1
                else:changed['snapshot']['auditor']['id']+=1
                changed['snapshot_sha256']=cert.digest(changed['snapshot'])
            with self.app.app_context():
                db.session.get(h.Interaction,eid).response_data=h.json.dumps(changed);db.session.commit()
            with patch('app.utils.branding.get_logo_data_uri',side_effect=AssertionError('Reject before presentation')):
                self.assertEqual(self.client.get(target).status_code,409,field)
            self.assertEqual(self.client.get('/sace/reading').status_code,409,field)
            with self.app.app_context():
                db.session.get(h.Interaction,eid).response_data=raw;db.session.commit()
        with self.app.app_context():
            source=db.session.get(h.Interaction,eid)
            db.session.add(h.Interaction(user_id=source.user_id,workshop_session_id=source.workshop_session_id,
                activity_slug=source.activity_slug,response_data=source.response_data));db.session.commit()
        self.assertEqual(self.client.get(target).status_code,409)

    def test_manual_content_navigation_and_stale_timetable(self):
        from pypdf import PdfReader
        import importlib.util
        spec = importlib.util.spec_from_file_location('facilitator_source', ROOT / 'scripts/build_sace_facilitator_manual.py')
        source = importlib.util.module_from_spec(spec); spec.loader.exec_module(source)
        facilitator = PdfReader(ROOT / 'app/static/pdf/Reading_Facilitator_Manual.pdf')
        workshop = PdfReader(ROOT / 'app/static/pdf/Reading_Workshop_Manual.pdf')
        self.assertEqual(len(facilitator.pages), 31)
        self.assertEqual(len(workshop.pages), 31)
        for number, (fpage, ppage, notes) in enumerate(zip(facilitator.pages, workshop.pages, source.NOTES), 1):
            self.assertEqual(len(fpage.images), 0)
            self.assertEqual(len(ppage.images), 1)
            text = ' '.join(fpage.extract_text().split())
            self.assertIn(f'Workshop slide {number} of 31', text)
            for note in notes: self.assertIn(note, text)
            self.assertNotIn('Purpose:', ppage.extract_text())
            self.assertNotIn('Facilitator:', ppage.extract_text())
        for kind in ('app_form', 'app_form_2', 'f_guide', 'p_guide', 'timetable'):
            page = self.client.get('/sace/secure_view/' + kind)
            returns = [a for a in Anchors(page.data).items if a['href'] == '/sace/reading']
            self.assertEqual([a['text'] for a in returns], ['Return to Auditor Board'])
        self.assertNotIn(b'Back to Auditor Board', self.client.get('/sace/reading').data)
        with self.app.app_context():
            db.session.add(h.SaceDocument(slug='reading', document_type='timetable',
                file_name='stale.pdf', file_path='pdf/missing-timetable.pdf'))
            db.session.commit()
        with self.client.get('/sace/material/timetable/content') as response:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, (ROOT / 'app/static/pdf/Reading Timetable.pdf').read_bytes())
        self.assertEqual(self.client.post('/sace/material/timetable/viewed').status_code, 200)

    def test_controller_interface_and_assignment_pass(self):
        controller = self.app.test_client()
        self.login(controller, 'r@example.test', '/sace/provisioning')
        page = controller.get('/sace/provisioning')
        self.assertNotIn(b'/sace/reading/lifecycle', page.data)
        self.assertNotIn(b'Engagement and appointments', page.data)
        code, _ = h.AccessJourneys.code(self, controller)
        from flask import url_for
        with self.app.test_request_context():
            url = url_for('sace_bp.print_access_slip', code=code)
        page = controller.get(url)
        self.assertEqual(page.status_code, 200)
        self.assertIn(b'This access pass is issued for this Reading endorsement review', page.data)
        self.assertIn(b'appointed SACE Evaluator only', page.data)

    def test_summary_pledge_and_ppp_remain_separate(self):
        pledge = (ROOT/"templates/program_sace/auditor_pledge.html").read_text(encoding="utf-8")
        self.assertNotIn("About AIT Submission",pledge)
        response=self.client.get("/sace/reading/auditor-map")
        self.assertEqual(Anchors(response.data).items,[])
        self.assertIsNone(self.event("map_reviewed"))
        self.assertEqual(self.client.post("/sace/reading/auditor-map").location,"/sace/reading")
        self.assertIsNotNone(self.event("map_reviewed"))
        self.assertIn(b"Examined",self.client.get("/sace/reading").data)
        ppp=self.client.get("/sace/reading/presentation")
        for label in (b"Previous",b"Next",b"Understood"):self.assertIn(label,ppp.data)
        self.assertNotIn(b"Return to Board",ppp.data)
        for i in range(1,32):
            with self.client.get(f"/sace/reading/slide/{i}") as image:
                self.assertEqual(image.status_code,200)
            self.assertEqual(self.client.post(f"/sace/reading/presentation/viewed/{i}").status_code,200)
        self.assertEqual(self.client.get("/sace/reading/presentation/complete").status_code,302)
        self.assertIsNotNone(self.event("ppp_complete"))
        self.assertIsNone(self.event("demo_slide_1"))

    def test_full_workshop_sequence_and_certificate_boundary(self):
        self.workshop()
        self.assertEqual(self.event("step34")["score"],100)
        self.assertNotIn("baseline", self.event("workshop_survey"))
        self.assertEqual(self.event("workshop_survey")["intention_to_use"], "Yes")
        self.assertIsNotNone(self.event("demo_complete"))
        self.assertIsNone(self.event("step35"))  # Reading-course MCQ is independent.
        result=self.client.get("/sace/reading/post_test/results")
        self.assertIn(b"Email workshop certificate",result.data)
        self.assertEqual(self.client.get("/sace/reading/course/certificate").status_code,200)
        self.assertEqual(self.client.post("/sace/reading/course/certificate",data={"email":"a@example.test"}).status_code,409)

    def test_new_instruments_required_answers_optional_comment_and_no_scores(self):
        from app.program_sace import workshop_interactions as instrument
        self.slides()
        answers = {key: 'No' for key, _ in instrument.FACILITATOR_QUESTIONS}
        for invalid in ({}, dict(answers, method_clear='Maybe'), dict(answers, method_clear=1)):
            self.assertEqual(self.client.post('/sace/reading/demo/advance', json={'step':32,'answers':invalid}).status_code,400)
            self.assertIsNone(self.event('step31'))
        self.step(32, answers=answers, comment='  Clear examples helped.  ')
        self.assertEqual(self.event('step31')['comment'],'Clear examples helped.')
        self.assertNotIn('score',self.event('step31'))
        answers = {key: 'Unsure' for key, _ in instrument.EXPERIENCE_QUESTIONS}
        incomplete = dict(answers); incomplete.pop('able_to_use')
        self.assertEqual(self.client.post('/sace/reading/demo/advance',json={'step':33,'answers':incomplete}).status_code,400)
        self.step(33,answers=answers)
        self.assertNotIn('score',self.event('step32'))
        for value in ('Unsure',None,1):
            self.assertEqual(self.client.post('/sace/reading/demo/advance',json={'step':34,'intention_to_use':value}).status_code,400)
        self.step(34,intention_to_use='No')
        self.assertEqual(self.event('step33')['intention_to_use'],'No')
        self.assertNotIn('score',self.event('step33'))
        self.assertIsNone(self.event('demo_complete'))
        self.assertEqual(self.client.post('/sace/reading/demo/advance',json={'step':34}).status_code,409)
        response=self.client.post('/sace/reading/step35',data=dict(q1='B',q2='B',q3='C',q4='A'))
        self.assertEqual(response.location,'/sace/reading/post_test/results')
        self.assertEqual(self.event('step34')['score'],100)
        self.assertIsNotNone(self.event('demo_complete'))
        self.assertIn(b'Email workshop certificate',self.client.get('/sace/reading/post_test/results').data)

    def test_empty_optional_comment_and_intention_only(self):
        from app.program_sace import workshop_interactions as instrument
        self.slides()
        self.step(32,answers={key:'Yes' for key,_ in instrument.FACILITATOR_QUESTIONS})
        self.assertEqual(self.event('step31')['comment'],'')
        self.step(33,answers={key:'No' for key,_ in instrument.EXPERIENCE_QUESTIONS})
        page=self.client.get('/sace/reading/simulator')
        self.assertIn(b'Longitudinal Research',page.data)
        for field in ('province','school','district','grades','workshop_date','cohort','teaching_experience'):
            self.assertNotIn(('name="'+field+'"').encode(),page.data)
        self.assertEqual(page.data.count(b'name="intention_to_use"'),2)
        self.assertNotIn(b'value="Unsure"',page.data)
        # Extra client data is never copied into endorsement evidence.
        self.step(34,intention_to_use='Yes',baseline={'province':'Gauteng'},school='Ignored')
        for slug in ('step33','workshop_survey'):
            event=self.event(slug)
            self.assertEqual(event['instrument'],'reading-longitudinal-intention-v1')
            self.assertEqual(event['intention_to_use'],'Yes')
            self.assertEqual(set(event),{'instrument','intention_to_use','recorded_at','responding_user_id','context'})
        with self.app.app_context():
            self.assertEqual(h.Interaction.query.filter_by(activity_slug='reading_research_followup').count(),0)

    def test_real_workshop_certificate_requires_all_steps_and_uses_registered_email(self):
        import sys
        from types import SimpleNamespace
        from unittest.mock import Mock
        import app.program_sace.routes as routes
        sender=Mock(return_value=True)
        with patch.dict(sys.modules,{'app.subject_reading.routes':SimpleNamespace(_email_certificate_pdf=sender)}), patch.object(routes,'_generate_sace_certificate_pdf',return_value=b'%PDF-real-workshop') as generate:
            self.slides()
            values=[(32,dict(answers=dict(method_clear='Yes',activities_clear='Yes',helpful_guidance='Yes'))),
                (33,dict(answers=dict(reading_problem='Yes',practical_activities='Yes',oral_activities='Yes',able_to_use='Yes'))),
                (34,dict(intention_to_use='Yes'))]
            for number,data in values:
                self.assertEqual(self.client.post('/sace/reading/certificate/email').status_code,409)
                self.assertEqual(self.client.get('/sace/reading/certificate-evidence/workshop').status_code,409)
                self.step(number,**data)
            self.assertEqual(self.client.post('/sace/reading/certificate/email').status_code,409)
            self.client.post('/sace/reading/step35',data={f'q{i}':'D' for i in range(1,5)})
            self.assertEqual(self.client.post('/sace/reading/certificate/email').status_code,409)
            generate.assert_not_called(); sender.assert_not_called()
            self.client.post('/sace/reading/step35',data=dict(q1='B',q2='B',q3='C',q4='D'))
            self.assertEqual(self.event('step34')['score'],75)  # Existing 70% pass rule.
            board=self.client.get('/sace/reading')
            self.assertNotIn(b'Workshop Certificate evidence',board.data)
            link='/sace/reading/post_test/results?evidence=1'  # Historical functionality remains available.
            with patch.object(self.app,'root_path',str(ROOT/'app')):
                page=self.client.get(link)
            html=self.certificate_html(page,'workshop')
            self.assertIn('Reading Workshop Certificate',html)
            self.assertIn('Overall Result: 75%',html)
            generate.assert_not_called()
            self.assertIn(b'Email workshop certificate',page.data)
            self.assertNotIn(b'SPECIMEN',page.data)
            result=self.client.get('/sace/reading/post_test/results')
            self.assertNotIn(b'/sace/reading/course',result.data)
            pdf=self.client.get('/sace/reading/certificate-evidence/workshop')
            self.assertEqual(pdf.data,b'%PDF-real-workshop')
            self.assertTrue(generate.call_args.args[0].startswith('AIT-WS-'))
            with self.app.app_context():
                self.assertEqual(db.session.get(h.auth_models.User,generate.call_args.args[3]).email,'a@example.test')
            cid=generate.call_args.args[0]
            self.assertIn(cid,html)
            sender.assert_not_called()
            self.assertIsNone(self.event('workshop_certificate'))
            response=self.client.post('/sace/reading/certificate/email',data={'email':'someone-else@example.test'})
            self.assertEqual(response.location,'/sace/reading')
            sender.assert_called_once_with('a@example.test',generate.call_args.args[1],cid,b'%PDF-real-workshop')
            self.assertEqual(self.event('workshop_certificate')['outcome'],'accepted_by_mail_sender')
            self.assertNotIn(b'Workshop Certificate evidence',self.client.get('/sace/reading').data)
            self.assertIsNone(self.event('reading_certificate'))
            self.assertEqual(self.client.post('/sace/reading/course/certificate').status_code,409)

    def test_real_course_certificate_requires_each_video_and_separate_assessment(self):
        import sys
        from types import SimpleNamespace
        from unittest.mock import Mock
        self.workshop()
        lessons=[dict(id=i,order=i,title=f'Fixture {i}',caption='',video_filename=f'{i}.mp4') for i in range(1,19)]
        sender=Mock(return_value=True); generate=Mock(return_value=b'%PDF-real-reading')
        content={'version':'certificate-test-v1','pass_percent':100,'questions':[{'id':'q1','prompt':'Fixture','options':{'A':'one','B':'two'},'answer':'B'}]}
        with patch.dict(self.app.config,{'AIT_READING_STEP35':content}), patch.object(flow,'course_lessons',return_value=lessons), patch.dict(sys.modules,{'app.subject_reading.routes':SimpleNamespace(_email_certificate_pdf=sender,_generate_certificate_pdf=generate)}):
            for i in range(1,19):
                self.assertEqual(self.client.post('/sace/reading/course/certificate').status_code,409)
                self.assertEqual(self.client.get('/sace/reading/certificate-evidence/reading').status_code,409)
                self.assertEqual(self.client.post(f'/sace/reading/course/{i}',json={'examined':True}).status_code,409)
                with patch('app.utils.reading_media.verify_reading_video'):
                    self.client.get(f'/sace/reading/course/{i}/video')
                self.assertEqual(self.client.post(f'/sace/reading/course/{i}',json={'examined':True}).status_code,200)
            self.assertEqual(self.client.post('/sace/reading/course/certificate').status_code,409)
            generate.assert_not_called(); sender.assert_not_called()
            certificate=self.client.get('/sace/reading/course/certificate')
            self.assertIn(b'/sace/reading/course/assessment',certificate.data)
            self.client.post('/sace/reading/course/assessment',data={'version':content['version'],'q1':'A'})
            self.assertEqual(self.client.post('/sace/reading/course/certificate').status_code,409)
            continuation=self.client.post('/sace/reading/course/assessment',data={'version':content['version'],'q1':'B'})
            self.assertEqual(continuation.location,'/sace/reading/course/certificate')
            with patch.dict(self.app.config,{'AIT_READING_STEP35':dict(content,version='new-version')}):
                self.assertEqual(self.client.post('/sace/reading/course/certificate').status_code,409)
            with patch.object(self.app,'root_path',str(ROOT/'app')):
                page=self.client.get('/sace/reading/course/certificate')
            html=self.certificate_html(page,'reading')
            self.assertIn('Completion Certificate',html)
            self.assertIn('Lessons 1 to 18',html)
            self.assertIn(b'Email Reading certificate',page.data)
            generate.assert_not_called()
            self.assertEqual(self.client.get('/sace/reading/certificate-evidence/reading').data,b'%PDF-real-reading')
            cid=generate.call_args.args[0]
            self.assertIn(cid,html)
            self.assertTrue(cid.startswith('AIT-RD-'))
            with self.app.app_context():
                self.assertEqual(db.session.get(h.auth_models.User,generate.call_args.args[3]).email,'a@example.test')
            self.assertEqual(self.client.post('/sace/reading/course/certificate').location,'/sace/reading')
            sender.assert_called_once_with('a@example.test',generate.call_args.args[1],cid,b'%PDF-real-reading')
            self.assertEqual(self.event('reading_certificate')['outcome'],'accepted_by_mail_sender')
            self.assertIsNone(self.event('workshop_certificate'))
            with self.app.app_context():
                row=db.session.get(h.Interaction,self.assignment_id)
                self.assertFalse(flow.certificate_delivered(row,'workshop_certificate'))
                self.assertTrue(flow.certificate_delivered(row,'reading_certificate'))

    def test_failed_delivery_and_specimen_confirmation_do_not_complete_board(self):
        import sys
        from types import SimpleNamespace
        from unittest.mock import Mock
        import app.program_sace.routes as routes
        self.workshop()
        with self.app.app_context():
            row=db.session.get(h.Interaction,self.assignment_id)
            db.session.add(h.Interaction(user_id=flow.payload(row)['claimed_by_user_id'],workshop_session_id=flow.room(row),activity_slug='workshop_certificate',response_data=h.json.dumps({'evidence':'participant certificate specimen examined'})))
            db.session.commit()
            self.assertIn('workshop_certificate',flow.completion_requirements(row))
        board=self.client.get('/sace/reading')
        self.assertNotIn(b'Workshop Certificate evidence',board.data)
        self.assertIn(b'aria-disabled="true">Certification',board.data)
        sender=Mock(return_value=False)
        with patch.dict(sys.modules,{'app.subject_reading.routes':SimpleNamespace(_email_certificate_pdf=sender)}), patch.object(routes,'_generate_sace_certificate_pdf',return_value=b'%PDF-real') as generate:
            self.assertEqual(self.client.post('/sace/reading/certificate/email').status_code,503)
            self.assertEqual(self.event('workshop_certificate_failed')['reason'],'delivery')
            with self.app.app_context():
                self.assertFalse(flow.certificate_delivered(db.session.get(h.Interaction,self.assignment_id),'workshop_certificate'))
            sender.return_value=True
            with patch.dict(self.app.config,{'MAIL_SUPPRESS_SEND':True}):
                self.assertEqual(self.client.post('/sace/reading/certificate/email').status_code,503)
            self.assertEqual(sender.call_count,1)
            generate.return_value=b''
            self.assertEqual(self.client.post('/sace/reading/certificate/email').status_code,503)
            self.assertEqual(self.event('workshop_certificate_failed')['reason'],'generation')
            generate.return_value=b'%PDF-real'
            self.assertEqual(self.client.post('/sace/reading/certificate/email').status_code,302)
            self.assertEqual(self.event('workshop_certificate')['outcome'],'accepted_by_mail_sender')

    def test_existing_certificate_helper_calls_standard_platform_sender(self):
        import ast, sys
        from types import SimpleNamespace
        from unittest.mock import Mock
        tree=ast.parse((ROOT/'app/subject_reading/routes.py').read_text(encoding='utf-8-sig'))
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_email_certificate_pdf')
        namespace={'current_app':h.flask.current_app}
        exec(compile(ast.Module(body=[function],type_ignores=[]),'standard-certificate-helper','exec'),namespace)
        sender=Mock(return_value=True)
        with self.app.app_context(), patch.dict(sys.modules,{'app.utils.mailer':SimpleNamespace(send_pdf_email=sender)}):
            self.assertTrue(namespace['_email_certificate_pdf']('a@example.test','Auditor','AIT-WS-test',b'%PDF-real'))
            self.assertEqual(sender.call_args.kwargs['to_email'],'a@example.test')
            self.assertEqual(sender.call_args.kwargs['pdf_bytes'],b'%PDF-real')
            sender.side_effect=RuntimeError('test-only transport failure')
            self.assertFalse(namespace['_email_certificate_pdf']('a@example.test','Auditor','AIT-WS-test',b'%PDF-real'))

    def test_auditor_video_examination_needs_no_elapsed_playback(self):
        lessons=[dict(id=i,order=i,title=f'Fixture {i}',caption='',video_filename=f'{i}.mp4') for i in range(1,19)]
        with patch.object(flow,'course_lessons',return_value=lessons):
            page=self.client.get('/sace/reading/course/1')
            self.assertIn(b'controls preload="metadata"',page.data)
            self.assertIn(b'Mark video examined',page.data)
            for restriction in (b'onseeking',b'ontimeupdate',b'currentTime=',b'watched',b'onended'):
                self.assertNotIn(restriction,page.data)
            self.assertEqual(self.client.post('/sace/reading/course/1',json={'examined':True}).status_code,409)
            with patch('app.utils.reading_media.verify_reading_video'):
                self.assertEqual(self.client.get('/sace/reading/course/1/video').status_code,302)
                self.assertEqual(self.client.get('/sace/reading/course/2/video').status_code,302)
            # Served material can be examined immediately, without elapsed time or an ended event.
            self.assertEqual(self.client.post('/sace/reading/course/2',json={'examined':True}).status_code,409)
            self.assertEqual(self.client.post('/sace/reading/course/1',json={'examined':True}).status_code,200)
            self.assertEqual(self.client.post('/sace/reading/course/2',json={'examined':True}).status_code,200)
            self.assertEqual(self.event('reading_lesson_1_complete')['evidence'],'Auditor confirmed video examination')

    def test_separate_participant_reading_guard_remains_unaffected(self):
        import ast
        from types import SimpleNamespace
        tree=ast.parse((ROOT/'app/subject_reading/routes.py').read_text(encoding='utf-8-sig'))
        guard=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_auditor_reading_journey')
        guard.decorator_list=[]
        namespace={'current_user':SimpleNamespace(is_authenticated=True),'redirect':h.flask.redirect,'url_for':h.flask.url_for,'request':h.flask.request}
        exec(compile(ast.Module(body=[guard],type_ignores=[]),'participant-guard','exec'),namespace)
        with self.app.test_request_context('/reading/certificate'):
            with patch.object(flow,'assignments',return_value=[]), patch.object(flow,'assignment') as assignment:
                self.assertIsNone(namespace['_auditor_reading_journey']())
                assignment.assert_not_called()

    def test_genuine_participant_certificate_still_generates_and_emails(self):
        import ast
        from types import SimpleNamespace
        from unittest.mock import Mock
        tree=ast.parse((ROOT/'app/subject_reading/routes.py').read_text(encoding='utf-8-sig'))
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_finalize_and_send_certificate')
        function.decorator_list=[]
        enrollment=SimpleNamespace(certificate_id='PARTICIPANT-123',completed_at=h.datetime(2026,1,1))
        generate=Mock(return_value=b'%PDF-participant')
        sender=Mock(return_value=True)
        persistence=SimpleNamespace(session=Mock())
        namespace={'_get_enrollment':lambda:enrollment,'db':persistence,'sa_text':h.text,
            'current_user':SimpleNamespace(name='Participant',email='p@example.test'),
            '_generate_certificate_pdf':generate,'_email_certificate_pdf':sender,'send_file':h.flask.send_file}
        exec(compile(ast.Module(body=[function],type_ignores=[]),'participant-certificate','exec'),namespace)
        with self.app.test_request_context('/reading/certificate'):
            response=namespace['_finalize_and_send_certificate'](77)
            self.assertEqual(response.mimetype,'application/pdf')
            response.close()
        generate.assert_called_once_with(certificate_id='PARTICIPANT-123',learner_name='Participant',completed_at=enrollment.completed_at,user_id=77)
        sender.assert_called_once_with(to_email='p@example.test',learner_name='Participant',certificate_id='PARTICIPANT-123',pdf_bytes=b'%PDF-participant')
        persistence.session.commit.assert_called_once()

    def test_course_assessment_is_separate_and_unconfigured(self):
        self.workshop()
        lessons=[dict(id=i,order=i,title=f"Fixture {i}",caption="",video_filename=f"{i}.mp4") for i in range(1,19)]
        with patch.object(flow,"course_lessons",return_value=lessons):
            course=self.client.get("/sace/reading/course")
            self.assertNotIn(b'/sace/reading/course/assessment',course.data)
            self.assertIn(b'<h2 class="text-xl font-semibold">I Learn to Read English Using the LITRE Method</h2>',course.data)
            self.assertEqual(len([a for a in Anchors(course.data).items if a["href"].startswith("/sace/reading/course/")]),18)
            self.assertEqual(self.client.get("/sace/reading/course/assessment").status_code,409)
            with self.app.app_context():
                for i in range(1,19):
                    db.session.add(h.Interaction(user_id=2,workshop_session_id=f"endorsement-{self.assignment_id}",activity_slug=f"reading_lesson_{i}_complete",response_data="{}"))
                db.session.commit()
            evidence=self.client.get('/sace/reading/course/certificate')
            self.assertIn(b'/sace/reading/course/assessment',evidence.data)
            result=self.client.get("/sace/reading/course/assessment")
            self.assertEqual(result.status_code,200)
            self.assertIn(b"questions have not yet been supplied",result.data)
            self.assertEqual(self.client.post("/sace/reading/course/assessment").status_code,409)
            self.assertIsNone(self.event("step35"))

    def test_existing_positions_and_earned_workshop_evidence_survive(self):
        with self.app.app_context():
            row=db.session.get(h.Interaction,self.assignment_id)
            state=flow.payload(row)
            state['demo_step']=32
            row.response_data=h.json.dumps(state)
            db.session.add(h.Interaction(user_id=2,workshop_session_id=flow.room(row),activity_slug='step31',response_data='{"vocalization":3,"positioning":3,"pacing":3}'))
            db.session.commit()
        self.assertIn(b"Participant Workshop Experience",self.client.get('/sace/reading/simulator').data)
        self.assertEqual(self.event('step31')['vocalization'],3)
        with self.app.app_context():
            row=db.session.get(h.Interaction,self.assignment_id)
            state=flow.payload(row)
            state.pop('demo_sequence',None)
            state['demo_step']=35
            row.response_data=h.json.dumps(state)
            for slug,values in [('step32',{'engagement':list(flow.ENGAGEMENT)}),('step33',{'instrument':'existing-baseline-v1'}),('step34',{'score':100,'passed':True}),('workshop_certificate',{'certificate_id':'EXISTING'})]:
                db.session.add(h.Interaction(user_id=2,workshop_session_id=flow.room(row),activity_slug=slug,response_data=h.json.dumps(values)))
            db.session.commit()
        self.assertEqual(self.client.get('/sace/reading/simulator').location,'/sace/reading/post_test/results')
        self.assertEqual(self.event('step33')['instrument'],'existing-baseline-v1')
        self.assertEqual(self.event('workshop_certificate')['certificate_id'],'EXISTING')
        self.assertIn(b'Email workshop certificate',self.client.get('/sace/reading/post_test/results').data)

    def test_no_skips_replays_or_incomplete_forms(self):
        self.client.get("/sace/reading/simulator")
        self.assertEqual(self.client.post("/sace/reading/demo/advance",json={"step":31}).status_code,409)
        self.slides()
        self.assertEqual(self.client.post("/sace/reading/demo/advance",json={"step":31}).status_code,409)
        self.assertEqual(self.client.post("/sace/reading/demo/advance",json={"step":32}).status_code,400)
        self.step(32,answers=dict(method_clear='Yes',activities_clear='Yes',helpful_guidance='Yes'))
        self.assertEqual(self.client.post("/sace/reading/demo/advance",json={"step":33,"engagement":[]}).status_code,400)
        self.step(33,answers=dict(reading_problem='Yes',practical_activities='Yes',oral_activities='Yes',able_to_use='Yes'))
        self.assertEqual(self.client.post("/sace/reading/demo/advance",json={"step":34,"competencies":{}}).status_code,400)
        self.step(34,intention_to_use='Yes')
        result=self.client.post("/sace/reading/step35",data={f"q{i}":"D" for i in range(1,5)})
        self.assertEqual(result.location,"/sace/reading/step35")
        self.assertFalse(self.event("step34")["passed"])
        self.assertIsNone(self.event("demo_complete"))


    def test_workshop_certificate_has_actual_ait_semantics(self):
        from flask import render_template
        with self.app.test_request_context('/'):
            html=render_template('program_sace/post_test/certificate_pdf.html',
                learner_name='Actual Auditor',completed_date='5 October 2026',certificate_id='AIT-WS-REAL',
                logo_path='',seal_path='',answers={'score':100})
        self.assertIn('Reading Workshop Certificate',html)
        self.assertIn('passed the required workshop post-test',html)
        self.assertNotIn('Workshop Simulation',html)
        self.assertNotIn('Sandton Convention Centre',html)
        self.assertNotIn('Sace activity',html)


if __name__ == "__main__":unittest.main(verbosity=2)
