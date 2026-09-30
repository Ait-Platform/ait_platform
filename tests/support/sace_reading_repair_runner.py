"""Consolidated Reading repairs: real routes and temporary local PostgreSQL evidence."""
import importlib.util
from pathlib import Path
import unittest
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

    def step(self, number, **values):
        response = self.client.post("/sace/reading/demo/advance", json=dict(step=number, **values))
        self.assertEqual(response.status_code, 200, response.data)
        return response.json["next"]

    def slides(self):
        response = self.client.get("/sace/reading/simulator")
        self.assertIn(b"Workshop slide 1", response.data)
        self.assertNotIn(b"Begin the workshop", response.data)
        for number in range(1,32):
            response = self.client.get("/sace/reading/simulator")
            self.assertIn(f"Workshop slide {number}".encode(), response.data)
            self.assertNotIn(b"Back to Auditor Board", response.data)
            self.assertIn(b"Save and Continue", response.data)
            self.assertNotIn(b"rubric_vocalization", response.data)
            self.assertEqual(self.step(number), "/sace/reading/simulator")
        self.assertIsNone(self.event("step31"))

    def workshop(self):
        self.slides()
        self.step(32, ratings=dict(vocalization=3, positioning=2, pacing=3))
        self.step(33, engagement=list(flow.ENGAGEMENT))
        self.assertEqual(self.step(34, competencies={key:4 for key in r.COMPETENCIES}), "/sace/reading/step35")
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
            ("Participant / Workshop Manual", "/sace/secure_view/p_guide"),
            ("AIT IP Pledge (reference)", "/sace/secure_view/ip_pledge"),
            ("Reading Timetable (T/T)", "/sace/secure_view/timetable"),
            ("PPP: examine all 31 slides", "/sace/reading/presentation"),
            ("Forward-only Demo: 31 slides and workshop interactions", "/sace/reading/simulator"),
            ("Evaluation and Assessment", "/sace/reading/step35"),
            ("Workshop Certificate evidence", "/sace/reading/post_test/results"),
            ("18-video Reading course", "/sace/reading/course"),
            ("Reading Course Certificate evidence", "/sace/reading/course/certificate")])
        self.assertNotIn(b"Slides 1-31 are the workshop presentation.",response.data)
        for kind in ("f_guide","p_guide","timetable"):
            self.assertEqual(self.client.get("/sace/secure_view/"+kind).status_code,200)
            with self.client.get("/sace/material/"+kind+"/content") as pdf:
                self.assertEqual(pdf.status_code,200)
                self.assertTrue(pdf.data.startswith(b"%PDF-"))

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
        self.assertEqual(len(self.event("workshop_survey")["competencies"]),8)
        self.assertIsNotNone(self.event("demo_complete"))
        self.assertIsNone(self.event("step35"))  # Reading-course MCQ is independent.
        result=self.client.get("/sace/reading/post_test/results")
        self.assertIn(b"Email workshop certificate",result.data)
        self.assertEqual(self.client.get("/sace/reading/course/certificate").status_code,200)
        self.assertEqual(self.client.post("/sace/reading/course/certificate",data={"email":"a@example.test"}).status_code,409)

    def test_course_assessment_is_separate_and_unconfigured(self):
        self.workshop()
        lessons=[dict(id=i,order=i,title=f"Fixture {i}",caption="",video_filename=f"{i}.mp4") for i in range(1,19)]
        with patch.object(flow,"course_lessons",return_value=lessons):
            course=self.client.get("/sace/reading/course")
            self.assertIn(b'/sace/reading/course/assessment',course.data)
            self.assertEqual(len([a for a in Anchors(course.data).items if a["href"].startswith("/sace/reading/course/")]),19)
            self.assertEqual(self.client.get("/sace/reading/course/assessment").status_code,409)
            with self.app.app_context():
                for i in range(1,19):
                    db.session.add(h.Interaction(user_id=2,workshop_session_id=f"endorsement-{self.assignment_id}",activity_slug=f"reading_lesson_{i}_complete",response_data="{}"))
                db.session.commit()
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
        self.assertIn(b"Classroom Application",self.client.get('/sace/reading/simulator').data)
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
        self.step(32,ratings=dict(vocalization=3,positioning=3,pacing=3))
        self.assertEqual(self.client.post("/sace/reading/demo/advance",json={"step":33,"engagement":[]}).status_code,400)
        self.step(33,engagement=list(flow.ENGAGEMENT))
        self.assertEqual(self.client.post("/sace/reading/demo/advance",json={"step":34,"competencies":{}}).status_code,400)
        self.step(34,competencies={key:4 for key in r.COMPETENCIES})
        result=self.client.post("/sace/reading/step35",data={f"q{i}":"D" for i in range(1,5)})
        self.assertEqual(result.location,"/sace/reading/step35")
        self.assertFalse(self.event("step34")["passed"])
        self.assertIsNone(self.event("demo_complete"))


if __name__ == "__main__":unittest.main(verbosity=2)
