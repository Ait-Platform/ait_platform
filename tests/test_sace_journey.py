"""Focused SACE journey tests; run python -B tests/test_sace_journey.py.

Loads actual function definitions without the app factory, whose startup performs
DB writes. Persistence is mocked; no SQLite or production database is used.
"""
import ast
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import flask
from flask import Flask
from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
from werkzeug.exceptions import HTTPException

ROOT = Path(__file__).resolve().parents[1]

def functions(path, env):
    tree = ast.parse((ROOT/path).read_text(encoding='utf-8'))
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.Assign)):
            if isinstance(node, ast.FunctionDef):
                node.decorator_list = []
            nodes.append(node)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), env)
    return env

class JourneyTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = 'test'
        self.app.static_folder = str(ROOT/'app/static')
        self.row = SimpleNamespace(id=41, user_id=7, response_data=json.dumps({'status':'Claimed','demo_step':0}))
        self.evidence = []
        self.db = SimpleNamespace(session=Mock())
        env = dict(json=json, abort=flask.abort, current_app=flask.current_app, datetime=datetime, timezone=timezone)
        self.f = functions('app/program_sace/endorsement.py', env)
        self.f.update(latest=self.latest, course_lessons=lambda:[{'id':i,'order':i} for i in range(1,19)])
        self.flow = SimpleNamespace(**{key:val for key,val in self.f.items() if not key.startswith('__')})
        self.flow.assignment = self.assignment
        self.flow.assignments = lambda: [self.row]
        self.flow.record = self.record
        self.flow.save = lambda row,state:setattr(row,'response_data',json.dumps(state))
        self.flow.events = lambda row:self.evidence
        self.flow.is_controller = lambda:False
        env = dict(flow=self.flow, db=self.db, Path=Path, json=json, current_user=SimpleNamespace(id=9,name='Auditor',email='auditor@example.test',is_authenticated=True))
        for name in ('abort','current_app','g','jsonify','redirect','request','session','make_response'):
            env[name] = getattr(flask,name)
        env['url_for'] = lambda endpoint,**kw:'/'+endpoint
        env['render_template'] = lambda name,**kw:json.dumps({'template':name,**kw},default=str)
        self.r = functions('app/program_sace/endorsement_routes.py',env)

    def latest(self,row,slug):
        return next((e for e in reversed(self.evidence) if e.activity_slug==slug),None)

    def assignment(self, lock=False, active=True):
        if active and self.flow.payload(self.row)['status']!='Claimed':
            flask.abort(403)
        return self.row

    def record(self,row,slug,values=None,once=False):
        old=self.latest(row,slug)
        if once and old:return old
        event=SimpleNamespace(id=len(self.evidence)+1,activity_slug=slug,response_data=json.dumps(values or {}))
        self.evidence.append(event)
        return event

    def add(self,slug,**data):self.record(self.row,slug,data)
    def state(self,step):self.flow.save(self.row,{'status':'Claimed','demo_step':step})
    def call(self,name,method='GET',data=None,json_data=None,**kw):
        with self.app.test_request_context('/',method=method,data=data,json=json_data):
            return self.r[name](**kw)
    def fails(self,code,name,**kw):
        with self.assertRaises(HTTPException) as caught:self.call(name,**kw)
        self.assertEqual(code,caught.exception.code)
    def workshop(self):
        self.state(35)
        for slug in ('map_complete','ppp_complete','demo_complete','step31','step33'):self.add(slug)
        self.add('step32',engagement=list(self.flow.ENGAGEMENT))
        self.add('step34',passed=True)
    def course(self):
        for i in range(1,19):self.add(f'reading_lesson_{i}_complete')
    def ready(self):
        self.workshop();self.course()
        for slug in self.flow.MAP_REQUIRED+('workshop_certificate','reading_complete','reading_certificate','board_returned'):self.add(slug)
        self.app.config['AIT_READING_STEP35']={'version':'supplied-v1'}
        self.add('step35',passed=True,version='supplied-v1')

    def test_empty_journey_cannot_close(self):
        self.fails(409,'finish_evaluation',method='POST')
        self.assertEqual('Claimed',self.flow.payload(self.row)['status'])
        self.assertIsNone(self.latest(self.row,'evaluation_complete'))

    def test_full_journey_notifies_then_closes_without_rubric(self):
        self.ready()
        result=self.call('finish_evaluation',method='POST')
        self.assertIn('evaluation_closed',result)
        self.assertEqual('Completed',self.flow.payload(self.row)['status'])
        self.assertEqual(['evaluation_complete','controller_notification','assignment_closed'],[e.activity_slug for e in self.evidence[-3:]])
        self.assertEqual(self.flow.FINAL_MESSAGE,self.flow.payload(self.evidence[-2])['message'])
        self.assertEqual(1,self.db.session.commit.call_count)
        before=len(self.evidence)
        self.call('finish_evaluation',method='POST')
        self.assertEqual(before,len(self.evidence))
        self.fails(403,'board')

    def test_each_required_activity_blocks_completion(self):
        self.ready()
        for slug in self.flow.MAP_REQUIRED+('map_complete','ppp_complete','demo_complete','step31','step32','step33','step34','workshop_certificate','reading_complete','reading_certificate','board_returned','step35','reading_lesson_18_complete'):
            with self.subTest(slug=slug):
                saved=self.evidence[:]
                self.evidence[:]=[e for e in saved if e.activity_slug!=slug]
                self.fails(409,'finish_evaluation',method='POST')
                self.evidence[:]=saved

    def test_step35_missing_content_never_records_pass(self):
        self.workshop();self.course()
        self.fails(409,'step35',method='POST')
        self.assertIsNone(self.latest(self.row,'step35'))

    def test_supplied_mcq_scores_on_server_and_requires_current_version(self):
        self.workshop();self.course()
        self.app.config['AIT_READING_STEP35']={'version':'v1','pass_percent':100,'questions':[{'id':'q1','prompt':'Test fixture only','options':{'A':'one','B':'two'},'answer':'B'}]}
        self.call('step35',method='POST',data={'version':'v1','q1':'A','score':'100','passed':'true'})
        self.assertFalse(self.flow.payload(self.latest(self.row,'step35'))['passed'])
        self.fails(409,'step35',method='POST',data={'version':'old','q1':'B'})
        self.call('step35',method='POST',data={'version':'v1','q1':'B'})
        with self.app.app_context():self.assertTrue(self.flow.step35_passed(self.row))
        self.app.config['AIT_READING_STEP35']['version']='v2'
        with self.app.app_context():self.assertFalse(self.flow.step35_passed(self.row))

    def test_demo_blocks_skip_replay_and_invalid_payload(self):
        self.add('map_complete');self.add('ppp_complete')
        self.fails(409,'demo_advance',method='POST',json_data={'step':4})
        self.call('demo_advance',method='POST',json_data={'step':0})
        self.fails(409,'demo_advance',method='POST',json_data={'step':0})
        self.fails(409,'demo_advance',method='POST',json_data=['invalid'])
        self.assertEqual(1,self.flow.payload(self.row)['demo_step'])

    def test_engagement_requires_actual_responses(self):
        self.add('map_complete');self.add('ppp_complete');self.state(32)
        self.fails(400,'demo_advance',method='POST',json_data={'step':32,'engagement':list(self.flow.ENGAGEMENT)})
        self.assertIsNone(self.latest(self.row,'step32'))

    def test_failed_workshop_mcq_cannot_unlock_certificate(self):
        self.state(34)
        for i in (31,32,33):self.add(f'step{i}')
        self.call('mark_workshop',method='POST',data={f'q{i}':'D' for i in range(1,5)})
        self.assertEqual(34,self.flow.payload(self.row)['demo_step'])
        self.assertFalse(self.flow.workshop_passed(self.row))

    def test_missing_reading_course_blocks_certificate_and_audits_denial(self):
        self.fails(409,'reading_certificate',method='POST')
        self.assertIsNotNone(self.latest(self.row,'reading_certificate_blocked'))
        self.assertIsNone(self.latest(self.row,'reading_certificate'))

    def test_reading_cannot_complete_unserved_or_later_video(self):
        self.workshop()
        self.fails(409,'reading_lesson',method='POST',data={'completed':'yes'},lesson_id=1)
        self.fails(409,'reading_lesson',lesson_id=2)
        self.assertIsNone(self.latest(self.row,'reading_lesson_1_complete'))

    def test_ppp_requires_all_slides(self):
        for i in range(1,30):self.add(f'ppp_slide_{i}')
        self.fails(409,'ppp_complete')
        self.add('ppp_slide_30');self.call('ppp_complete')
        self.assertIsNotNone(self.latest(self.row,'ppp_complete'))

    def test_csv_formula_cells_escaped(self):
        self.assertEqual("'=1+1",self.r['csv_cell']('=1+1'))
        self.assertEqual('plain',self.r['csv_cell']('plain'))

    def test_certificate_delivery_failure_and_retry(self):
        import sys
        from unittest.mock import patch
        sender=Mock(return_value=False)
        module=SimpleNamespace(_email_certificate_pdf=sender)
        with patch.dict(sys.modules, {'app.subject_reading.routes':module}):
            self.fails(503,'deliver_certificate',method='POST',data={'email':'a@example.test'},row=self.row,slug='workshop_certificate',certificate_id='TEST',pdf=b'pdf')
            self.assertIsNone(self.latest(self.row,'workshop_certificate'))
            self.assertIsNotNone(self.latest(self.row,'workshop_certificate_failed'))
            sender.return_value=True
            self.call('deliver_certificate',method='POST',data={'email':'a@example.test'},row=self.row,slug='workshop_certificate',certificate_id='TEST',pdf=b'pdf')
            self.assertIsNotNone(self.latest(self.row,'workshop_certificate'))

    def test_suppressed_certificate_is_never_sent_or_credited(self):
        import sys
        from unittest.mock import patch
        sender=Mock(return_value=True)
        self.app.config['MAIL_SUPPRESS_SEND']=True
        with patch.dict(sys.modules, {'app.subject_reading.routes':SimpleNamespace(_email_certificate_pdf=sender)}):
            self.fails(503,'deliver_certificate',method='POST',data={'email':'a@example.test'},row=self.row,slug='reading_certificate',certificate_id='TEST',pdf=b'pdf')
        sender.assert_not_called()
        self.assertIsNone(self.latest(self.row,'reading_certificate'))

    def test_r_audit_forbidden_to_auditor(self):
        self.app.add_url_rule('/audit',endpoint='sace_bp.audit_export',view_func=lambda:'')
        with self.app.test_request_context('/audit'):
            with self.assertRaises(HTTPException) as caught:self.r['protect_endorsement']()
            self.assertEqual(403,caught.exception.code)

    def test_real_blueprint_registration_and_template_endpoints(self):
        # Register actual decorators without importing the app factory or startup hooks.
        bp=flask.Blueprint('sace_bp',__name__)
        env=dict(self.r,sace_bp=bp,login_required=lambda f:f)
        for path in ('app/program_sace/routes.py','app/program_sace/endorsement_routes.py'):
            tree=ast.parse((ROOT/path).read_text(encoding='utf-8-sig'))
            nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef)]
            exec(compile(ast.Module(body=nodes,type_ignores=[]),path,'exec'),env)
        app=Flask('route_check')
        app.register_blueprint(bp)
        app.add_url_rule('/welcome',endpoint='public_bp.welcome',view_func=lambda:'')
        app.add_url_rule('/login',endpoint='auth_bp.login',view_func=lambda:'')
        expected={'sace_bp.reading_hub','sace_bp.demo_advance','sace_bp.step35','sace_bp.finish_evaluation','sace_bp.audit_export'}
        self.assertTrue(expected.issubset(app.view_functions))
        # Every literal SACE endpoint in the new templates must resolve.
        import re
        with app.test_request_context('/'):
            for path in (ROOT/'templates/program_sace').glob('*.html'):
                if not path.name.startswith(('endorsement_','evaluation_','step35')):continue
                for endpoint in re.findall(r"url_for\('([^']+)'",path.read_text(encoding='utf-8')):
                    if endpoint.endswith('.'):continue
                    flask.url_for(endpoint,slide=1,lesson_id=1,doc_type='p_guide',filename='test.png')

    def test_all_journey_templates_parse_and_render(self):
        from jinja2 import Environment
        env=Environment(loader=ChoiceLoader([DictLoader({'layout.html':'{% block content %}{% endblock %}'}),FileSystemLoader(str(ROOT/'templates'))]))
        env.globals.update(url_for=lambda name,**kw:'/'+name,csrf_token=lambda:'test',get_flashed_messages=lambda **kw:[],current_user=SimpleNamespace(email='auditor@example.test'))
        values=dict(ticks=set(),materials=self.r['MATERIALS'],missing=['step35_pass'],answers={},eligible=False,lessons=[],completed=set(),events=[],ready=False,content=None,result={},lesson={'id':1,'order':1,'title':'Test','caption':''},doc_title='Material',doc_url='/material',viewed_url='/viewed')
        for path in (ROOT/'templates/program_sace').glob('*.html'):
            if path.name.startswith(('endorsement_','evaluation_')) or path.name=='step35.html':
                for step in (0,1,31,32,33,34,35):
                    with self.subTest(template=path.name,step=step):env.get_template('program_sace/'+path.name).render(step=step,**values)

if __name__=='__main__':unittest.main()
