"""Read-only history/action rendering checks using synthetic records and no DB startup."""
import ast,copy,types,sys,unittest,uuid
from pathlib import Path
from functools import wraps
from datetime import datetime,timezone
from unittest.mock import Mock,patch
from flask import Flask,Blueprint,abort,request,current_app,render_template,url_for
from itsdangerous import URLSafeTimedSerializer
from jinja2 import ChoiceLoader,DictLoader,FileSystemLoader
from werkzeug.exceptions import HTTPException
from sqlalchemy.exc import SQLAlchemyError
ROOT=Path(__file__).resolve().parents[1]

class LeaseDocumentsActionsTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask('lease-actions-history');self.app.secret_key='synthetic-key'
        self.app.jinja_loader=ChoiceLoader([DictLoader({'program_billing/lease_workflow_base.html':'<h1>{{ title }}</h1>{% block workspace %}{% endblock %}'}),FileSystemLoader(ROOT/'templates')])
        self.app.jinja_env.globals['csrf_token']=lambda:'synthetic-csrf'
        kit_tree=ast.parse((ROOT/'app/program_billing/lease_kit.py').read_text(encoding='utf-8-sig'))
        constants=next(n for n in kit_tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='COMPONENTS' for t in n.targets))
        action=next(n for n in kit_tree.body if isinstance(n,ast.FunctionDef) and n.name=='action_documents')
        kit=types.ModuleType('app.program_billing.lease_kit')
        kit.url_for=url_for
        exec(compile(ast.Module(body=[constants,action],type_ignores=[]),'actual-actions','exec'),kit.__dict__)
        self.kit=kit
        mods={name:types.ModuleType(name) for name in ('app','app.program_billing')}
        for m in mods.values():m.__path__=[]
        mods[kit.__name__]=kit
        self.mods=patch.dict(sys.modules,mods);self.mods.start();self.addCleanup(self.mods.stop)
        self.draft={'id':'draft-one','version':4,'items':{'_lease_history':{'documents':[{
            'id':'11111111-1111-1111-1111-111111111111','filename':'signed-original.pdf','kind':'signed_original',
            'version':1,'uploaded_at':'2026-10-01','uploaded_by':7,'state':'available','sha256':'synthetic-hash','lease_reference':'ORIGINAL'}],
            'emails':[{'at':'2026-10-02','recipient':'owner@example.com','filename':'signed-original.pdf','status':'submitted'}]}}}
        self.original=copy.deepcopy(self.draft)
        legacy={'id':'22222222-2222-2222-2222-222222222222','filename':'legacy-upload.pdf','state':'available','created_at':datetime(2026,10,3,tzinfo=timezone.utc),'sha256':'legacy-synthetic-hash'}
        self.db=types.SimpleNamespace(session=types.SimpleNamespace(execute=Mock(return_value=types.SimpleNamespace(mappings=lambda:[legacy])),commit=Mock(),rollback=Mock()))
        def require_user():
            if request.headers.get('X-Test-Owner')!='yes':abort(403)
        snap=ast.parse((ROOT/'app/program_billing/lease_snapshot.py').read_text(encoding='utf-8-sig'))
        wrapper=next(n for n in snap.body if isinstance(n,ast.FunctionDef) and n.name=='endpoint')
        env={'wraps':wraps,'HTTPException':HTTPException,'SQLAlchemyError':SQLAlchemyError,'abort':abort,'require_pm_user':require_user,'db':self.db}
        exec(compile(ast.Module(body=[wrapper],type_ignores=[]),'actual-wrapper','exec'),env)
        bp=Blueprint('billing_bp',__name__)
        for endpoint,path in [('lease_kit_pdf','kit.pdf'),('lease_kit_email','kit/email'),('lease_history_document','history/<document_id>'),('lease_history_download','history/<document_id>/download'),('lease_document_email','email')]:
            bp.add_url_rule('/billing/leases/property/<int:property_id>/'+path,endpoint,lambda **kw:'synthetic endpoint')
        def selected(property_id,lock=False):
            if property_id!=26 or request.args.get('draft_id')!='draft-one':abort(404)
            return {'id':26},None,self.draft
        history_tree=ast.parse((ROOT/'app/program_billing/lease_history.py').read_text(encoding='utf-8-sig'))
        names={'context','ledger','originals','signer','history'}
        nodes=[n for n in history_tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
        env.update({'__name__':'app.program_billing.lease_history','__package__':'app.program_billing',
            'billing_bp':bp,'snapshot':types.SimpleNamespace(endpoint=env['endpoint']), 'workflow':types.SimpleNamespace(selected_context=selected),
            'current_user':types.SimpleNamespace(id=7),'current_app':current_app,'request':request,'url_for':url_for,
            'deepcopy':copy.deepcopy,'uuid':uuid,'URLSafeTimedSerializer':URLSafeTimedSerializer,'text':lambda s:s,
            'KINDS':{'signed_original':'Original signed copy'},'render_pm_template':render_template})
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-history-view','exec'),env)
        self.app.register_blueprint(bp);self.client=self.app.test_client()

    def test_six_ordered_rows_and_preserved_history_links_and_audit(self):
        response=self.client.get('/billing/leases/property/26/history?draft_id=draft-one',headers={'X-Test-Owner':'yes'})
        self.assertEqual(response.status_code,200);html=response.text
        labels=['Cover','Certificate','Residential Core','Supplementary Conditions','Signature Page','Full Lease Agreement']
        positions=[html.index('<th scope="row">'+label+'</th>') for label in labels]
        self.assertEqual(positions,sorted(positions))
        self.assertEqual(html.count('data-action="email"'),6);self.assertEqual(html.count('data-action="print"'),6)
        self.assertIn('Lease Documents &amp; Actions',html)
        for key,_ in self.kit.COMPONENTS:self.assertIn('component='+key,html)
        self.assertIn('id="lease-document-history"',html)
        for text in ('signed-original.pdf','legacy-upload.pdf','View / Open','Download exact copy','Email exact copy','owner@example.com','submitted','Upload Signed Copy'):
            self.assertIn(text,html)
        self.assertEqual(self.draft,self.original);self.db.session.commit.assert_not_called()
        self.assertTrue(str(self.db.session.execute.call_args.args[0]).startswith('SELECT'))

    def test_history_authentication_and_property_draft_scope(self):
        url='/billing/leases/property/26/history?draft_id=draft-one'
        self.assertEqual(self.client.get(url).status_code,403)
        self.assertEqual(self.client.get(url.replace('/26/','/27/'),headers={'X-Test-Owner':'yes'}).status_code,404)
        self.assertEqual(self.client.get(url.replace('draft-one','wrong'),headers={'X-Test-Owner':'yes'}).status_code,404)
        self.assertEqual(self.draft,self.original);self.db.session.commit.assert_not_called()

    def test_view_lease_has_only_inspection_controls_for_all_six(self):
        for key,label in self.kit.COMPONENTS:
            with self.app.test_request_context('/'):
                html=render_template('program_billing/lease_kit_view.html',selected_component=key,selected_label=label,
                    components=[{'key':k,'label':v,'url':'/inspect?component='+k} for k,v in self.kit.COMPONENTS],
                    states={k:{'available':True} for k,_ in self.kit.COMPONENTS},pdf_url='/kit.pdf?component='+key)
            self.assertIn('lease-kit-canvas',html);self.assertIn('lease-kit-page-count',html)
            for action in ('zoom-in','zoom-out','previous','next'):self.assertIn('id="lease-kit-'+action+'"',html)
            for action in ('play','pause','print','download','email'):self.assertNotIn('id="lease-kit-'+action+'"',html)
            self.assertNotIn('<iframe',html);self.assertNotIn('target="_blank"',html)

if __name__=='__main__':unittest.main()
