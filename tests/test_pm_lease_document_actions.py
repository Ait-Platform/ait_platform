"""Read-only history/action rendering checks using synthetic records and no DB startup."""
import ast,copy,types,sys,unittest,uuid,io,importlib.util
from pathlib import Path
from functools import wraps
from datetime import datetime,timezone
from unittest.mock import Mock,patch
from flask import Flask,Blueprint,abort,request,current_app,render_template,url_for,redirect,flash
from itsdangerous import URLSafeTimedSerializer
from jinja2 import ChoiceLoader,DictLoader,FileSystemLoader
from werkzeug.exceptions import HTTPException
from sqlalchemy.exc import SQLAlchemyError,IntegrityError
from flask_wtf.csrf import CSRFProtect,generate_csrf
from werkzeug.exceptions import NotFound
ROOT=Path(__file__).resolve().parents[1]

class LeaseDocumentsActionsTests(unittest.TestCase):
    def setUp(self):
        self.app=Flask('lease-actions-history');self.app.secret_key='synthetic-key'
        self.app.jinja_loader=ChoiceLoader([DictLoader({'program_billing/lease_workflow_base.html':'<h1>{{ title }}</h1>{% block workspace %}{% endblock %}'}),FileSystemLoader(ROOT/'templates')])
        self.app.jinja_env.globals['csrf_token']=generate_csrf
        CSRFProtect(self.app)
        kit_tree=ast.parse((ROOT/'app/program_billing/lease_kit.py').read_text(encoding='utf-8-sig'))
        constants=next(n for n in kit_tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='COMPONENTS' for t in n.targets))
        action=next(n for n in kit_tree.body if isinstance(n,ast.FunctionDef) and n.name=='action_documents')
        kit=types.ModuleType('app.program_billing.lease_kit')
        kit.url_for=url_for
        exec(compile(ast.Module(body=[constants,next(n for n in kit_tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ORDER' for t in n.targets)),action],type_ignores=[]),'actual-actions','exec'),kit.__dict__)
        self.kit=kit
        mods={name:types.ModuleType(name) for name in ('app','app.program_billing')}
        for m in mods.values():m.__path__=[]
        mods[kit.__name__]=kit
        r2=types.ModuleType('app.program_billing.lease_r2');r2.settings=Mock(return_value={'bucket':'ait-lease-controlled-private'})
        mods[r2.__name__]=r2;self.r2=r2
        self.mods=patch.dict(sys.modules,mods);self.mods.start();self.addCleanup(self.mods.stop)
        spec=importlib.util.spec_from_file_location('app.program_billing.lease_storage',ROOT/'app/program_billing/lease_storage.py')
        self.storage=importlib.util.module_from_spec(spec);sys.modules[spec.name]=self.storage;spec.loader.exec_module(self.storage)
        self.addCleanup(sys.modules.pop,spec.name,None)
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
        for endpoint,path in [('lease_sc_word','special-conditions.docx'),('lease_certificate_edit','certificate'),('lease_live_view','view'),('pm_lease_management','manage'),('lease_addendum','addendum'),('lease_kit_pdf','kit.pdf'),('lease_kit_email','kit/email'),('lease_history_document','history/<document_id>'),('lease_history_download','history/<document_id>/download'),('lease_document_email','email')]:
            bp.add_url_rule('/billing/leases/property/<int:property_id>/'+path,endpoint,lambda **kw:'synthetic endpoint')
        def selected(property_id,lock=False):
            if property_id!=26 or request.args.get('draft_id')!='draft-one':abort(404)
            return {'id':26},None,self.draft
        history_tree=ast.parse((ROOT/'app/program_billing/lease_history.py').read_text(encoding='utf-8-sig'))
        names={'context','ledger','originals','signer','history','signed_copy','private_history'}
        nodes=[n for n in history_tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
        env.update({'__name__':'app.program_billing.lease_history','__package__':'app.program_billing',
            'billing_bp':bp,'snapshot':types.SimpleNamespace(endpoint=env['endpoint']), 'workflow':types.SimpleNamespace(selected_context=selected),
            'current_user':types.SimpleNamespace(id=7),'current_app':current_app,'request':request,'url_for':url_for,
            'redirect':redirect,'flash':flash,'deepcopy':copy.deepcopy,'uuid':uuid,'URLSafeTimedSerializer':URLSafeTimedSerializer,'text':lambda s:s,
            'KINDS':{'signed_original':'Original signed copy','signed_addendum':'Signed addendum','signed_termination':'Signed termination'},'render_pm_template':render_template})
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-history-view','exec'),env)
        self.store=Mock();self.document=Mock(return_value=({},self.draft['items']['_lease_history']['documents'][0],b'synthetic'))
        env.update(store=self.store,document=self.document)
        bank_tree=ast.parse((ROOT/'app/program_billing/lease_property_payments.py').read_text(encoding='utf-8-sig'))
        bank=next(n for n in bank_tree.body if isinstance(n,ast.FunctionDef) and n.name=='workspace')
        self.bank_render=Mock(return_value='Existing banking page')
        self.bank_add=Mock()
        bank_env=dict(env,readiness=lambda:None,association_readiness=lambda:None,register_accounts=lambda:[],accounts=lambda p:[],arrangements=lambda p:[],signed=lambda d:False,render_pm_template=self.bank_render,add_account=self.bank_add,IntegrityError=IntegrityError,NotFound=NotFound)
        exec(compile(ast.Module(body=[bank],type_ignores=[]),'unchanged-bank-route','exec'),bank_env)
        self.app.register_blueprint(bp);self.app.add_url_rule('/csrf',view_func=generate_csrf)
        self.client=self.app.test_client();self.csrf=self.client.get('/csrf').text

    def test_six_ordered_rows_without_history_or_upload_display(self):
        response=self.client.get('/billing/leases/property/26/history?draft_id=draft-one',headers={'X-Test-Owner':'yes'})
        self.assertEqual(response.status_code,200);html=response.text
        labels=['Cover','Certificate','Core Lease','Special Conditions','Signatures','Full Lease Agreement']
        positions=[html.index('<th scope="row">'+label+'</th>') for label in labels]
        self.assertEqual(positions,sorted(positions))
        self.assertEqual(html.count('data-action="email"'),6)
        from lxml import html as parser
        rows=parser.fromstring(html).xpath('//tr[@data-component]')
        for row in rows:
            pdf=row.get('data-pdf-url');links=row.xpath('./td/a')
            self.assertEqual(links[0].get('href'),pdf);self.assertEqual(links[0].get('target'),'_blank')
            self.assertEqual(links[1].get('href'),pdf+'&download=1')
        self.assertEqual(html.count('>Export Word</a>'),1);self.assertEqual(html.count('id="sc-import-open"'),1)
        self.assertIn('Email / Print',html)
        for key,_ in self.kit.COMPONENTS:self.assertIn('component='+key,html)
        for text in ('Chronological document history','Email submission history','signed-original.pdf','legacy-upload.pdf','owner@example.com','Upload Signed Copy','No documents have been recorded','No email attempts recorded'):
            self.assertNotIn(text,html)
        self.assertEqual(self.draft,self.original)
        self.db.session.execute.assert_not_called();self.db.session.commit.assert_not_called()

    def test_signed_copy_retains_original_upload_form_and_existing_documents(self):
        response=self.client.get('/billing/leases/property/26/signed-copy?draft_id=draft-one',headers={'X-Test-Owner':'yes'})
        self.assertEqual(response.status_code,200);html=response.text
        for text in ('Signed Copy','Upload Signed Copy','signed-original.pdf','legacy-upload.pdf','View / Open','Download exact copy','Email exact copy'):
            self.assertIn(text,html)
        self.assertNotIn('Email submission history',html)
        self.assertEqual(response.headers['Cache-Control'],'private, no-store')
        self.assertEqual(self.draft,self.original);self.db.session.commit.assert_not_called()

    def test_history_authentication_and_property_draft_scope(self):
        for path in ('history','signed-copy','banking'):
            url='/billing/leases/property/26/'+path+'?draft_id=draft-one'
            self.assertEqual(self.client.get(url).status_code,403)
            self.assertEqual(self.client.get(url.replace('/26/','/27/'),headers={'X-Test-Owner':'yes'}).status_code,404)
            self.assertEqual(self.client.get(url.replace('draft-one','wrong'),headers={'X-Test-Owner':'yes'}).status_code,404)
        self.assertEqual(self.draft,self.original);self.db.session.commit.assert_not_called()

    def test_view_lease_has_only_inspection_controls_for_all_six(self):
        for key,label in self.kit.COMPONENTS:
            with self.app.test_request_context('/'):
                html=render_template('program_billing/lease_kit_view.html',selected_component=key,selected_label=label,
                    components=[{'key':k,'label':v,'url':'/inspect?component='+k} for k,v in self.kit.COMPONENTS],
                    states={k:{'available':True,'omitted':True} for k,_ in self.kit.COMPONENTS},pdf_url='/kit.pdf?component=core',selected_documents=self.kit.ORDER if key=='lease' else [key])
            if key in ('core','lease'):
                self.assertIn('lease-kit-canvas',html);self.assertIn('lease-kit-page-count',html)
                for action in ('zoom-in','zoom-out','previous','next'):self.assertIn('id="lease-kit-'+action+'"',html)
            else:self.assertNotIn('lease-kit-canvas',html)
            from lxml import html as parser
            keys=parser.fromstring(html).xpath('//*[@data-lease-component]/@data-lease-component')
            self.assertEqual(keys,list(self.kit.ORDER) if key=='lease' else [key])
            for action in ('play','pause','print','download','email'):self.assertNotIn('id="lease-kit-'+action+'"',html)
            self.assertNotIn('<iframe',html);self.assertNotIn('target="_blank"',html)

    def upload(self,kind='signed_original',payload=None,**changes):
        from pypdf import PdfWriter
        if payload is None:
            output=io.BytesIO();writer=PdfWriter();writer.add_blank_page(width=595,height=842);writer.write(output);payload=output.getvalue()
        data={'csrf_token':self.csrf,'kind':kind,'lease_reference':'Synthetic existing agreement','version':'4',
              'signed_document':(io.BytesIO(payload),'signed.pdf')}
        data.update(changes)
        return self.client.post('/billing/leases/property/26/signed-copy?draft_id=draft-one',headers={'X-Test-Owner':'yes'},data=data)

    def test_signed_upload_uses_existing_validator_storage_and_original_snapshot(self):
        for kind in ('signed_original','signed_addendum','signed_termination'):
            response=self.upload(kind,original_id='11111111-1111-1111-1111-111111111111')
            self.assertEqual(response.status_code,302);self.assertIn('/signed-copy?',response.location)
            args=self.store.call_args.args
            self.assertEqual(args[1],kind);self.assertEqual(args[3],'application/pdf')
            self.assertTrue(args[4].startswith(b'%PDF-'))
            if kind=='signed_original':self.assertEqual(args[5]['particulars'],{})
            else:self.assertEqual(args[5]['original_id'],'11111111-1111-1111-1111-111111111111')
        self.assertEqual(self.draft,self.original)

    def test_upload_rejects_invalid_file_reference_type_and_csrf(self):
        for values in ({'payload':b'not PDF'},{'lease_reference':''},{'kind':'unsupported'},{'csrf_token':''}):
            self.assertEqual(self.upload(**values).status_code,400)
        self.store.assert_not_called()
        self.r2.settings.side_effect=ValueError('Private storage unavailable')
        self.assertEqual(self.upload().status_code,400);self.store.assert_not_called()
        self.assertEqual(self.draft,self.original)

    def test_signed_upload_requires_owner_and_property_scope(self):
        url='/billing/leases/property/26/signed-copy?draft_id=draft-one'
        self.assertEqual(self.client.post(url,data={'csrf_token':self.csrf}).status_code,403)
        self.assertEqual(self.client.post(url.replace('/26/','/27/'),headers={'X-Test-Owner':'yes'},data={'csrf_token':self.csrf}).status_code,404)
        self.store.assert_not_called();self.assertEqual(self.draft,self.original)

    def test_old_upload_submission_redirects_to_single_signed_copy_handler(self):
        response=self.client.post('/billing/leases/property/26/history?draft_id=draft-one',headers={'X-Test-Owner':'yes'},data={'csrf_token':self.csrf})
        self.assertEqual(response.status_code,307);self.assertIn('/signed-copy?',response.location)
        self.store.assert_not_called()

    def test_compact_menu_keeps_bank_route_and_all_six_items(self):
        from lxml import html
        with self.app.test_request_context('/'):
            page=render_template('program_billing/lease_submenu.html',property={'id':26},lease_draft=self.draft)
        links=html.fromstring(page).xpath('//a')
        self.assertEqual([link.text_content() for link in links],['Data Capture','View Lease','Bank','Addendum','Email / Print','Signed Copy'])
        self.assertIn('/banking?',links[2].get('href'));self.assertIn('/signed-copy?',links[-1].get('href'))
        css=(ROOT/'app/static/css/lease_capture.css').read_text(encoding='utf-8-sig')
        self.assertIn('display:flex; flex-wrap:nowrap;',css);self.assertIn('@media (max-width:640px)',css)
        response=self.client.get(links[2].get('href'),headers={'X-Test-Owner':'yes'})
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.bank_render.call_args.kwargs['title'],'Banking Details')
        response=self.client.post(links[2].get('href'),headers={'X-Test-Owner':'yes'},data={'action':'add_account','csrf_token':self.csrf})
        self.assertEqual(response.status_code,302);self.bank_add.assert_called_once()
        self.assertEqual(self.draft,self.original)

if __name__=='__main__':unittest.main()
