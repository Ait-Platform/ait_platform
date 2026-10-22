"""Isolated Lease Kit PDF/email route checks: no platform startup, DB or real email."""
import ast,sys,types,unittest
from pathlib import Path
from io import BytesIO
from functools import wraps
from unittest.mock import patch,Mock
from flask import Flask,Blueprint,request,abort
from flask_wtf.csrf import CSRFProtect,generate_csrf
from werkzeug.exceptions import HTTPException
from sqlalchemy.exc import SQLAlchemyError
ROOT=Path(__file__).resolve().parents[1]

class LeaseKitViewerTests(unittest.TestCase):
    def setUp(self):
        self.user=types.SimpleNamespace(id=7)
        self.user_patch=patch('flask_login.current_user',self.user);self.user_patch.start();self.addCleanup(self.user_patch.stop)
        self.items={'saved':'unchanged'}
        self.payload=b'%PDF-1.4\nexact review PDF bytes'
        self.version=3
        self.available=True
        self.components=('cover','certificate','core','special_conditions','signature','lease')
        self.sender=Mock(return_value=True)
        modules={name:types.ModuleType(name) for name in ('app','app.program_billing','app.utils','app.program_billing.lease_kit','app.program_billing.lease_kit_pdf','app.utils.mailer')}
        for name in ('app','app.program_billing','app.utils'):modules[name].__path__=[]
        modules['app.program_billing.lease_kit'].preview_context=self.context
        modules['app.program_billing.lease_kit_pdf'].build_pdf=lambda component,context:self.payload
        modules['app.utils.mailer'].send_standard_report_email=self.sender
        self.mods=patch.dict(sys.modules,modules);self.mods.start();self.addCleanup(self.mods.stop)
        self.app=Flask('lease-kit-routes');self.app.secret_key='synthetic-test-key'
        self.app.config['TESTING']=True
        CSRFProtect(self.app)
        self.bp=Blueprint('billing_bp',__name__)
        def require_user():
            if request.headers.get('X-Test-Owner')!='yes':abort(403)
        snapshot_tree=ast.parse((ROOT/'app/program_billing/lease_snapshot.py').read_text(encoding='utf-8-sig'))
        wrapper=next(n for n in snapshot_tree.body if isinstance(n,ast.FunctionDef) and n.name=='endpoint')
        ns={'wraps':wraps,'HTTPException':HTTPException,'SQLAlchemyError':SQLAlchemyError,'abort':abort,
            'require_pm_user':require_user,'db':types.SimpleNamespace(session=types.SimpleNamespace(rollback=lambda:None))}
        exec(compile(ast.Module(body=[wrapper],type_ignores=[]),'actual-snapshot-wrapper','exec'),ns)
        tree=ast.parse((ROOT/'app/program_billing/lease_preview.py').read_text(encoding='utf-8-sig'))
        names={'private_preview','kit_pdf','kit_email_signer','kit_email_selection','kit_email'}
        nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
        env={'__name__':'app.program_billing.lease_preview','__package__':'app.program_billing','billing_bp':self.bp,
            'snapshot':types.SimpleNamespace(endpoint=ns['endpoint']),'BytesIO':BytesIO,'abort':abort,'request':request}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-kit-routes','exec'),env)
        self.env=env;self.app.register_blueprint(self.bp)
        self.app.add_url_rule('/csrf',view_func=lambda:generate_csrf())
        self.client=self.app.test_client();self.headers={'X-Test-Owner':'yes'}
        self.csrf=self.client.get('/csrf').text

    def context(self,property_id):
        if property_id!=26 or request.args.get('draft_id')!='draft-one':abort(404)
        component=request.args.get('component','cover')
        if component not in self.components:abort(404)
        return {'property':{'id':26},'lease_draft':{'id':'draft-one','version':self.version,'items':self.items},
            'selected_component':component,'selected_label':component,'states':{component:{'available':self.available}}}

    def url(self,action='kit.pdf',component='core'):
        return '/billing/leases/property/26/'+action+'?draft_id=draft-one&component='+component

    def token(self,component='core'):
        response=self.client.get(self.url(component=component),headers=self.headers)
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.data,self.payload)
        self.assertEqual(response.headers['Cache-Control'],'private, no-store')
        return response.headers['X-Lease-PDF-Token']

    def send(self,token,component='core',**changes):
        data={'csrf_token':self.csrf,'document_token':token,'recipient':'owner@example.com','message':'Review only'}
        data.update(changes)
        return self.client.post(self.url('kit/email',component),headers=self.headers,data=data)

    def test_every_component_emails_exact_pdf_without_mutating_draft(self):
        for component in self.components:
            response=self.send(self.token(component),component)
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.headers['Cache-Control'],'private, no-store')
            values=self.sender.call_args.kwargs
            self.assertEqual(values['pdf_bytes'],self.payload)
            self.assertEqual(values['pdf_url'],'')
            self.assertTrue(values['filename'].endswith('-REVIEW.pdf'))
        self.assertEqual(self.items,{'saved':'unchanged'})

    def test_authentication_scope_and_methods(self):
        self.assertEqual(self.client.get(self.url()).status_code,403)
        self.assertEqual(self.client.get(self.url().replace('/26/','/27/'),headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.url().replace('draft-one','wrong'),headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.url(component='invalid'),headers=self.headers).status_code,404)
        self.assertEqual(self.client.get(self.url('kit/email'),headers=self.headers).status_code,405)
        token=self.token()
        self.assertEqual(self.client.post(self.url('kit/email'),data={'csrf_token':self.csrf,'document_token':token}).status_code,403)
        self.sender.assert_not_called()

    def test_csrf_and_explicit_email_validation(self):
        token=self.token()
        self.assertEqual(self.send(token,csrf_token='').status_code,400)
        self.assertEqual(self.send(token,recipient='bad-address').status_code,400)
        self.assertEqual(self.send(token,message='x'*5001).status_code,400)
        self.sender.assert_not_called()

    def test_tampered_cross_component_user_and_stale_document_rejected(self):
        token=self.token()
        self.assertEqual(self.send(token+'tampered').status_code,400)
        self.assertEqual(self.send(token,'lease').status_code,409)
        self.user.id=8
        self.assertEqual(self.send(token).status_code,409)
        self.user.id=7;self.version+=1
        self.assertEqual(self.send(token).status_code,409)
        self.version-=1;self.payload+=b'changed'
        self.assertEqual(self.send(token).status_code,409)
        self.sender.assert_not_called()

    def test_expired_token_unavailable_document_and_transport_failure(self):
        token=self.token()
        from itsdangerous import TimestampSigner
        with patch.object(TimestampSigner,'get_timestamp',return_value=0):old=self.token()
        self.assertEqual(self.send(old).status_code,400)
        self.available=False
        self.assertEqual(self.send(token).status_code,409)
        self.available=True;self.sender.side_effect=RuntimeError('sensitive SMTP detail')
        response=self.send(token)
        self.assertEqual(response.status_code,503)
        self.assertNotIn(b'sensitive SMTP detail',response.data)

    def test_review_metadata_is_stable_without_changing_pdf_page(self):
        from pypdf import PdfReader,PdfWriter
        tree=ast.parse((ROOT/'app/program_billing/lease_kit_pdf.py').read_text(encoding='utf-8-sig'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='stable_review_pdf')
        env={'BytesIO':BytesIO}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'review-metadata-normalizer','exec'),env)
        results=[]
        for timestamp in ('D:20261022000000','D:20261022000002'):
            writer=PdfWriter();writer.add_blank_page(width=595,height=842)
            writer.add_metadata({'/Title':'Review only','/CreationDate':timestamp,'/ModDate':timestamp})
            output=BytesIO();writer.write(output)
            results.append(env['stable_review_pdf'](output.getvalue()))
        self.assertEqual(results[0],results[1])
        document=PdfReader(BytesIO(results[0]))
        self.assertEqual(len(document.pages),1)
        self.assertEqual(document.metadata.title,'Review only')
        self.assertNotIn('/CreationDate',document.metadata)

if __name__=='__main__':unittest.main()
