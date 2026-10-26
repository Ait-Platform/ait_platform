"""Focused PDF addendum continuation of existing signed leases, isolated PostgreSQL/R2."""
import copy,io,json,sys,types,uuid,unittest
from datetime import datetime,timezone
from hashlib import sha256
from sqlalchemy import text
from flask import request,abort,url_for,redirect,flash,send_file,current_app,render_template
from itsdangerous import URLSafeTimedSerializer,BadSignature,SignatureExpired
from unittest.mock import Mock,patch
import test_pm_lease_sc_import as current_fixture
from test_pm_lease_lessee_persistence import functions
from lxml import html
from pypdf import PdfReader
import test_pm_lease_history as fixture

class AddendumPDFTests(unittest.TestCase):
    stored=current_fixture.SpecialConditionsImportTests.stored
    payload=current_fixture.SpecialConditionsImportTests.payload
    def pdf(self):
        from reportlab.pdfgen import canvas
        output=io.BytesIO();page=canvas.Canvas(output,invariant=1);page.drawString(40,700,'Synthetic Addendum PDF');page.save();return output.getvalue()
    sender=fixture.LeaseHistoryTests.sender
    history_doc_url=fixture.LeaseHistoryTests.history_doc_url
    def sql(self,statement):return self.session.execute(text(statement))
    def ledger(self):return self.stored()['items'].get('_lease_history',{'documents':[],'emails':[]})
    def setUp(self):
        current_fixture.SpecialConditionsImportTests.setUp(self);self.draft=self.identifier
        self.client.environ_base['HTTP_X_TEST_OWNER']='yes'
        items=copy.deepcopy(self.stored()['items'])
        for email in items.get('_lease_history',{}).get('emails',[]):email.setdefault('id',str(uuid.uuid4()))
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'items':json.dumps(items),'id':self.identifier});self.session.commit()
        self.session.execute(text("ALTER TABLE bil_lease_document ALTER COLUMN state SET DEFAULT 'pending'"));self.session.commit()
        snap=sys.modules['app.program_billing.lease_snapshot']
        def context(property_id,lock=False):return snap.authorised_property(property_id,lock),snap.draft_row(property_id,request.args.get('draft_id'),lock)
        env={'__name__':'app.program_billing.lease_history','__package__':'app.program_billing','json':json,'db':self.db,'text':text,'deepcopy':copy.deepcopy,'datetime':datetime,'timezone':timezone,'sha256':sha256,'uuid':uuid,'request':request,'abort':abort,'url_for':url_for,'redirect':redirect,'flash':flash,'send_file':send_file,'current_app':current_app,'current_user':self.user,'BytesIO':io.BytesIO,'URLSafeTimedSerializer':URLSafeTimedSerializer,'BadSignature':BadSignature,'SignatureExpired':SignatureExpired,'snapshot':snap,'context':context,'re':__import__('re'),'CLAUSE_SOURCES':{'certificate':'Certificate','core':'Core Lease','special_conditions':'Special Conditions','signature':'Signatures'},'MIME':'application/vnd.openxmlformats-officedocument.wordprocessingml.document','billing_bp':types.SimpleNamespace(route=lambda *a,**kw:lambda f:f,get=lambda *a,**kw:lambda f:f,after_request=lambda f:f),'render_pm_template':render_template}
        functions('app/program_billing/lease_history.py',{'clause_lease','clause_catalog','clause_relationships','clause_register_state','clause_register_save','now','ledger','update_locked','locked','document','originals','store','addendum_pdf','addendum_history','addendum','signer','view_token','view_document','download','resolve_token','email_document','private_history'},env)
        sys.modules['app.program_billing.lease_property_payments'].readiness=lambda:'Bank register unavailable in this isolated fixture'
        self.history_env=env;self.app._got_first_request=False
        for path,endpoint,function,methods in [('/billing/leases/property/<int:property_id>/addendum','lease_addendum','addendum',['GET','POST']),('/billing/leases/property/<int:property_id>/history/<uuid:document_id>','lease_history_document','view_document',['GET']),('/billing/leases/property/<int:property_id>/history/<uuid:document_id>/download','lease_history_download','download',['GET']),('/billing/leases/property/<int:property_id>/email','lease_document_email','email_document',['GET','POST'])]:
            wrapped=env[function];self.app.view_functions['billing_bp.'+endpoint]=wrapped
            self.app.add_url_rule(path,endpoint='billing_bp.'+endpoint,view_func=wrapped,methods=methods)
        self.app.after_request(env['private_history'])
    def signed(self):
        with self.app.test_request_context('/?draft_id='+str(self.draft),headers=self.headers):
            self.history_env['store'](self.history_env['locked'](26,self.draft),'signed_original','Original.pdf','application/pdf',self.pdf(),{'lease_reference':'Almond lease 2026-01','particulars':copy.deepcopy(self.stored()['items'])},continuation=True)
        return types.SimpleNamespace(status_code=302)

    def post_addendum_pdf(self,payload=None,**changes):
        original=self.ledger()['documents'][0]
        data={'csrf_token':self.csrf,'action':'upload_pdf','original_id':original['id'],'addendum_pdf':(io.BytesIO(payload or self.pdf()),'Amendment.pdf')};data.update(changes)
        return self.client.post('/billing/leases/property/26/addendum?draft_id='+str(self.draft),data=data,content_type='multipart/form-data')
    def test_addendum_pdf_upload_multiple_history_original_unchanged(self):
        self.assertEqual(self.signed().status_code,302);original=copy.deepcopy(self.ledger()['documents'][0])
        before=copy.deepcopy(self.sql('SELECT items FROM bil_lease_draft').scalar());original_bytes=self.client.get(self.history_doc_url(original['id'],'/download')).data
        for _ in range(2):self.assertEqual(self.post_addendum_pdf().status_code,302)
        documents=self.ledger()['documents'];self.assertEqual(len(documents),3)
        self.assertEqual([entry['addendum_number'] for entry in documents[1:]],[1,2])
        for entry in documents[1:]:
            self.assertEqual(entry['original_id'],original['id']);self.assertEqual(entry['original_sha256'],original['sha256'])
            response=self.client.get(self.history_doc_url(entry['id']));self.assertEqual(response.status_code,200);self.assertEqual(response.data,self.pdf());self.assertIn('inline',response.headers['Content-Disposition']);self.assertIn('no-store',response.headers['Cache-Control'])
        self.assertEqual(documents[0],original);self.assertEqual(self.client.get(self.history_doc_url(original['id'],'/download')).data,original_bytes)
        after=self.sql('SELECT items FROM bil_lease_draft').scalar();self.assertEqual({k:v for k,v in after.items() if k!='_lease_history'},{k:v for k,v in before.items() if k!='_lease_history'})
        page=self.client.get('/billing/leases/property/26/addendum?draft_id='+str(self.draft));self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);self.assertEqual(len(tree.xpath('//a[text()="Open PDF"]')),2);self.assertEqual(len(tree.xpath('//a[text()="Email"]')),2)
        self.assertIn('Addendum History',page.text);self.assertNotIn('r2.dev',page.text)
    def test_addendum_pdf_created_without_master_and_exact_pdf_email(self):
        self.assertEqual(self.signed().status_code,302);original=self.ledger()['documents'][0]
        response=self.client.post('/billing/leases/property/26/addendum?draft_id='+str(self.draft),data={'csrf_token':self.csrf,'action':'create_pdf','original_id':original['id'],'wording':'Agreed amendment: rent changes to ZAR 1000.\nAll other terms remain unchanged.'})
        self.assertEqual(response.status_code,302,response.text);entry=self.ledger()['documents'][-1]
        payload=self.client.get(self.history_doc_url(entry['id'])).data;body=''.join(p.extract_text() for p in PdfReader(io.BytesIO(payload)).pages)
        self.assertIn(original['lease_reference'],body);self.assertIn('rent changes to ZAR 1000',body)
        page=self.client.get(response.location);link=html.fromstring(page.data).xpath('//a[text()="Email"]/@href')[0]
        module,guard=self.sender()
        with guard:
            self.assertEqual(self.client.get(link).status_code,200);module.send_standard_report_email.assert_not_called()
            self.assertEqual(self.client.post(link,data={'csrf_token':self.csrf,'recipient':'attorney@example.com'}).status_code,302)
        self.assertEqual(module.send_standard_report_email.call_args.kwargs['pdf_bytes'],payload);self.assertEqual(module.send_standard_report_email.call_args.kwargs['attachment_mime'],'application/pdf')
    def test_addendum_pdf_draft_link_and_foreign_draft_original_rejected(self):
        response=self.client.post('/billing/leases/property/26/addendum?draft_id='+str(self.draft),data={'csrf_token':self.csrf,'action':'upload_pdf','addendum_pdf':(io.BytesIO(self.pdf()),'Draft-amendment.pdf')},content_type='multipart/form-data')
        self.assertEqual(response.status_code,302);entry=self.ledger()['documents'][-1]
        self.assertIsNone(entry['original_id']);self.assertEqual(entry['lease_reference'],'Lease draft '+str(self.draft))
        self.signed();original=self.ledger()['documents'][-1];other=uuid.uuid4()
        self.session.execute(text('INSERT INTO bil_lease_draft(id,property_id,manager_id,source_version,items,version,status,registration_token) SELECT :other,property_id,manager_id,source_version,items,version,status,:token FROM bil_lease_draft WHERE id=:id'),{'other':other,'token':uuid.uuid4(),'id':self.draft})
        self.session.commit()
        response=self.client.post('/billing/leases/property/26/addendum?draft_id='+str(other),data={'csrf_token':self.csrf,'action':'upload_pdf','original_id':original['id'],'addendum_pdf':(io.BytesIO(self.pdf()),'Foreign.pdf')},content_type='multipart/form-data')
        self.assertEqual(response.status_code,404)

    def test_capture_navigation_order_current_section_and_saved_transition(self):
        keys=['parties','premises','period','financial','banking','additional','signing']
        for key in keys:
            response=self.client.get('/capture/26/'+key+'?draft_id='+str(self.draft));self.assertEqual(response.status_code,200,response.text)
            tree=html.fromstring(response.data);links=tree.xpath('//nav[@aria-label="Data Capture sections"]/a')
            self.assertEqual([link.get('data-capture-section') for link in links],keys)
            self.assertEqual([link.text_content() for link in links],['Parties','Premises','Lease Period','Financial','Banking','Special Conditions','Signing'])
            self.assertEqual([link.get('data-capture-section') for link in links if link.get('aria-current')=='page'],[key])
            for link in links:self.assertIn(str(self.draft),link.get('href'))
        response=self.client.get('/capture/26/premises?draft_id='+str(self.draft));tree=html.fromstring(response.data)
        data={field.get('name'):field.get('value','') for field in tree.xpath('//form[@id="certificate-form"]//input[@name]')}
        for field in tree.xpath('//form[@id="certificate-form"]//textarea[@name]'):data[field.get('name')]=field.text or ''
        data.update(PROPERTY_ADDRESS='Saved through section navigation',capture_return='section',capture_return_section='signing')
        saved=self.client.post('/capture/26/premises?draft_id='+str(self.draft),data=data);self.assertEqual(saved.status_code,302,saved.text)
        self.assertIn('/signing',saved.location)
        reopened=self.client.get('/capture/26/premises?draft_id='+str(self.draft));self.assertIn('Saved through section navigation',reopened.text)

    def test_addendum_pdf_invalid_foreign_scope_and_failed_storage(self):
        self.assertEqual(self.signed().status_code,302);original=copy.deepcopy(self.ledger()['documents'][0])
        self.assertEqual(self.post_addendum_pdf(b'not a pdf').status_code,400)
        self.assertEqual(self.post_addendum_pdf(original_id='invalid').status_code,404)
        self.r2.upload_bytes_to_r2.side_effect=RuntimeError('synthetic storage failure');self.assertEqual(self.post_addendum_pdf().status_code,503);self.r2.upload_bytes_to_r2.side_effect=None
        pending=self.ledger()['documents'][-1];self.assertEqual(self.client.get(self.history_doc_url(pending['id'])).status_code,404)
        self.assertEqual(self.ledger()['documents'][0],original)
        self.assertEqual(self.client.get(self.history_doc_url(original['id']).replace('/26/','/27/')).status_code,404)
        self.client.environ_base.pop('HTTP_X_TEST_OWNER');self.assertEqual(self.post_addendum_pdf().status_code,403)
