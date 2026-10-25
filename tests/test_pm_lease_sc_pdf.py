"""Current PDF filing/preview/history/readiness on isolated temporary PostgreSQL + private R2 double."""
import copy,io,json,unittest,uuid
from unittest.mock import patch
from zipfile import ZipFile
from lxml import etree as ET
from sqlalchemy import text
import test_pm_lease_sc_import as fixture

class FiledSpecialConditionsTests(unittest.TestCase):
    setUp=fixture.SpecialConditionsImportTests.setUp
    stored=fixture.SpecialConditionsImportTests.stored
    payload=fixture.SpecialConditionsImportTests.payload
    def pdf(self,label='CURRENT OFFICIAL SC WITH MAP'):
        from reportlab.pdfgen import canvas
        out=io.BytesIO();page=canvas.Canvas(out,invariant=1);page.drawString(40,760,label);page.rect(40,100,450,400);page.showPage();page.save();return out.getvalue()
    def post_pdf(self,action,payload=None,**changes):
        data={'csrf_token':self.csrf,'action':action,'source':(io.BytesIO(payload or self.pdf()),'Special-Conditions.pdf')};data.update(changes)
        return self.client.post(self.endpoint,headers=self.headers,data=data,content_type='multipart/form-data')
    def file(self,payload=None):
        source=payload or self.pdf();preview=self.post_pdf('sc_pdf_preview',source);self.assertEqual(preview.status_code,200,preview.text)
        result=self.post_pdf('sc_pdf_confirm',source,token=preview.get_json()['token'],confirmed='yes');self.assertEqual(result.status_code,200,result.text)
        return result
    def test_preview_read_only_confirmation_files_versions_without_changing_clauses(self):
        before=copy.deepcopy(dict(self.stored()));payload=self.pdf();preview=self.post_pdf('sc_pdf_preview',payload)
        self.assertEqual(preview.status_code,200);self.assertEqual(preview.get_json()['pages'],1)
        self.assertEqual(dict(self.stored()),before);self.r2.upload_bytes_to_r2.assert_not_called()
        self.assertEqual(self.post_pdf('sc_pdf_confirm',payload,token=preview.get_json()['token']).status_code,400)
        self.assertEqual(self.post_pdf('sc_pdf_confirm',payload,token='tampered',confirmed='yes').status_code,409)
        self.assertEqual(self.post_pdf('sc_pdf_confirm',self.pdf('DIFFERENT PDF'),token=preview.get_json()['token'],confirmed='yes').status_code,409)
        self.file(payload);one=copy.deepcopy(self.stored()['items']);first=one['_sc_pdf_current'];self.file(self.pdf('REPLACEMENT OFFICIAL SC'))
        saved=self.stored()['items'];self.assertEqual(saved['_supplementary_conditions'],before['items']['_supplementary_conditions']);self.assertEqual(len(saved['_sc_pdf_versions']),2);self.assertEqual([e['version'] for e in saved['_sc_pdf_versions']],[1,2]);self.assertNotEqual(saved['_sc_pdf_current'],first)
        for key,value in before['items'].items():self.assertEqual(saved[key],value)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_lease_document')).scalar(),2)
        from app.program_billing.lease_sc_pdf import retrieve
        with self.app.test_request_context('/',headers=self.headers):
            restored,_=retrieve({'id':26},dict(id=self.identifier,items=saved),first);self.assertEqual(restored,payload)
    def test_output_and_full_lease_use_exact_current_pdf_and_preserve_core(self):
        from app.program_billing import lease_kit_pdf
        import fitz
        payload=self.pdf();self.file(payload)
        with self.app.test_request_context('/view/26?draft_id='+str(self.identifier)+'&component=special_conditions',headers=self.headers):
            context=self.kit.preview_context(26);self.assertEqual(context['states']['special_conditions']['state'],'filed_pdf')
            self.assertEqual(lease_kit_pdf.build_pdf('special_conditions',context),payload)
        with self.app.test_request_context('/view/26?draft_id='+str(self.identifier)+'&component=lease',headers=self.headers),patch.object(lease_kit_pdf,'core_bytes',return_value=lease_kit_pdf.CORE.read_bytes()):
            context=self.kit.preview_context(26);pdf=lease_kit_pdf.build_pdf('lease',context);pages=[p.get_text() for p in fitz.open(stream=pdf,filetype='pdf')];body=''.join(pages)
            self.assertIn('CURRENT OFFICIAL SC WITH MAP',body);self.assertNotIn('Original first.',body);self.assertLess(body.index('CURRENT OFFICIAL SC WITH MAP'),body.index('SIGNATURES'));self.assertGreaterEqual(len(pages),13)
    def test_filing_routes_are_private_scoped_and_word_conversion_uses_current_pdf(self):
        self.file();items=self.stored()['items'];identifier=items['_sc_pdf_current'];url='/billing/leases/property/26/special-conditions/files/'+identifier+'?draft_id='+str(self.identifier)
        result=self.client.get(url,headers=self.headers);self.assertEqual(result.status_code,200);self.assertEqual(result.data,self.pdf());self.assertEqual(result.headers['Cache-Control'],'private, no-store')
        word=self.client.get(url+'&word=1',headers=self.headers);self.assertEqual(word.status_code,200)
        with ZipFile(io.BytesIO(word.data)) as package:
            xml=ET.fromstring(package.read('word/document.xml'));body=''.join(xml.itertext());self.assertIn('CURRENT OFFICIAL SC WITH MAP',body);self.assertNotIn('Original first.',body);self.assertTrue(package.testzip() is None)
        for path,headers,status in ((url,{},403),(url.replace('/26/','/27/'),self.headers,404),(url.replace(identifier,str(uuid.uuid4())),self.headers,404),(url.replace(str(self.identifier),str(uuid.uuid4())),self.headers,404)):
            self.assertEqual(self.client.get(path,headers=headers).status_code,status)
    def test_shared_help_output_controls_and_filing_cabinet(self):
        self.file()
        output=self.client.get(self.endpoint,headers=self.headers);capture=self.client.get(self.url,headers=self.headers)
        self.assertEqual(output.status_code,200,output.text);self.assertEqual(capture.status_code,200)
        from lxml import html
        output_tree=html.fromstring(output.data);capture_tree=html.fromstring(capture.data)
        self.assertEqual(output_tree.xpath('//h1/text()'),['Output'])
        self.assertEqual(output_tree.xpath('//*[@id="sc-shared-help"]')[0].text_content(),capture_tree.xpath('//*[@id="sc-shared-help"]')[0].text_content())
        for text_value in ('Special Conditions — Preparing, Updating and Filing Your Document','Verification Before Signing','Corrections After Signing','An Addendum does not automatically cure every legal defect.'):
            self.assertIn(text_value,output.text)
        self.assertTrue(output_tree.xpath('//*[@id="lease-filing-cabinet"]//a[contains(@href,"special-conditions/files")]'))
        self.assertTrue(output_tree.xpath('//a[text()="Download as Word"]'));self.assertNotIn('id="sc-import-form"',output.text);self.assertNotIn('id="lease-map-form"',output.text)
    def test_scanned_pdf_conversion_retains_page_image_without_claiming_ocr(self):
        from PIL import Image
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import ImageReader
        out=io.BytesIO();picture=Image.new('RGB',(120,80),'green');page=canvas.Canvas(out,invariant=1);page.drawImage(ImageReader(picture),40,100,width=480,height=320);page.showPage();page.save()
        self.file(out.getvalue());identifier=self.stored()['items']['_sc_pdf_current'];url='/billing/leases/property/26/special-conditions/files/'+identifier+'?draft_id='+str(self.identifier)+'&word=1'
        result=self.client.get(url,headers=self.headers);self.assertEqual(result.status_code,200)
        with ZipFile(io.BytesIO(result.data)) as package:
            self.assertTrue(any(name.startswith('word/media/') for name in package.namelist()));self.assertIn(b'OCR is not performed',package.read('docProps/core.xml'))
    def test_ready_transition_requires_current_review_and_preserves_other_checks(self):
        import types
        from datetime import datetime,timezone
        from flask import request,abort,redirect,flash
        from unittest.mock import Mock
        from test_pm_lease_lessee_persistence import functions
        self.file();save=Mock()
        source={'id':uuid.uuid4(),'version':1,'sha256':'source-hash'}
        env={'__name__':'app.program_billing.lease_standard','__package__':'app.program_billing','billing_bp':types.SimpleNamespace(route=lambda *a,**k:lambda f:f),'snapshot':types.SimpleNamespace(endpoint=lambda f:f,documents=lambda draft:[]),'context':lambda *a:({'id':26},{'id':self.identifier,'items':copy.deepcopy(self.stored()['items'])}),'deepcopy':copy.deepcopy,'get_source':lambda *a:(source,b'',{}),'request':request,'abort':abort,'authority_parties':lambda items:[],'DICTIONARY':{'version':1},'digest':lambda value:'digest','DISCLAIMER_VERSION':1,'DISCLAIMER_HASH':'notice','particulars_digest':lambda items:'particulars','current_user':self.user,'datetime':datetime,'timezone':timezone,'workflow':types.SimpleNamespace(save_items=save),'readiness':lambda *a:[],'flash':flash,'redirect':redirect,'url_for':lambda *a,**k:'/standard'}
        functions('app/program_billing/lease_standard.py',{'workspace'},env)
        from werkzeug.exceptions import BadRequest
        data={'acknowledge':'on','request_ready':'on'}
        with self.app.test_request_context('/standard',method='POST',data=data):
            with self.assertRaises(BadRequest):env['workspace'](26)
        save.assert_not_called()
        with self.app.test_request_context('/standard',method='POST',data={**data,'review_complete':'on'}):self.assertEqual(env['workspace'](26).status_code,302)
        self.assertEqual(save.call_args.args[1]['_ait_standard']['status'],'ready-for-signature')
        save.reset_mock();env['readiness']=lambda *a:['Legal release approval still required.']
        with self.app.test_request_context('/standard',method='POST',data={**data,'review_complete':'on'}):
            with self.assertRaises(BadRequest):env['workspace'](26)
        save.assert_not_called()

    def test_signed_data_stale_upload_and_storage_failure_are_protected(self):
        payload=self.pdf();before=copy.deepcopy(dict(self.stored()));token=self.post_pdf('sc_pdf_preview',payload).get_json()['token']
        self.r2.upload_bytes_to_r2.side_effect=RuntimeError('storage unavailable')
        self.assertEqual(self.post_pdf('sc_pdf_confirm',payload,token=token,confirmed='yes').status_code,503);self.assertEqual(dict(self.stored()),before)
        self.r2.upload_bytes_to_r2.side_effect=None
        self.session.execute(text('UPDATE bil_lease_draft SET version=version+1 WHERE id=:id'),{'id':self.identifier});self.session.commit()
        self.assertEqual(self.post_pdf('sc_pdf_confirm',payload,token=token,confirmed='yes').status_code,409)
        items=copy.deepcopy(before['items']);items['_lease_history']['documents']=[{'kind':'signed_original','sha256':'unchanged'}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.identifier,'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.post_pdf('sc_pdf_preview',payload).status_code,409);self.assertEqual(self.stored()['items'],items)
    def test_upload_rejects_invalid_encrypted_active_and_unauthorised_pdfs(self):
        from pypdf import PdfWriter
        from pypdf.generic import NameObject,DictionaryObject,TextStringObject
        before=copy.deepcopy(dict(self.stored()))
        self.assertEqual(self.post_pdf('sc_pdf_preview',b'not a PDF').status_code,400)
        encrypted=io.BytesIO();writer=PdfWriter();writer.add_blank_page(width=595,height=842);writer.encrypt('test-password');writer.write(encrypted)
        self.assertEqual(self.post_pdf('sc_pdf_preview',encrypted.getvalue()).status_code,400)
        active=io.BytesIO();writer=PdfWriter();writer.add_blank_page(width=595,height=842);writer._root_object[NameObject('/OpenAction')]=DictionaryObject({NameObject('/S'):NameObject('/JavaScript'),NameObject('/JS'):TextStringObject('app.alert("test")')});writer.write(active)
        self.assertEqual(self.post_pdf('sc_pdf_preview',active.getvalue()).status_code,400)
        self.headers={};self.assertEqual(self.post_pdf('sc_pdf_preview').status_code,403);self.headers={'X-Test-Owner':'yes'}
        self.endpoint=self.endpoint.replace('/26/','/27/');self.assertEqual(self.post_pdf('sc_pdf_preview').status_code,404)
        self.assertEqual(dict(self.stored()),before);self.r2.upload_bytes_to_r2.assert_not_called()

    def test_final_review_is_required_content_bound_and_audited(self):
        from app.program_billing.lease_signature_review import valid,ACKNOWLEDGEMENT
        def review(checked=True):return self.client.post(self.endpoint,headers=self.headers,data={'csrf_token':self.csrf,'action':'record_signature_review','version':str(self.stored()['version']),'review_complete':'on' if checked else ''})
        self.assertEqual(review().status_code,409);self.file();self.assertFalse(valid(self.stored()['items']));self.assertEqual(review(False).status_code,400)
        self.assertEqual(review().status_code,200);items=self.stored()['items'];self.assertTrue(valid(items));self.assertEqual(items['_signature_review']['acknowledgement'],ACKNOWLEDGEMENT);self.assertEqual(len(items['_signature_review_history']),1)
        from test_pm_lease_lessee_persistence import functions
        from hashlib import sha256
        env={'json':json,'sha256':sha256};functions('app/program_billing/lease_standard.py',{'digest','particulars_digest'},env)
        without_review={k:v for k,v in items.items() if k not in ('_signature_review','_signature_review_history')}
        self.assertEqual(env['particulars_digest'](items),env['particulars_digest'](without_review))
        changed=copy.deepcopy(items);changed['_parties']['lessees']=[{'legal_name':'Changed tenant'}];self.assertFalse(valid(changed))
        self.file(self.pdf('NEW SC REQUIRES NEW REVIEW'));self.assertFalse(valid(self.stored()['items']));self.assertEqual(len(self.stored()['items']['_signature_review_history']),1)
        self.assertEqual(review().status_code,200);self.assertEqual(len(self.stored()['items']['_signature_review_history']),2)

if __name__=='__main__':unittest.main()
