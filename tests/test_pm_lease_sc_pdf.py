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
        result=self.post_pdf('sc_pdf_upload',payload);self.assertEqual(result.status_code,200,result.text)
        self.assertTrue(result.get_json()['saved']);self.assertIn('/special-conditions/files/',result.get_json()['pdf_url'])
        return result
    def test_upload_without_clauses_and_internal_versions_are_recoverable(self):
        items=copy.deepcopy(self.stored()['items']);items.pop('_supplementary_conditions',None)
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.identifier,'items':json.dumps(items)});self.session.commit()
        payload=self.pdf();self.file(payload);first=self.stored()['items']['_sc_pdf_current'];self.file(self.pdf('REPLACEMENT'))
        saved=self.stored()['items'];self.assertNotIn('_supplementary_conditions',saved)
        self.assertEqual([v['version'] for v in saved['_sc_pdf_versions']],[1,2]);self.assertNotEqual(saved['_sc_pdf_current'],first)
        for key,value in items.items():self.assertEqual(saved[key],value)
        self.assertEqual(self.session.execute(text('SELECT count(*) FROM bil_lease_document')).scalar(),2)
        from app.program_billing.lease_sc_pdf import retrieve
        with self.app.test_request_context('/',headers=self.headers):
            restored,_=retrieve({'id':26},dict(id=self.identifier,items=saved),first);self.assertEqual(restored,payload)
        self.r2.upload_bytes_to_r2.assert_called();self.assertIn('private/leases/property-26/',self.r2.upload_bytes_to_r2.call_args.args[0])

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
    def test_shared_help_and_simplified_output_controls(self):
        self.file()
        output=self.client.get(self.endpoint,headers=self.headers);capture=self.client.get(self.url,headers=self.headers)
        self.assertEqual(output.status_code,200,output.text);self.assertEqual(capture.status_code,200)
        from lxml import html
        output_tree=html.fromstring(output.data);capture_tree=html.fromstring(capture.data)
        self.assertEqual(output_tree.xpath('//h1/text()'),['Output'])
        self.assertEqual(output_tree.xpath('//*[@id="sc-shared-help"]')[0].text_content(),capture_tree.xpath('//*[@id="sc-shared-help"]')[0].text_content())
        for text_value in ('Special Conditions — Preparing, Updating and Filing Your Document','Verification Before Signing','Corrections After Signing','An Addendum does not automatically cure every legal defect.'):
            self.assertIn(text_value,output.text)
        for identifier in ('lease-filing-cabinet','lease-final-review-form','sc-pdf-preview','sc-pdf-approved','sc-pdf-confirm'):
            self.assertFalse(output_tree.xpath('//*[@id="'+identifier+'"]'))
        self.assertIn('Select PDF',output.text);self.assertIn('Upload',output.text)
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
    def test_signed_data_and_storage_failures_are_protected(self):
        from botocore.exceptions import ClientError
        before=copy.deepcopy(dict(self.stored()))
        self.r2.upload_bytes_to_r2.side_effect=ClientError({'Error':{'Code':'AccessDenied','Message':'secret private provider details'}},'PutObject')
        result=self.post_pdf('sc_pdf_upload');self.assertEqual(result.status_code,503);self.assertIn('AccessDenied',result.text);self.assertNotIn('secret private',result.text);self.assertEqual(dict(self.stored()),before)
        self.r2.upload_bytes_to_r2.side_effect=ValueError('Private Lease R2 storage has not been verified and approved. Uploads and retrieval remain disabled.')
        result=self.post_pdf('sc_pdf_upload');self.assertEqual(result.status_code,503);self.assertIn('Private Lease R2 storage has not been verified and approved',result.text)
        self.r2.upload_bytes_to_r2.side_effect=None
        items=copy.deepcopy(before['items']);items['_lease_history']['documents']=[{'kind':'signed_original','sha256':'unchanged'}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.identifier,'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.post_pdf('sc_pdf_upload').status_code,409);self.assertEqual(self.stored()['items'],items)

    def test_upload_rejects_invalid_encrypted_active_and_unauthorised_pdfs(self):
        from pypdf import PdfWriter
        from pypdf.generic import NameObject,DictionaryObject,TextStringObject
        before=copy.deepcopy(dict(self.stored()))
        self.assertEqual(self.post_pdf('sc_pdf_upload',b'not a PDF').status_code,400)
        encrypted=io.BytesIO();writer=PdfWriter();writer.add_blank_page(width=595,height=842);writer.encrypt('test-password');writer.write(encrypted)
        self.assertEqual(self.post_pdf('sc_pdf_upload',encrypted.getvalue()).status_code,400)
        active=io.BytesIO();writer=PdfWriter();writer.add_blank_page(width=595,height=842);writer._root_object[NameObject('/OpenAction')]=DictionaryObject({NameObject('/S'):NameObject('/JavaScript'),NameObject('/JS'):TextStringObject('app.alert("test")')});writer.write(active)
        self.assertEqual(self.post_pdf('sc_pdf_upload',active.getvalue()).status_code,400)
        self.headers={};self.assertEqual(self.post_pdf('sc_pdf_upload').status_code,403);self.headers={'X-Test-Owner':'yes'}
        self.endpoint=self.endpoint.replace('/26/','/27/');self.assertEqual(self.post_pdf('sc_pdf_upload').status_code,404)
        self.assertEqual(dict(self.stored()),before);self.r2.upload_bytes_to_r2.assert_not_called()

    def test_upload_merges_metadata_into_latest_locked_draft(self):
        from app.program_billing import lease_snapshot
        original=lease_snapshot.draft_row
        def latest(property_id,draft_id,lock=False):
            if lock:
                items=copy.deepcopy(self.stored()['items']);items['_concurrent_saved_particular']='preserve this edit'
                self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb),version=version+1 WHERE id=:id'),{'id':self.identifier,'items':json.dumps(items)})
            return original(property_id,draft_id,lock)
        with patch.object(lease_snapshot,'draft_row',side_effect=latest):self.file()
        self.assertEqual(self.stored()['items']['_concurrent_saved_particular'],'preserve this edit')

    def test_metadata_failure_keeps_previous_current_and_reports_safe_reason(self):
        from sqlalchemy.exc import ProgrammingError
        self.file();before=copy.deepcopy(dict(self.stored()));execute=self.session.execute
        class DatabaseFailure(Exception):pgcode='42501'
        def deny_insert(statement,*args,**kwargs):
            if 'INSERT INTO bil_lease_document' in str(statement):raise ProgrammingError('private SQL','private parameters',DatabaseFailure())
            return execute(statement,*args,**kwargs)
        with patch.object(self.session,'execute',side_effect=deny_insert):result=self.post_pdf('sc_pdf_upload')
        self.assertEqual(result.status_code,503);self.assertIn('database permission denied (42501)',result.text);self.assertNotIn('private parameters',result.text)
        self.assertEqual(dict(self.stored()),before)

    def test_upload_preserves_saved_clauses_and_historical_review_records(self):
        before=copy.deepcopy(self.stored()['items']);before['_signature_review_history']=[{'historical':'unchanged'}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.identifier,'items':json.dumps(before)});self.session.commit()
        self.file()
        for key,value in before.items():self.assertEqual(self.stored()['items'][key],value)

if __name__=='__main__':unittest.main()
