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
    def test_uploaded_sc_uses_shared_inline_viewer_in_component_and_full_lease(self):
        from lxml import html
        self.file()
        for component,expected in (('special_conditions',['lease-sc']),('core',['lease-kit']),('lease',['lease-kit','lease-sc'])):
            response=self.client.get('/view/26?draft_id='+str(self.identifier)+'&component='+component,headers=self.headers)
            self.assertEqual(response.status_code,200,response.text);tree=html.fromstring(response.data)
            viewers=tree.xpath('//*[@data-lease-pdf-viewer]')
            self.assertEqual([viewer.get('data-viewer-prefix') for viewer in viewers],expected)
            identifiers=[node.get('id') for node in viewers for node in node.xpath('.//*[@id]')]
            self.assertEqual(len(identifiers),len(set(identifiers)))
            self.assertEqual(len(tree.xpath('//script[contains(@src,"js/lease_kit.js")]')),1)
            for viewer in viewers:
                self.assertEqual(viewer.xpath('.//input[@aria-label="Page number"]/@value'),['1'])
                self.assertEqual(viewer.xpath('.//button/text()'),['Zoom Out','Zoom In','Previous','Next'])
                self.assertFalse(viewer.xpath('.//a[@download or @target="_blank"]'))
                if viewer.get('data-viewer-prefix')=='lease-sc':
                    url=viewer.get('data-pdf-url');self.assertIn(str(self.identifier),url);self.assertIn('component=special_conditions',url)
                    pdf=self.client.get(url,headers=self.headers);self.assertEqual(pdf.status_code,200,pdf.data[:200]);self.assertEqual(pdf.data,self.pdf())
                    self.assertIn('inline',pdf.headers['Content-Disposition']);self.assertIn('no-store',pdf.headers['Cache-Control'])

    def test_success_message_has_scoped_accessible_contrast_and_underlined_link(self):
        from pathlib import Path
        css=Path('app/static/css/lease_kit.css').read_text(encoding='utf-8')
        self.assertIn('#sc-pdf-success:not(:empty)',css);self.assertIn('#sc-pdf-success a{',css)
        self.assertIn('background:#f0fdf4;color:#14532d',css);self.assertIn('color:#1e40af;text-decoration:underline',css)
        def luminance(color):
            channels=[int(color[i:i+2],16)/255 for i in (0,2,4)]
            linear=[c/12.92 if c<=0.04045 else ((c+0.055)/1.055)**2.4 for c in channels]
            return sum(c*w for c,w in zip(linear,(0.2126,0.7152,0.0722)))
        for foreground in ('14532d','1e40af'):
            self.assertGreater((luminance('f0fdf4')+0.05)/(luminance(foreground)+0.05),4.5)

    def test_compiled_pagination_preserves_core_sc_map_and_sources(self):
        import fitz,re,os
        from pathlib import Path
        from hashlib import sha256
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import ImageReader
        from PIL import Image
        from app.program_billing import lease_kit_pdf
        output=io.BytesIO();source=canvas.Canvas(output,invariant=1)
        picture=Image.new('RGB',(100,100),'green')
        for number in range(2):
            source.drawString(40,800,'DRAFT')
            source.drawString(40,740,'SC contractual wording '+str(number+1)+' refers to clause 7.2 and Version 3 of the municipal tariff.')
            source.drawString(40,710,'Water meter | W-01 | Tenant allocation')
            source.drawImage(ImageReader(picture),60,250,width=180,height=180)
            source.rect(60,250,180,180)
            source.drawString(260,25,'Page '+str(number+1)+' of 2');source.showPage()
        source.save();sc=output.getvalue();self.file(sc);before=copy.deepcopy(dict(self.stored()))
        core=lease_kit_pdf.CORE.read_bytes();core_hash=sha256(core).hexdigest()
        with self.app.test_request_context('/view/26?draft_id='+str(self.identifier)+'&component=lease',headers=self.headers),patch.object(lease_kit_pdf,'core_bytes',return_value=core):
            context=self.kit.preview_context(26);payload=lease_kit_pdf.build_pdf('lease',context)
            certificate=fitz.open(stream=lease_kit_pdf.html_pdf('certificate',context),filetype='pdf');core_start=1+len(certificate)
        document=fitz.open(stream=payload,filetype='pdf');original_core=fitz.open(stream=core,filetype='pdf');original_sc=fitz.open(stream=sc,filetype='pdf')
        for index,page in enumerate(document):
            value=page.get_text();numbers=re.findall(r'(?m)^Page \d+(?: of \d+)?$',value)
            self.assertEqual(numbers,[] if index==0 else ['Page '+str(index)])
            self.assertNotIn('DRAFT',value);self.assertNotIn('FINAL REVIEW',value);self.assertNotIn('Final review draft',value);self.assertNotIn("For the owner's final review.",value);self.assertNotIn(str(self.identifier),value)
            self.assertEqual(value.count('Powered by AIT'),1 if index==0 else 0)
        self.assertIn('LEASE CERTIFICATE',document[1].get_text());self.assertIn('SIGNATURES',document[-1].get_text())
        for i,page in enumerate(original_core):
            original=[line.strip() for line in page.get_text(sort=True).splitlines() if line.strip() and line.strip()!='AIT Residential Core | FINAL REVIEW' and not re.fullmatch(r'Final review \| Page \d+',line.strip())]
            actual=[line.strip() for line in document[core_start+i].get_text(sort=True).splitlines() if line.strip() and not re.fullmatch(r'Page \d+',line.strip())]
            if i==0:
                original=original[original.index('1 Agreement components and interpretation'):]
                actual=actual[actual.index('1 Agreement components and interpretation'):]
            self.assertEqual(actual,original)
        for i,page in enumerate(original_sc):
            compiled=document[core_start+9+i];self.assertIn('SC contractual wording '+str(i+1),compiled.get_text());self.assertIn('Version 3 of the municipal tariff.',compiled.get_text());self.assertIn('Water meter | W-01',compiled.get_text())
            clip=fitz.Rect(50,400,250,610)
            self.assertEqual(page.get_pixmap(clip=clip).samples,compiled.get_pixmap(clip=clip).samples,'Map pixels must remain unchanged')
            self.assertEqual(len(page.get_images()),len(compiled.get_images()))
        self.assertEqual(sha256(lease_kit_pdf.CORE.read_bytes()).hexdigest(),core_hash);self.assertEqual(dict(self.stored()),before)
        if os.getenv('AIT_PDF_SAMPLE')=='1':
            folder=Path('output/pdf');folder.mkdir(parents=True,exist_ok=True);(folder/'Full_Lease_Pagination_Sample.pdf').write_bytes(payload)
            for index in (0,1,core_start,core_start+9,len(document)-1):document[index].get_pixmap(matrix=fitz.Matrix(.9,.9)).save(str(folder/('pagination-page-'+str(index+1)+'.png')))

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
