"""Word headings/tables and immutable private Almond map integration on local temporary fixtures."""
import copy,io,json,unittest
from unittest.mock import patch
from zipfile import ZipFile,ZIP_DEFLATED
from lxml import etree as ET,html
from sqlalchemy import text
import test_pm_lease_sc_import as fixture

class WordTablesAndMapTests(unittest.TestCase):
    setUp=fixture.SpecialConditionsImportTests.setUp
    stored=fixture.SpecialConditionsImportTests.stored
    payload=fixture.SpecialConditionsImportTests.payload
    document=fixture.SpecialConditionsImportTests.document
    post=fixture.SpecialConditionsImportTests.post
    def map_upload(self,payload,filename='Map.pdf',**changes):
        data={'csrf_token':self.csrf,'action':'map_upload','version':str(self.stored()['version']),'map':(io.BytesIO(payload),filename)};data.update(changes)
        return self.client.post(self.endpoint,headers=self.headers,data=data,content_type='multipart/form-data')
    def test_styled_bold_sc01_to_sc15_and_genuine_table_round_trip(self):
        from app.program_billing.lease_sc_blocks import plain_text
        from app.program_billing import lease_sc_export
        clauses=[('Heading '+str(i),'Original wording '+str(i)) for i in range(1,16)]
        self.assertEqual(self.client.post(self.url,headers=self.headers,data=self.payload(clauses)).status_code,302)
        saved=copy.deepcopy(self.stored()['items']['_supplementary_conditions']['clauses'])
        blocks=[{'kind':'paragraph','text':'Meter particulars.'},{'kind':'table','rows':[['Service','Meter'],['Water','W-26'],['Electricity','E-26']]}]
        saved[-1].update(heading='Utility Register',blocks=blocks,text=plain_text(blocks))
        payload=self.document(saved);w='{'+lease_sc_export.W+'}'
        with ZipFile(io.BytesIO(payload)) as old:
            root=ET.fromstring(old.read('word/document.xml'));body=root.find(w+'body')
            for p in list(body):
                if p.tag!=w+'p':continue
                literal=''.join(p.itertext())
                if not literal.startswith('SC-'):continue
                label,heading=literal.split(' — ',1)
                for child in list(p):p.remove(child)
                prop=ET.SubElement(p,w+'pPr');ET.SubElement(prop,w+'pStyle',{w+'val':'Heading1'})
                run=ET.SubElement(p,w+'r');rp=ET.SubElement(run,w+'rPr');ET.SubElement(rp,w+'b');ET.SubElement(run,w+'t').text=label
                if label=='SC-15':
                    hp=ET.Element(w+'p');pr=ET.SubElement(hp,w+'pPr');ET.SubElement(pr,w+'pStyle',{w+'val':'Heading2'});ET.SubElement(ET.SubElement(hp,w+'r'),w+'t').text=heading
                    body.insert(list(body).index(p)+1,hp)
                else:ET.SubElement(ET.SubElement(p,w+'r'),w+'t').text=' — '+heading
            output=io.BytesIO()
            with ZipFile(output,'w',ZIP_DEFLATED) as new:
                for name in old.namelist():new.writestr(name,ET.tostring(root) if name=='word/document.xml' else old.read(name))
        before=copy.deepcopy(dict(self.stored()));source=output.getvalue()
        response=self.post('sc_import_preview',source);self.assertEqual(response.status_code,200,response.text)
        preview=response.get_json();self.assertEqual([r['number'] for r in preview['comparison']],['SC-%02d'%i for i in range(1,16)])
        self.assertEqual(preview['comparison'][-1]['proposed_blocks'][-1],blocks[-1]);self.assertEqual(dict(self.stored()),before)
        confirmed=self.post('sc_import_confirm',source,token=preview['token'],confirmed='yes');self.assertEqual(confirmed.status_code,200,confirmed.text)
        identifier=confirmed.get_json()['draft_id'];row=self.session.execute(text('SELECT * FROM bil_lease_draft WHERE id=:id'),{'id':identifier}).mappings().one()
        self.assertEqual(row['items']['_supplementary_conditions']['clauses'][-1]['blocks'][-1],blocks[-1]);self.assertEqual(dict(self.stored()),before)
        from datetime import datetime,timezone
        exported=lease_sc_export.build_word({'id':26,'name':'Almond'},row,datetime.now(timezone.utc))
        with ZipFile(io.BytesIO(exported)) as archive:
            xml=ET.fromstring(archive.read('word/document.xml'));self.assertEqual(len(xml.findall('.//'+w+'tbl')),1);self.assertIn('W-26',''.join(xml.itertext()))
        view=self.client.get('/view/26?draft_id='+identifier+'&component=special_conditions',headers=self.headers)
        self.assertEqual(view.status_code,200);self.assertTrue(html.fromstring(view.data).xpath('//table[@class="legal-utility-table"]//td[text()="W-26"]'))
        data=self.payload([(c['heading'],c['text']) for c in row['items']['_supplementary_conditions']['clauses']]);data['version']=str(row['version'])
        data['sc_text_1']='Changed first clause only';data['sc_custom_heading_15']='Updated Utility Register heading'
        url=self.url.replace(str(self.identifier),identifier)
        self.assertEqual(self.client.post(url,headers=self.headers,data=data).status_code,302)
        current=self.session.execute(text('SELECT items FROM bil_lease_draft WHERE id=:id'),{'id':identifier}).scalar()
        self.assertEqual(current['_supplementary_conditions']['clauses'][-1]['blocks'][-1],blocks[-1]);self.assertEqual(current['_supplementary_conditions']['clauses'][-1]['heading'],'Updated Utility Register heading')
    def test_map_pdf_and_image_are_private_immutable_and_follow_sc_in_full_lease(self):
        from reportlab.pdfgen import canvas
        from app.program_billing import lease_kit_pdf
        from PIL import Image
        import fitz
        source=io.BytesIO();page=canvas.Canvas(source,invariant=1);page.drawString(42,700,'MAP UNIQUE ALMOND');page.showPage();page.save();payload=source.getvalue()
        before=copy.deepcopy(dict(self.stored()));response=self.map_upload(payload);self.assertEqual(response.status_code,200,response.text)
        identifier=response.get_json()['draft_id'];self.assertEqual(dict(self.stored()),before)
        row=self.session.execute(text('SELECT * FROM bil_lease_draft WHERE id=:id'),{'id':identifier}).mappings().one();entry=row['items']['_map_annexures'][0]
        self.assertEqual(self.objects[entry['storage_key']],payload);self.assertEqual(row['items']['_supplementary_conditions'],before['items']['_supplementary_conditions'])
        with self.app.test_request_context('/view/26?draft_id='+identifier+'&component=lease',headers=self.headers),patch.object(lease_kit_pdf,'core_bytes',return_value=lease_kit_pdf.CORE.read_bytes()):
            context=self.kit.preview_context(26);pdf=lease_kit_pdf.build_pdf('lease',context);self.assertEqual(pdf,lease_kit_pdf.build_pdf('lease',context))
            pages=[p.get_text() for p in fitz.open(stream=pdf,filetype='pdf')];alltext=''.join(pages)
            self.assertLess(alltext.index('Original second.'),alltext.index('Map Annexure'));self.assertLess(alltext.index('MAP UNIQUE ALMOND'),alltext.index('SIGNATURES'))
        image=io.BytesIO();Image.new('RGB',(120,80),'green').save(image,format='PNG');image_bytes=image.getvalue()
        result=self.map_upload(image_bytes,'Almond.png');self.assertEqual(result.status_code,200,result.text)
        row=self.session.execute(text('SELECT * FROM bil_lease_draft WHERE id=:id'),{'id':result.get_json()['draft_id']}).mappings().one();entry=row['items']['_map_annexures'][0]
        self.assertEqual(self.objects[entry['storage_key']],image_bytes)
        from app.program_billing.lease_map_annexure import compiled
        with self.app.test_request_context('/',headers=self.headers):
            output=list(compiled({'property':{'id':26,'name':'Almond'},'lease_draft':row,'premises_name':'Almond'}))[0]
            document=fitz.open(stream=output,filetype='pdf');self.assertEqual(len(document),1);self.assertIn('Map Annexure',document[0].get_text());self.assertTrue(document[0].get_images())
    def test_map_validation_stale_auth_and_signed_draft_protection(self):
        before=copy.deepcopy(dict(self.stored()))
        for payload,name in ((b'not an image','Map.png'),(b'<svg/>','Map.svg'),(b'not a pdf','Map.pdf')):
            response=self.map_upload(payload,name);self.assertEqual(response.status_code,400);self.assertIn('error',response.get_json())
        self.assertEqual(self.map_upload(b'x',version='0').status_code,409)
        self.assertEqual(dict(self.stored()),before);self.r2.upload_bytes_to_r2.assert_not_called()
        self.headers={};self.assertEqual(self.map_upload(b'x').status_code,403);self.headers={'X-Test-Owner':'yes'}
        items=copy.deepcopy(before['items']);items['_lease_history']['documents']=[{'kind':'signed_original'}]
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb) WHERE id=:id'),{'id':self.identifier,'items':json.dumps(items)});self.session.commit()
        self.assertEqual(self.map_upload(b'x').status_code,409);self.r2.upload_bytes_to_r2.assert_not_called()

if __name__=='__main__':unittest.main()
