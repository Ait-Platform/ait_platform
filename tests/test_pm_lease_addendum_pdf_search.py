"""Persistent heading list and clause selection on isolated PostgreSQL/R2 fixtures."""
import copy,io,json,unittest
from unittest.mock import patch
from lxml import html
from pypdf import PdfReader
import test_pm_lease_addendum_workspace as fixture

class FullLeaseSelectionTests(unittest.TestCase):
    stored=fixture.AddendumWorkspaceTests.stored
    payload=fixture.AddendumWorkspaceTests.payload
    sql=fixture.AddendumWorkspaceTests.sql
    signed=fixture.AddendumWorkspaceTests.signed
    pdf=fixture.AddendumWorkspaceTests.pdf
    register_url=fixture.AddendumWorkspaceTests.register_url
    def setUp(self):
        fixture.AddendumWorkspaceTests.setUp(self)
        from app.program_billing import lease_core_master
        listing=patch.object(lease_core_master,'list_keys',return_value=[]);listing.start();self.addCleanup(listing.stop)
        from app.program_billing import lease_kit_pdf
        self.core=patch.object(lease_kit_pdf,'core_bytes',return_value=lease_kit_pdf.CORE.read_bytes());self.core.start();self.addCleanup(self.core.stop)
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import ImageReader
        from PIL import Image
        output=io.BytesIO();page=canvas.Canvas(output,invariant=1)
        page.setFont('Helvetica-Bold',12);page.drawString(40,780,'SC-42 Current uploaded utility provision');page.setFont('Helvetica',10)
        page.drawString(40,750,'CURRENT_UPLOADED_PHRASE is the official text. CURRENT_UPLOADED_PHRASE repeats.');page.drawString(40,730,'This provision supplements Core Lease clause 5.1.');page.showPage()
        page.drawImage(ImageReader(Image.new('RGB',(100,100),'green')),40,400,width=200,height=200);page.showPage()
        page.setFont('Helvetica-Bold',12);page.drawString(40,780,'UNNUMBERED SUPPORTING MATERIAL');page.setFont('Helvetica',10);page.drawString(40,750,'UNNUMBERED_EXACT_PHRASE remains original PDF wording.');page.showPage();page.save()
        self.sc=output.getvalue();result=self.client.post(self.endpoint,data={'csrf_token':self.csrf,'action':'sc_pdf_upload','source':(io.BytesIO(self.sc),'Current-SC.pdf')},content_type='multipart/form-data');self.assertEqual(result.status_code,200,result.text)

    def index(self):
        return self.client.get(self.register_url().split('?')[0],query_string={'draft_id':str(self.draft),'action':'clause_index'})

    def select(self,result,entry,checked=True,**changes):
        data={'csrf_token':self.csrf,'action':'select_lease_pdf','register_version':str(self.stored()['version']),'source_sha256':result['source_sha256'],'provision_id':entry['id'] if checked else result['source_sha256']+':'+entry['id'],'selected':'yes' if checked else 'no'};data.update(changes)
        return self.client.post(self.register_url(),data=data)

    def test_complete_persistent_heading_index_no_pdf_work_or_original_wording(self):
        from app.program_billing import lease_kit_pdf,lease_clause_index
        before=copy.deepcopy(dict(self.stored()))
        with patch.object(lease_kit_pdf,'build_pdf',side_effect=AssertionError('PDF work during heading selection')),patch.object(lease_clause_index,'entries',wraps=lease_clause_index.entries) as builder:
            first=self.index();self.assertEqual(first.status_code,200,first.text);index=first.json
            self.assertEqual({entry['source'] for entry in index['results']},{'certificate','core','special_conditions','signature'})
            self.assertTrue(any(e['reference']=='5.4' for e in index['results']))
            self.assertTrue(any(e['reference']=='SC-01' for e in index['results']))
            self.assertFalse(any('original' in e or 'search_text' in e or 'page' in e for e in index['results']))
            self.assertTrue(any('not verified' in notice for notice in index['notices']))
            second=self.index();self.assertEqual(second.json,index);self.assertEqual(builder.call_count,1)
        after=copy.deepcopy(dict(self.stored()));saved=after['items']['_lease_history'].pop('full_lease_clause_index');self.assertEqual(after,before)
        self.assertEqual(saved['source_version'],index['source_sha256'])
        page=self.client.get(self.register_url()+'&workspace=selection');self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);self.assertEqual(tree.xpath('//thead/tr/th/text()'),['Select','Clause / Field'])
        self.assertEqual(len(tree.xpath('//*[@id="lease-pdf-search-results"]/tr')),len(index['results']))
        self.assertNotIn('Preparing the Clause Index',page.text)
        self.assertFalse(tree.xpath('//input[@name="q"]'));self.assertNotIn('Search clauses:',page.text)
        header=tree.xpath('//*[@id="lease-clause-header"]')[0]
        self.assertIn('justify-between',header.get('class'));self.assertEqual(header.xpath('./h2/text()'),['Select Clause'])
        self.assertEqual(header.xpath('./a[@id="lease-pdf-finish"]/text()'),['Finish Selection'])

    def test_multiple_selections_persist_remove_and_generate_addendum_without_lease_changes(self):
        before=copy.deepcopy(self.stored()['items']);index=self.index().json
        entries=[next(e for e in index['results'] if e['source']==source) for source in ('core','special_conditions')]
        for entry in entries:
            self.assertEqual(self.select(index,entry).status_code,200)
            self.assertEqual(self.select(index,entry).status_code,200)
        self.assertEqual(len(self.index().json['state']['selected']),2)
        response=self.client.post(self.register_url(),data={'csrf_token':self.csrf,'action':'addendum_preview','wording':'The agreed amendment only.'})
        self.assertEqual(response.status_code,200,response.data[:100]);pages=PdfReader(io.BytesIO(response.data)).pages;body=''.join(p.extract_text() for p in pages)
        self.assertIn('NOTICE REGARDING CHANGES TO CLAUSES:',body);self.assertIn('All other terms and conditions',body)
        self.assertIn('FOR AND ON BEHALF OF THE LESSOR',pages[-1].extract_text());self.assertIn('Witness 2',pages[-1].extract_text())
        self.assertEqual(self.select(index,entries[0],False).status_code,200);self.assertEqual(len(self.index().json['state']['selected']),1)
        after=self.stored()['items'];self.assertEqual({k:v for k,v in before.items() if k!='_lease_history'},{k:v for k,v in after.items() if k!='_lease_history'})
        self.assertEqual(before['_lease_history']['documents'],after['_lease_history']['documents'])

    def test_index_rebuilds_on_saved_heading_or_full_lease_change(self):
        first=self.index().json
        items=copy.deepcopy(self.stored()['items']);items['_supplementary_conditions']['clauses'][0]['heading']='Corrected saved heading'
        self.sql("UPDATE bil_lease_draft SET items='"+json.dumps(items).replace("'","''")+"'::jsonb,version=version+1");self.session.commit()
        second=self.index().json;self.assertNotEqual(first['source_sha256'],second['source_sha256'])
        self.assertTrue(any(e['title']=='Corrected saved heading' for e in second['results']))
        self.assertFalse(any('CURRENT_UPLOADED_PHRASE' in e['title'] for e in second['results']))
        self.assertEqual(self.select(first,first['results'][0]).status_code,409)

    def test_json_csrf_access_and_failure_leave_saved_selections_unchanged(self):
        index=self.index().json;entry=index['results'][0];before=copy.deepcopy(self.stored()['items'])
        for changes,status in (({'csrf_token':'invalid'},400),({'register_version':'0'},409),({'provision_id':'not-in-index'},404)):
            response=self.select(index,entry,**changes);self.assertEqual(response.status_code,status,response.text);self.assertTrue(response.is_json)
        other=self.client.get(self.register_url().replace('/26/','/27/')+'&action=clause_index');self.assertEqual(other.status_code,404);self.assertTrue(other.is_json)
        self.assertEqual(self.stored()['items'],before)

    def test_missing_source_clear_error_manual_retry_no_background(self):
        from app.program_billing import lease_core_master
        with patch.object(lease_core_master,'headings',side_effect=ValueError('Synthetic missing headings')):
            page=self.client.get(self.register_url()+'&workspace=selection');self.assertEqual(page.status_code,200);self.assertIn('unavailable',page.text)
            response=self.index();self.assertEqual(response.status_code,503);self.assertTrue(response.is_json)
        retry=self.client.post(self.register_url(),data={'csrf_token':self.csrf,'action':'retry_clause_index'});self.assertEqual(retry.status_code,200,retry.text);self.assertEqual(retry.json['status'],'ready')

    def test_addendum_representative_signatures_are_last_with_two_witnesses(self):
        from app.program_billing import lease_signing
        items=copy.deepcopy(self.stored()['items'])
        items['_contracting_lessors']['identities']=[dict(type='individual',name='Owner A',identity_number='A'),dict(type='individual',name='Owner B',identity_number='B')]
        items['_parties']['lessor']=dict(signatory_name='Mandated Agent',signatory_identity_number='AGENT',signatory_capacity='Mandated Agent')
        items['_parties']['lessees']=[dict(party_type='natural_person',legal_name='Natural Tenant',identity_number='TENANT')]
        for party in lease_signing.rows(items):items.setdefault(lease_signing.KEY,{})[party['key']]={'place':'Warner Beach, Durban','date':'2026-10-24'}
        before=copy.deepcopy(items)
        with self.app.test_request_context('/'):
            payload=self.history_env['addendum_pdf']({'id':26,'name':'Almond'},'Original lease','Only amendment text',1,'GENERAL AMENDMENT',[],items)
        pages=PdfReader(io.BytesIO(payload)).pages;signature=pages[-1].extract_text()
        self.assertIn('SIGNATURES',signature);self.assertEqual(signature.count('SIGNED at WARNER BEACH, DURBAN'),2)
        self.assertEqual(signature.count('24th day of OCTOBER 2026'),2);self.assertNotIn('CHANGE FINANCIAL',signature)
        self.assertEqual(signature.count('FOR AND ON BEHALF OF THE LESSOR'),1);self.assertIn('Mandated Agent',signature)
        self.assertIn('LESSEE / AUTHORISED REPRESENTATIVE',signature);self.assertEqual(signature.count('Witness 1 - Name'),2);self.assertEqual(signature.count('Witness 2 - Name'),2)
        self.assertEqual(items,before)
