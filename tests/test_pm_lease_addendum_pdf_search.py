"""Full Lease PDF-only search and clause selection on isolated PostgreSQL/R2 fixtures."""
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
    def search(self,query):
        # Compatibility helper exercises extraction/cache logic directly; the UI
        # uses only the separately tested asynchronous clause_index endpoint.
        with self.app.test_request_context('/?draft_id='+str(self.draft)+'&q='+__import__('urllib.parse',fromlist=['quote']).quote(query),headers=self.headers):
            prop,draft=self.history_env['context'](26)
            return self.history_env['full_lease_search'](prop,draft).json
    def select(self,result,entry,checked=True,**changes):
        data={'csrf_token':self.csrf,'action':'select_lease_pdf','register_version':str(self.stored()['version']),'source_sha256':result['source_sha256'],'provision_id':entry['id'] if checked else result['source_sha256']+':'+entry['id'],'selected':'yes' if checked else 'no'};data.update(changes)
        return self.client.post(self.register_url(),data=data)
    def test_search_certificate_core_uploaded_sc_actual_pages_grouping_and_nonsearchable_notice(self):
        before=copy.deepcopy(dict(self.stored()));certificate=self.search('Security deposit');self.assertTrue(any(e['source']=='certificate' and e['reference']=='Security deposit' for e in certificate['results']))
        signature=self.search('SIGNED at');self.assertTrue(any(e['source']=='signature' for e in signature['results']))
        core=self.search('5.1');self.assertEqual(len([e for e in core['results'] if e['source']=='core' and e['reference']=='5.1']),1);self.assertFalse(any(e['reference']=='15.1' for e in core['results']))
        current=self.search('CURRENT_UPLOADED_PHRASE');self.assertEqual(len(current['results']),1);entry=current['results'][0];self.assertEqual(entry['source'],'special_conditions');self.assertEqual(entry['reference'],'SC-42');self.assertIn('CURRENT_UPLOADED_PHRASE',entry['excerpt'])
        source=self.client.get(self.register_url().split('?')[0],query_string={'action':'lease_pdf','draft_id':str(self.draft),'source_sha256':current['source_sha256']});self.assertEqual(source.status_code,200);self.assertIn('inline',source.headers['Content-Disposition']);self.assertIn('no-store',source.headers['Cache-Control'])
        pdf=PdfReader(io.BytesIO(source.data));self.assertIn('CURRENT_UPLOADED_PHRASE',pdf.pages[entry['page']-1].extract_text())
        for section,reference in ((certificate,'Security deposit'),(core,'5.1'),(current,'SC-42')):
            indexed=next(e for e in section['results'] if e['reference']==reference);self.assertIn(reference,pdf.pages[indexed['page']-1].extract_text());self.assertTrue(indexed['title']);self.assertEqual(indexed['source_version'],current['source_sha256'])
        self.assertEqual(self.search('Original first')['results'],[]);self.assertTrue(any('no extractable text' in n for n in current['notices']));after=copy.deepcopy(dict(self.stored()));index=after['items']['_lease_history'].pop('full_lease_clause_index');self.assertEqual(after,before);self.assertEqual(index['source_sha256'],current['source_sha256']);self.assertTrue(all(e['source_version']==current['source_sha256'] for e in index['entries']))
    def test_multiple_selections_survive_searches_duplicate_checks_removal_and_clean_finish(self):
        before=copy.deepcopy(self.stored()['items']);core=self.search('5.1');entry=next(e for e in core['results'] if e['source']=='core' and e['reference']=='5.1');result=self.select(core,entry);self.assertEqual(result.status_code,200,result.text)
        after=self.stored()['items'];self.assertEqual({k:v for k,v in after.items() if k!='_lease_history'},{k:v for k,v in before.items() if k!='_lease_history'});self.assertEqual(after['_lease_history']['documents'],before['_lease_history']['documents'])
        current=self.search('CURRENT_UPLOADED_PHRASE');self.assertEqual(len(current['state']['selected']),1);sc=current['results'][0];self.assertEqual(self.select(current,sc).status_code,200);self.assertEqual(self.select(current,sc).status_code,200)
        preview=self.client.post(self.register_url(),data={'csrf_token':self.csrf,'action':'addendum_preview','subject':'Two PDF provisions','wording':'The amendment covers both selected provisions.'});self.assertEqual(preview.status_code,200,preview.data[:200]);body=''.join(p.extract_text() for p in PdfReader(io.BytesIO(preview.data)).pages);self.assertIn('Core Lease',body);self.assertIn('5.1',body);self.assertIn('SC-42',body);self.assertIn('NOTICE REGARDING CHANGES TO CLAUSES:',body)
        page=self.client.get(self.register_url().split('?')[0],query_string={'workspace':'selection','draft_id':str(self.draft)});self.assertEqual(page.status_code,200,page.text);tree=html.fromstring(page.data)
        self.assertIn('SEARCH LEASE AGREEMENT',page.text);self.assertTrue(tree.xpath('//*[@data-lease-pdf-viewer and @data-viewer-prefix="lease-selection"]'));finish=tree.xpath('//*[@id="lease-pdf-finish"]/@href')[0]
        main=self.client.get(finish);self.assertEqual(main.status_code,200,main.text);body=html.fromstring(main.data);state=json.loads(body.xpath('//*[@id="addendum-clause-data"]/text()')[0]);self.assertEqual(len(state['selected']),2)
        self.assertEqual(body.xpath('//*[@id="addendum-workflow"]/button/text()'),['Select Clause','Create Addendum','View Addendum','Help']);self.assertEqual(len(body.xpath('//button[@data-sc-help-open]')),1);self.assertIn('flex-wrap:nowrap',main.text);self.assertFalse(body.xpath('//*[@id="addendum-clause-catalog" or @id="addendum-clause-selected" or @id="addendum-clause-relationships"]'));self.assertEqual(body.xpath('//*[@id="addendum-clauses"]//th/text()'),['Page','Clause / Field','Matching Text']);self.assertTrue(all(len(cell.text_content())<=220 for cell in body.xpath('//*[@id="addendum-clauses"]//tr/td[3]')))
        self.assertEqual(self.select(current,sc,False).status_code,200);self.assertEqual(len(self.search('Security deposit')['state']['selected']),1)
    def test_unidentified_reference_retains_page_excerpt_and_create_view_signature_output(self):
        match=self.search('UNNUMBERED_EXACT_PHRASE');entry=match['results'][0];self.assertEqual(entry['reference'],'');self.assertEqual(self.select(match,entry).status_code,200)
        data={'csrf_token':self.csrf,'selection_mode':'full_pdf','action':'addendum_preview','subject':'Actual PDF amendment','wording':'The owner supplies this amendment wording.'}
        before=copy.deepcopy(dict(self.stored()));preview=self.client.post(self.register_url(),data=data);self.assertEqual(preview.status_code,200,preview.data[:200]);document=PdfReader(io.BytesIO(preview.data));text=''.join(p.extract_text() for p in document.pages)
        self.assertIn('PDF page '+str(entry['page'])+' (text excerpt)',text);self.assertNotIn('SC-06',text);self.assertIn('SUBJECT: UNNUMBERED_EXACT_PHRASE',text);self.assertNotIn('Actual PDF amendment',text);self.assertIn('SIGNATURES',document.pages[-1].extract_text());self.assertEqual(dict(self.stored()),before)
        data['action']='create_pdf';saved=self.client.post(self.register_url(),data=data);self.assertEqual(saved.status_code,302,saved.text);output=self.client.get(self.endpoint);self.assertIn('Saved Addendums',output.text)
    def test_general_addendum_without_signed_copy_verification_and_signed_record_preservation(self):
        self.signed();original=copy.deepcopy(self.stored()['items']['_lease_history']['documents'][0]);data={'csrf_token':self.csrf,'selection_mode':'full_pdf','action':'addendum_preview','subject':'General amendment','wording':'General agreed change without an identified clause.'}
        before=copy.deepcopy(dict(self.stored()))
        with patch.dict(self.history_env,document=lambda *args:(_ for _ in ()).throw(AssertionError('No signed-copy retrieval is required'))):
            response=self.client.post(self.register_url(),data=data);self.assertEqual(response.status_code,200,response.data[:200]);body=''.join(page.extract_text() for page in PdfReader(io.BytesIO(response.data)).pages);self.assertIn('GENERAL ADDENDUM',body);self.assertIn(original['lease_reference'],body);self.assertEqual(dict(self.stored()),before)
            data['action']='create_pdf';saved=self.client.post(self.register_url(),data=data);self.assertEqual(saved.status_code,302,saved.text);self.assertIn('saved_addendum=',saved.location)
        self.assertEqual(self.stored()['items']['_lease_history']['documents'][0],original)

    def test_unreadable_text_returns_notice_without_fabricated_matches(self):
        class BrokenText:
            def __iter__(self):return iter([self])
            def get_text(self,*args,**kwargs):raise RuntimeError('Unreadable font data')
            def close(self):pass
        with patch('fitz.open',return_value=BrokenText()):
            units,notices=self.history_env['full_lease_provisions'](b'ignored fixture')
        self.assertEqual(units,[]);self.assertIn('could not be extracted',notices[0])

    def test_source_mismatch_csrf_and_access_scope_preserve_saved_state(self):
        match=self.search('CURRENT_UPLOADED_PHRASE');entry=match['results'][0];before=copy.deepcopy(dict(self.stored()))
        self.assertEqual(self.select(match,entry,source_sha256='wrong').status_code,409);self.assertEqual(self.select(match,entry,csrf_token='').status_code,400);self.assertEqual(self.client.get(self.register_url().split('?')[0].replace('/26/','/27/'),query_string={'action':'search_lease','q':'5.1','draft_id':str(self.draft)}).status_code,404);self.assertEqual(dict(self.stored()),before)
        self.client.environ_base.pop('HTTP_X_TEST_OWNER');self.assertEqual(self.select(match,entry).status_code,403)

    def test_index_reused_across_queries_selections_pdf_and_worker_restart(self):
        extractor=self.history_env['full_lease_provisions']
        from app.program_billing import lease_kit_pdf
        with patch.dict(self.history_env,full_lease_provisions=__import__('unittest.mock',fromlist=['Mock']).Mock(wraps=extractor)):
            with patch.object(lease_kit_pdf,'build_pdf',wraps=lease_kit_pdf.build_pdf) as build:
                first=self.search('');self.assertGreater(len(first['results']),10)
                self.assertTrue(any(e['source']=='certificate' for e in first['results']));self.assertTrue(any(e['source']=='core' for e in first['results']));self.assertTrue(any(e['reference']=='SC-42' for e in first['results']))
                version=self.stored()['version'];indexed=copy.deepcopy(self.stored()['items']['_lease_history']['full_lease_clause_index'])
                self.search('rent');self.search('SC-42');self.assertEqual(build.call_count,1)
                entry=next(e for e in first['results'] if e['reference']=='SC-42');self.assertEqual(self.select(first,entry).status_code,200)
                again=self.search('');self.assertEqual(again['source_sha256'],first['source_sha256']);self.assertEqual(build.call_count,1)
                self.app.extensions.pop('lease_clause_pdf_cache',None)
                self.search('deposit');self.assertEqual(build.call_count,1)
                pdf=self.client.get(self.register_url().split('?')[0],query_string={'action':'lease_pdf','draft_id':str(self.draft)});self.assertEqual(pdf.status_code,200);self.assertEqual(build.call_count,1)
                self.assertEqual(self.history_env['full_lease_provisions'].call_count,1)
                self.assertEqual(self.stored()['items']['_lease_history']['full_lease_clause_index'],indexed)
                self.assertEqual(self.stored()['version'],version+1) # selection only, never indexing

    def test_index_rebuilt_after_current_uploaded_pdf_changes_and_old_selection_not_reused(self):
        first=self.search('CURRENT_UPLOADED_PHRASE');self.assertEqual(self.select(first,first['results'][0]).status_code,200)
        from reportlab.pdfgen import canvas
        output=io.BytesIO();page=canvas.Canvas(output,invariant=1);page.drawString(40,780,'SC-77 Replacement current provision');page.drawString(40,750,'REPLACEMENT_CURRENT_TEXT');page.save()
        response=self.client.post(self.endpoint,data={'csrf_token':self.csrf,'action':'sc_pdf_upload','source':(io.BytesIO(output.getvalue()),'Replacement-SC.pdf')},content_type='multipart/form-data');self.assertEqual(response.status_code,200,response.text)
        new=self.search('');self.assertNotEqual(new['source_sha256'],first['source_sha256']);self.assertTrue(any(e['reference']=='SC-77' for e in new['results']));self.assertFalse(any(e['reference']=='SC-42' for e in new['results']))
        self.assertEqual(self.search('CURRENT_UPLOADED_PHRASE')['results'],[])
        preview=self.client.post(self.register_url(),data={'csrf_token':self.csrf,'action':'addendum_preview','subject':'Changed source','wording':'Do not reuse stale references.'});self.assertEqual(preview.status_code,400);self.assertIn('Full Lease changed',preview.json['error'])
        self.assertEqual(self.select(first,first['results'][0]).status_code,409)

    def test_index_failure_is_explicit_without_fallback_or_partial_cache(self):
        from app.program_billing import lease_kit_pdf
        before=copy.deepcopy(dict(self.stored()))
        with patch.object(lease_kit_pdf,'build_pdf',side_effect=RuntimeError('Synthetic renderer failure')):
            with self.assertRaises(__import__('werkzeug.exceptions',fromlist=['ServiceUnavailable']).ServiceUnavailable) as failed:self.search('')
        self.assertIn('Clause Index could not be generated',failed.exception.description);self.assertEqual(dict(self.stored()),before)

    def test_same_pdf_digest_reuses_extraction_even_when_inputs_need_revalidation(self):
        from app.program_billing import lease_clause_index,lease_kit_pdf
        from unittest.mock import Mock
        extractor=Mock(wraps=self.history_env['full_lease_provisions'])
        with patch.dict(self.history_env,full_lease_provisions=extractor):
            first=self.search('');version=self.stored()['version'];inputs=lease_clause_index.inputs
            def changed_inputs(*args):
                context,fingerprint=inputs(*args)
                return context,'revalidated-'+fingerprint
            with patch.object(lease_clause_index,'inputs',side_effect=changed_inputs),patch.object(lease_kit_pdf,'build_pdf',wraps=lease_kit_pdf.build_pdf) as build:
                second=self.search('');self.assertEqual(build.call_count,1)
            self.assertEqual(second['source_sha256'],first['source_sha256']);self.assertEqual(extractor.call_count,1);self.assertEqual(self.stored()['version'],version)

    def test_selection_json_contract_includes_security_and_pre_dispatch_errors(self):
        match=self.search('SC-42');entry=next(e for e in match['results'] if e['reference']=='SC-42')
        before=copy.deepcopy(dict(self.stored()))
        missing=self.select(match,entry,csrf_token='');self.assertEqual(missing.status_code,400);self.assertTrue(missing.is_json);self.assertIn('security token',missing.json['error'])
        foreign=self.client.post(self.register_url().replace('/26/','/27/'),data={'action':'select_lease_pdf','csrf_token':self.csrf});self.assertEqual(foreign.status_code,404);self.assertTrue(foreign.is_json)
        self.client.environ_base.pop('HTTP_X_TEST_OWNER');denied=self.select(match,entry);self.assertEqual(denied.status_code,403);self.assertTrue(denied.is_json);self.client.environ_base['HTTP_X_TEST_OWNER']='yes'
        from flask import abort,redirect
        with patch.dict(self.history_env,context=lambda *args:abort(503)):
            failed=self.select(match,entry);self.assertEqual(failed.status_code,503);self.assertTrue(failed.is_json)
        with patch.dict(self.history_env,full_lease_select=lambda *args:redirect('/login')):
            expired=self.select(match,entry);self.assertEqual(expired.status_code,401);self.assertTrue(expired.is_json)
        with patch.dict(self.history_env,full_lease_select=lambda *args:abort(500)):
            failed=self.select(match,entry);self.assertEqual(failed.status_code,500);self.assertTrue(failed.is_json)
        self.assertEqual(dict(self.stored()),before)
        saved=self.select(match,entry);self.assertEqual(saved.status_code,200);self.assertTrue(saved.is_json);self.assertIsInstance(saved.json['state']['selected'],list);self.assertIsInstance(saved.json['state']['version'],int)

    def test_automatic_subject_precedence_and_saved_signing_statements_with_witnesses(self):
        from app.program_billing import lease_signing
        from sqlalchemy import text
        items=copy.deepcopy(self.stored()['items']);items['_parties']={'lessees':[{'party_type':'natural_person','tenant_id':11,'legal_name':'Natural Person','identity_number':'PERSON-11'}]}
        items['_signing_particulars']={party['key']:{'place':'Warner Beach, Durban','date':'2026-10-24'} for party in lease_signing.rows(items)}
        self.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb),version=version+1 WHERE id=:id'),{'items':json.dumps(items),'id':self.draft});self.session.commit()
        match=self.search('SC-42');entry=next(e for e in match['results'] if e['reference']=='SC-42');self.assertEqual(self.select(match,entry).status_code,200)
        before=copy.deepcopy(dict(self.stored()));response=self.client.post(self.register_url(),data={'action':'addendum_preview','csrf_token':self.csrf,'subject':'FORGED MANUAL SUBJECT','wording':'Only this amendment content is supplied by the user.'});self.assertEqual(response.status_code,200,response.data[:200])
        document=PdfReader(io.BytesIO(response.data));body=' '.join(' '.join(p.extract_text() for p in document.pages).split());statement='SIGNED at WARNER BEACH, DURBAN on this the 24th day of OCTOBER 2026'
        self.assertEqual(body.count(statement),2);self.assertIn('SUBJECT: SC-42 Current uploaded utility provision',body);self.assertNotIn('FORGED MANUAL SUBJECT',body);self.assertIn(self.history_env['addendum_precedence'](),body)
        signatures=' '.join(document.pages[-1].extract_text().split());self.assertIn('LESSOR',signatures);self.assertIn('LESSEE',signatures);self.assertEqual(signatures.count('Witness 1 - name:'),2);self.assertEqual(signatures.count('Witness 2 - signature:'),2);self.assertNotIn('Place of signing:',signatures);self.assertNotIn('Date of signing:',signatures);self.assertNotIn('SIGNATURES',document.pages[0].extract_text())
        page=self.client.get(self.register_url());tree=html.fromstring(page.data);self.assertEqual(tree.xpath('//*[@id="addendum-create-form"]//input[not(@type="hidden")]'),[]);self.assertEqual(tree.xpath('//*[@id="addendum-create-form"]//textarea/@name'),['wording']);self.assertIn(entry['title'],tree.xpath('//*[@id="addendum-subject"]')[0].text_content());self.assertEqual(dict(self.stored()),before)
        import os
        if os.getenv('AIT_ADDENDUM_FINAL_SAMPLE')=='1':
            from pathlib import Path
            import fitz
            folder=Path('tmp/pdfs/addendum-final');folder.mkdir(parents=True,exist_ok=True);(folder/'addendum.pdf').write_bytes(response.data)
            pdf=fitz.open(stream=response.data,filetype='pdf')
            for index,p in enumerate(pdf):p.get_pixmap(matrix=fitz.Matrix(1.2,1.2)).save(str(folder/('addendum-'+str(index+1)+'.png')))
