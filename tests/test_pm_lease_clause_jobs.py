"""Background index requests never execute a PDF renderer; isolated local PostgreSQL/R2."""
import copy,io,unittest
from unittest.mock import patch
from sqlalchemy import text
import test_pm_lease_addendum_pdf_search as fixture

class BackgroundIndexTests(unittest.TestCase):
    def setUp(self):
        fixture.FullLeaseSelectionTests.setUp(self)
        from app.program_billing import lease_clause_jobs as jobs
        self.user.is_active=True;self.user.is_authenticated=True
        actor=patch.object(jobs,'job_actor',return_value=self.user);actor.start();self.addCleanup(actor.stop)
    stored=fixture.FullLeaseSelectionTests.stored
    payload=fixture.FullLeaseSelectionTests.payload
    sql=fixture.FullLeaseSelectionTests.sql
    signed=fixture.FullLeaseSelectionTests.signed
    pdf=fixture.FullLeaseSelectionTests.pdf
    register_url=fixture.FullLeaseSelectionTests.register_url
    search=fixture.FullLeaseSelectionTests.search
    select=fixture.FullLeaseSelectionTests.select
    def poll(self,**query):
        return self.client.get(self.register_url().split('?')[0],query_string={'draft_id':str(self.draft),'action':'clause_index',**query})
    def test_request_claims_once_background_build_persists_and_reuses_index_and_pdf(self):
        from app.program_billing import lease_clause_jobs as jobs,lease_kit_pdf
        pending=[]
        with patch.object(jobs,'submit',side_effect=lambda *args:pending.append(args)),patch.object(lease_kit_pdf,'build_pdf',wraps=lease_kit_pdf.build_pdf) as build:
            first=self.poll();self.assertEqual(first.status_code,202,first.text);self.assertEqual(first.json['status'],'building');self.assertEqual(build.call_count,0)
            self.assertEqual(self.poll().status_code,202);self.assertEqual(len(pending),1);version=self.stored()['version']
            from flask_login.utils import current_user as real_proxy
            from app.program_billing import lease_sc_pdf,lease_snapshot
            with patch.object(lease_sc_pdf,'current_user',real_proxy),patch.object(lease_snapshot,'current_user',real_proxy):jobs.run(*pending.pop())
            ready=self.poll();self.assertEqual(ready.status_code,200,ready.text);self.assertEqual(ready.json['status'],'ready');self.assertEqual(build.call_count,1)
            self.assertTrue(any(e['source']=='certificate' for e in ready.json['results']));self.assertTrue(any(e['source']=='core' for e in ready.json['results']));sc=next(e for e in ready.json['results'] if e['reference']=='SC-42')
            index=self.stored()['items']['_lease_history']['full_lease_clause_index'];self.assertTrue(index['pdf_parts']);self.assertEqual(self.stored()['version'],version)
            self.app.extensions.pop('lease_clause_pdf_cache',None)
            for _ in range(3):self.assertEqual(self.poll().status_code,200)
            pdf=self.client.get(self.register_url().split('?')[0],query_string={'draft_id':str(self.draft),'action':'lease_pdf'});self.assertEqual(pdf.status_code,200);self.assertEqual(build.call_count,1)
            from pypdf import PdfReader
            self.assertIn('SC-42',PdfReader(io.BytesIO(pdf.data)).pages[sc['page']-1].extract_text());
            import fitz
            compiled=fitz.open(stream=pdf.data,filetype='pdf');self.assertTrue(compiled[sc['page']].get_images());compiled.close();
            compact=lease_kit_pdf.compiled_presentation(self.sc,0);original_get=fitz.Page.get_text
            def including_images(page,*args,**kwargs):
                if args and args[0]=='dict':kwargs.pop('flags',None)
                return original_get(page,*args,**kwargs)
            with patch.object(fitz.Page,'get_text',new=including_images):reference=lease_kit_pdf.compiled_presentation(self.sc,0)
            self.assertEqual(compact,reference,'Skipping discarded image extraction must not change compiled PDF bytes')
            self.assertEqual(self.select(ready.json,sc).status_code,200)
            self.assertEqual(build.call_count,1);self.assertEqual(self.stored()['items']['_lease_history']['full_lease_index_job']['state'],'ready')
            self.session.execute(text("UPDATE bil_lease_draft SET items=jsonb_set(items,'{_index_test_change}','true'::jsonb),version=version+1 WHERE id=:id"),{'id':self.draft});self.session.commit()
            stale_pdf=self.client.get(self.register_url().split('?')[0],query_string={'draft_id':str(self.draft),'action':'lease_pdf'});self.assertEqual(stale_pdf.status_code,409)
            stale_selection=self.select(ready.json,sc);self.assertEqual(stale_selection.status_code,409);self.assertTrue(stale_selection.is_json);self.assertEqual(build.call_count,1,'Stale normal requests must not trigger a cold PDF build')
    def test_failure_is_persisted_retry_is_explicit_and_csrf_is_not_bypassed(self):
        from app.program_billing import lease_clause_jobs as jobs,lease_kit_pdf
        before=copy.deepcopy(self.stored()['items']);pending=[]
        with patch.object(jobs,'submit',side_effect=lambda *args:pending.append(args)):
            self.assertEqual(self.poll().status_code,202)
            with patch.object(lease_kit_pdf,'build_pdf',side_effect=RuntimeError('Synthetic background renderer failure')):jobs.run(*pending.pop())
            failed=self.poll();self.assertEqual(failed.status_code,503);self.assertTrue(failed.is_json);self.assertTrue(failed.json['retryable']);self.assertEqual(len(pending),0)
            invalid=self.client.post(self.register_url(),data={'action':'retry_clause_index'});self.assertEqual(invalid.status_code,400);self.assertTrue(invalid.is_json)
            retry=self.client.post(self.register_url(),data={'action':'retry_clause_index','csrf_token':self.csrf});self.assertEqual(retry.status_code,202,retry.text);self.assertEqual(len(pending),1)
            jobs.run(*pending.pop());self.assertEqual(self.poll().status_code,200)
        after=self.stored()['items'];self.assertEqual({k:v for k,v in after.items() if k!='_lease_history'},{k:v for k,v in before.items() if k!='_lease_history'});self.assertEqual(after['_lease_history']['documents'],before['_lease_history']['documents'])
    def test_actual_sc01_title_and_page_are_indexed_without_database_substitution(self):
        from reportlab.pdfgen import canvas
        from app.program_billing import lease_clause_jobs as jobs
        output=io.BytesIO();page=canvas.Canvas(output,invariant=1);page.setFont('Helvetica-Bold',12);page.drawString(30,780,'SC-01 - PERMITTED USE AND RESIDENT OCCUPATION');page.setFont('Helvetica',10);page.drawString(30,750,'The exact current uploaded provision.');page.save()
        saved=self.client.post(self.endpoint,data={'csrf_token':self.csrf,'action':'sc_pdf_upload','source':(io.BytesIO(output.getvalue()),'SC01.pdf')},content_type='multipart/form-data');self.assertEqual(saved.status_code,200,saved.text)
        pending=[]
        with patch.object(jobs,'submit',side_effect=lambda *args:pending.append(args)):
            self.assertEqual(self.poll().status_code,202);jobs.run(*pending.pop());ready=self.poll().json
        clause=next(e for e in ready['results'] if e['reference']=='SC-01');self.assertIn('PERMITTED USE AND RESIDENT OCCUPATION',clause['title']);self.assertIn('exact current uploaded provision',clause['search_text']);self.assertFalse(any('Original first' in e['search_text'] for e in ready['results']))

    def test_revoked_actor_cannot_build_or_read_private_documents(self):
        from app.program_billing import lease_clause_jobs as jobs,lease_kit_pdf
        from flask import abort
        pending=[]
        with patch.object(jobs,'submit',side_effect=lambda *args:pending.append(args)):
            self.assertEqual(self.poll().status_code,202)
        with patch.object(jobs,'job_actor',side_effect=lambda *args:abort(403)),patch.object(lease_kit_pdf,'build_pdf') as build:
            jobs.run(*pending.pop());build.assert_not_called()
        self.assertEqual(self.stored()['items']['_lease_history']['full_lease_index_job']['state'],'failed')
