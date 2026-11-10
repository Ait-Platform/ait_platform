"""Stage 3A tests: deterministic HTTP fixtures, temp evidence, no live website."""
import ast
from datetime import datetime,timezone
from io import BytesIO
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock,patch
from urllib.error import HTTPError,URLError
from lxml import html
from reportlab.pdfgen import canvas
from sqlalchemy import event,text
from test_pm_admin_landing import ROOT,function
from test_pm_annual_tariff_register import AnnualRegisterTests


class PublicationTests(AnnualRegisterTests):
    publication_url='/admin/billing/tariffs/municipal-publications'

    def setUp(self):
        super().setUp()
        self.modules={}
        for short in ('municipal_tariff_sources','municipal_tariff_documents','municipal_tariff_download','municipal_tariff_auto_activation','municipal_tariff_policy','municipal_tariff_audit_storage'):
            name='app.admin.programs.billing.'+short
            module=types.ModuleType(name)
            if short=='municipal_tariff_documents':
                module.official_url=self.source.official_url;module.year_start=self.source.year_start
            tree=ast.parse((ROOT/('app/admin/programs/billing/'+short+'.py')).read_text(encoding='utf8'))
            tree.body=[node for node in tree.body if not (isinstance(node,ast.ImportFrom) and node.level)]
            exec(compile(tree,short+'.py','exec'),module.__dict__)
            self.modules[name]=module
            if short=='municipal_tariff_sources':self.source=module
            if short=='municipal_tariff_documents':self.documents=module
            if short=='municipal_tariff_download':self.controller=module
            if short=='municipal_tariff_policy':self.policy=module
        patched=patch.dict(sys.modules,self.modules);patched.start();self.addCleanup(patched.stop)
        self.controller.services=lambda:(self.tariff_service,self.source,self.documents)
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.app.config['PM_TARIFF_DOCUMENT_ROOT']=self.folder.name
        env=self.app.view_functions['admin_bp.billing_tariffs'].__globals__
        for name in ('billing_tariff_publications','billing_tariff_publication_pdf'):
            self.app.view_functions['admin_bp.'+name]=function('app/admin/programs/billing/routes.py',name,env)
        self.markup=(ROOT/'tests/fixtures/pm_tariff_sources/listing.html').read_bytes()
        buffer=BytesIO();pdf=canvas.Canvas(buffer,invariant=1)
        pdf.drawString(40,800,'TEST FIXTURE - tariff document 2026/2027');pdf.save()
        self.pdf=buffer.getvalue();self.calls=[]

    def fetch(self,url,limit,deadline):
        self.calls.append(url)
        if limit==self.source.MAX_HTML:return self.markup,{'content-type':'text/html'},url
        return self.pdf,{'content-type':'application/pdf','last-modified':'Mon, 22 Jun 2026 00:00:00 GMT'},url

    def run_download(self,year='2026/27'):
        form=html.fromstring(self.client.get(self.publication_url).data)
        csrf=form.xpath('//input[@name="csrf_token"]/@value')[0]
        discovery=self.source.discovery;retrieve=self.source.retrieve
        with patch.object(self.source,'discovery',side_effect=lambda year,**kw:discovery(year,fetch=self.fetch,**kw)),patch.object(self.source,'retrieve',side_effect=lambda candidate,**kw:retrieve(candidate,fetch=self.fetch,**kw)):
            return self.client.post(self.publication_url,data=dict(csrf_token=csrf,financial_year=year))

    def test_discovery_labels_years_dates_and_official_only_sources(self):
        rows,issues=self.source.discovery('2026/27',fetch=self.fetch)
        self.assertFalse(issues);self.assertEqual(len(rows),3)
        self.assertEqual({row['publication_status'] for row in rows},{'DRAFT','PROPOSED','FINAL_LABELLED'})
        self.assertEqual(rows[0]['publication_date'],'2026-06-22')
        self.assertTrue(all(row['financial_year']=='2026/27' for row in rows))
        self.assertTrue(all('evil.example' not in row['source_url'] for row in rows))
        self.assertEqual(self.source.years_in('tariffs-20262027.pdf'),['2026/27'])

    def test_retrieval_validates_pdf_and_never_confirms_adoption(self):
        candidate=self.source.discovery('2026/27',fetch=self.fetch)[0][0]
        content,record=self.source.retrieve(candidate,fetch=self.fetch)
        self.assertEqual(content,self.pdf);self.assertEqual(record['page_count'],1)
        self.assertEqual(record['publication_status'],'FINAL_LABELLED')
        self.assertEqual(record['approval_status'],'UNVERIFIED')
        self.assertIn('2026/27',record['document_years'])
        for invalid in (b'<html>error</html>',b'%PDF-invalid'):
            with self.assertRaises(self.source.RetrievalError):
                self.source.retrieve(candidate,fetch=lambda *args:(invalid,{},candidate['source_url']))

    def test_redirects_and_invalid_source_addresses_fail_closed(self):
        for url in ('http://www.durban.gov.za/x','https://127.0.0.1/x','file:///etc/passwd','https://www.durban.gov.za.evil.example/x','https://user:pass@www.durban.gov.za/x','https://www.durban.gov.za:8888/x'):
            with self.subTest(url=url),self.assertRaises(self.source.RetrievalError):self.source.official_url(url)
        response=HTTPError('https://www.durban.gov.za',302,'redirect',{'Location':'https://evil.example/tariffs.pdf'},BytesIO())
        opener=Mock();opener.open.side_effect=response
        with patch.object(self.source,'build_opener',return_value=opener),self.assertRaises(self.source.RetrievalError):
            self.source.read_url('https://www.durban.gov.za/uploads/test.pdf',self.source.MAX_PDF,float('inf'))
        self.assertEqual(opener.open.call_count,1)

    def test_network_failure_missing_year_and_challenge_are_reported(self):
        def failure(*args):raise self.source.RetrievalError('Synthetic network failure')
        rows,issues=self.source.discovery('2026/27',fetch=failure)
        self.assertFalse(rows);self.assertIn('Synthetic network failure',' '.join(issues))
        rows,issues=self.source.discovery('2027/28',fetch=self.fetch)
        self.assertFalse(rows);self.assertIn('No matching tariff PDFs',' '.join(issues))
        rows,issues=self.source.discovery('2026/27',fetch=lambda url,*args:(b'<title>Just a moment</title>',{'content-type':'text/html'},url))
        self.assertFalse(rows);self.assertIn('browser challenge',' '.join(issues))
        with self.assertRaises(self.source.RetrievalError):self.source.year_start('2026/99')

    def test_immutable_storage_duplicates_receipts_and_integrity(self):
        candidate=self.source.discovery('2026/27',fetch=self.fetch)[0][0]
        content,record=self.source.retrieve(candidate,fetch=self.fetch)
        with self.app.app_context():
            store=self.documents.backend()
            first=self.documents.save(store,content,record);second=self.documents.save(store,content,record)
            self.assertNotEqual(first['receipt_id'],second['receipt_id'])
            self.assertEqual(len(list(Path(self.folder.name).rglob('*.pdf'))),1)
            self.assertEqual(self.documents.read(store,'2026/27',first['receipt_id']),content)
            list(Path(self.folder.name).rglob('*.pdf'))[0].write_bytes(b'%PDF-corrupted')
            with self.assertRaises(self.documents.StorageError):self.documents.read(store,'2026/27',first['receipt_id'])
            with self.assertRaises(self.documents.StorageError):self.documents.read(store,'2026/27','../other')
            with self.assertRaises(self.documents.StorageError):self.documents.Disk(ROOT/'app/static/not-private')

    def test_admin_download_workflow_changes_no_tariff_rows(self):
        before=self.snapshot();queries=[]
        def record(conn,cursor,sql,parameters,context,many):queries.append(sql)
        event.listen(self.conn,'before_cursor_execute',record)
        try:response=self.run_download()
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(response.status_code,200,response.text)
        self.assertIn('3 municipal source document(s) retained',response.text)
        self.assertIn('UNVERIFIED',response.text);self.assertNotIn('Confirm Approve and Activate',response.text)
        self.assertEqual(before,self.snapshot())
        self.assertTrue(all(sql.lstrip().upper().startswith('SELECT') for sql in queries),queries)
        path=html.fromstring(response.data).xpath('//a[contains(text(),"Download stored PDF")]/@href')[0]
        stored=self.client.get(path);self.assertEqual(stored.status_code,200);self.assertEqual(stored.data,self.pdf)
        self.assertEqual(stored.headers['X-Content-Type-Options'],'nosniff')
        self.assertIn('Download Municipal Tariffs',self.client.get(self.url).text)
        self.conn.execute(text("UPDATE pm_admin_test_identity SET role='manager'"));self.conn.commit()
        self.assertEqual(self.client.get(self.publication_url).status_code,403)
        self.assertEqual(self.client.get(path).status_code,403)
        with self.client.session_transaction() as state:state.clear()
        self.assertEqual(self.client.get(self.publication_url).status_code,302)

    def test_get_is_network_free_and_post_requires_csrf(self):
        with patch.object(self.source,'discovery',side_effect=AssertionError('GET must not fetch')):
            page=self.client.get(self.publication_url);self.assertEqual(page.status_code,200)
        self.assertEqual(self.client.post(self.publication_url,data=dict(financial_year='2026/27')).status_code,400)
        self.assertFalse(list(Path(self.folder.name).rglob('*')))

    def test_storage_failure_preserves_existing_sources_and_rates(self):
        self.run_download();before=self.snapshot();existing=list(Path(self.folder.name).rglob('*.pdf'))
        with patch.object(self.documents,'save',side_effect=self.documents.StorageError('Synthetic storage failure')):
            response=self.run_download()
        self.assertIn('Document storage/read-back failed',response.text)
        self.assertNotIn('3 municipal source document(s) retained',response.text)
        self.assertEqual(before,self.snapshot());self.assertEqual(existing,list(Path(self.folder.name).rglob('*.pdf')))

    def test_existing_r2_transport_is_reused_without_public_user_metadata(self):
        objects={}
        def put(key,content,**kwargs):
            if key in objects:self.assertEqual(objects[key],content)
            objects[key]=content;return key
        api=types.SimpleNamespace(upload_bytes_to_r2=put,read_file_from_r2=lambda key:objects[key])
        candidate=self.source.discovery('2026/27',fetch=self.fetch)[0][0]
        content,record=self.source.retrieve(candidate,fetch=self.fetch)
        with self.app.app_context(),patch.object(self.documents,'import_module',return_value=api):
            store=self.documents.R2();saved=self.documents.save(store,content,record)
            self.assertEqual(self.documents.read(store,'2026/27',saved['receipt_id']),content)
        self.assertTrue(all(key.startswith('municipal-tariffs/ethekwini/2026-27/') for key in objects))
        self.assertTrue(all(b'actor' not in raw for key,raw in objects.items() if key.endswith('.json')))

    def test_download_limits_and_unavailable_durable_storage(self):
        import os
        import time
        response=Mock();response.headers={'Content-Length':str(self.source.MAX_PDF+1)}
        response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        opener=Mock();opener.open.return_value=response
        with patch.object(self.source,'build_opener',return_value=opener),self.assertRaises(self.source.RetrievalError):
            self.source.read_url('https://www.durban.gov.za/uploads/test.pdf',self.source.MAX_PDF,time.monotonic()+10)
        with self.assertRaises(self.source.RetrievalError):
            self.source.read_url('https://www.durban.gov.za/uploads/test.pdf',self.source.MAX_PDF,0)
        with self.app.app_context(),patch.dict(os.environ,{'RENDER':'true'},clear=True):
            self.app.config.pop('PM_TARIFF_DOCUMENT_ROOT')
            with self.assertRaises(self.documents.StorageError):self.documents.backend()

    def test_browser_download_review_and_return_to_hub(self):
        from playwright.sync_api import sync_playwright
        from urllib.parse import urlsplit,parse_qs
        discovery=self.source.discovery;retrieve=self.source.retrieve
        def route_request(route):
            url=urlsplit(route.request.url);path=url.path+('?' + url.query if url.query else '')
            if route.request.method=='POST':
                data={key:values[-1] for key,values in parse_qs(route.request.post_data or '').items()}
                response=self.client.post(path,data=data)
            else:response=self.client.get(path)
            route.fulfill(status=response.status_code,body=response.data,content_type='text/html; charset=utf-8')
        with patch.object(self.source,'discovery',side_effect=lambda year,**kw:discovery(year,fetch=self.fetch,**kw)),patch.object(self.source,'retrieve',side_effect=lambda candidate,**kw:retrieve(candidate,fetch=self.fetch,**kw)),sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page(viewport=dict(width=1440,height=1000))
                page.route('http://tariff-review.test/**',route_request)
                page.goto('http://tariff-review.test'+self.url)
                page.get_by_role('link',name='Download Municipal Tariffs',exact=True).click()
                page.select_option('#publication-year','2026/27')
                page.get_by_role('button',name='Download Municipal Tariffs',exact=True).click()
                self.assertIn('3 municipal source document(s) retained',page.locator('[role=status]').inner_text())
                self.assertEqual(page.get_by_role('link',name='Download stored PDF',exact=True).count(),3)
                self.assertIn('UNVERIFIED',page.locator('body').inner_text())
                artifact=ROOT/'output/annual_tariff_sources_stage3a';artifact.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(artifact/'municipal-download-desktop.png'),full_page=True)
                page.get_by_role('link',name='Close',exact=True).click()
                self.assertEqual(page.locator('#annual-tariff-workspace').count(),0)
            finally:browser.close()


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(PublicationTests(name) for name in PublicationTests.__dict__ if name.startswith('test_'))
