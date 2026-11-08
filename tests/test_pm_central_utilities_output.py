"""One output endpoint, seven existing reports, unchanged arithmetic and scope."""
import unittest
from unittest.mock import patch
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from sqlalchemy import text,event
from lxml import html
from pypdf import PdfReader
import test_pm_municipal_metsoa as fixture

class CentralOutputTests(unittest.TestCase):
    url='/billing/utilities/26/output'
    def setUp(self):
        self.fx=fixture.MunicipalMetsoaTests(methodName='test_permissions_and_token_free_pm_access_remain_unchanged')
        self.fx.setUp();self.addCleanup(self.fx.doCleanups);self.addCleanup(self.fx.tearDown)
        from app.program_billing import utilities_output,municipal_readings_pdf,municipal_arrangements
        self.output=utilities_output;self.arrangements=municipal_arrangements
        for module in (utilities_output,municipal_readings_pdf,municipal_arrangements):
            for name,value in [('db',self.fx.db),('current_user',self.fx.user)]:
                if hasattr(module,name):m=patch.object(module,name,value);m.start();self.addCleanup(m.stop)
        self.fx.user.is_authenticated=True
        s=self.fx.session
        s.execute(text('CREATE TEMP TABLE "user"(id integer PRIMARY KEY,is_active boolean)'))
        s.execute(text('INSERT INTO "user" VALUES(7,true)'))
        s.execute(text('CREATE TEMP TABLE bil_municipal_arrangement(id integer PRIMARY KEY,property_id integer,account_id integer,reference text,agreement_date date,original_amount numeric,instalment_amount numeric,created_at timestamp)'))
        s.execute(text('CREATE TEMP TABLE bil_municipal_arrangement_payment(id integer)'))
        s.execute(text('CREATE TEMP TABLE bil_municipal_arrangement_event(id integer)'))
        s.execute(text("INSERT INTO bil_municipal_arrangement VALUES(1,26,1,'CA-EXISTING','2026-09-01',1000,100,'2026-09-01')"))
        s.commit();self.fx.conn.commit()
    def get(self,source,report,**values):
        return self.fx.client.get(self.url,query_string=dict(source=source,report=report,preview='1',billing_month='2026-10',**values),headers=self.fx.headers)
    def fields(self,response):
        tree=html.fromstring(response.data)
        return {e.get('name'):e.get('value','') for e in tree.xpath('//form[@id="output-email-form"]//input[@name]')}
    def test_all_seven_reports_use_one_work_area_and_preserve_records(self):
        from app.program_billing.utilities_output_reports import CATALOG
        before=self.fx.snapshot();queries=[]
        def observe(conn,cursor,statement,params,ctx,many):queries.append(statement.lstrip().lower())
        event.listen(self.fx.conn,'before_cursor_execute',observe)
        try:
            for source,options in CATALOG.items():
                for report,label in options:
                    with self.subTest(report=report):
                        response=self.get(source,report,recipient_id='11')
                        self.assertEqual(response.status_code,200,response.text)
                        tree=html.fromstring(response.data)
                        self.assertEqual(len(tree.xpath('//*[@id="output-preview"]')),1)
                        self.assertIn(label,tree.xpath('//*[@id="utilities-report"]')[0].text_content())
                        self.assertTrue(tree.xpath('//button[@id="output-print" and not(@disabled)]'))
                        self.assertTrue(tree.xpath('//button[@id="output-email" and not(@disabled)]'))
                        if report=='metsoa_statement':
                            self.assertIn('Statement For All Metro Meters',response.text)
                            self.assertFalse(tree.xpath('//*[@id="output-preview"]//form'))
                            self.assertFalse(tree.xpath('//*[@id="metsoa-comparison"]'))
                        if report=='utilities_reconciliation':self.assertTrue(tree.xpath('//*[@id="metsoa-comparison"]'))
                        if report=='municipal_submission':
                            self.assertIn('MUNICIPAL ACKNOWLEDGEMENT OF RECEIPT',response.text)
                            self.assertNotIn('Average Consumption',response.text)
                        if report=='arrangement_register':self.assertIn('800.00',response.text)
        finally:event.remove(self.fx.conn,'before_cursor_execute',observe)
        self.assertFalse(any(q.startswith(('insert ','update ','delete ')) for q in queries))
        self.assertEqual(before,self.fx.snapshot())
    def test_metsoa_output_button_preserves_month_recipient_and_test_mode(self):
        response=self.fx.client.get(self.fx.generation_url+'&test_report=1',headers=self.fx.headers)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data);links=tree.xpath('//a[@id="metsoa-output"]/@href')
        self.assertEqual(len(links),1)
        for part in ('source=metsoa','report=metsoa_statement','billing_month=2026-10','recipient_id=11','test_report=1'):self.assertIn(part,links[0])
        self.assertNotIn('Open PDF',response.text);self.assertNotIn('Download PDF',response.text)
        central=self.fx.client.get(links[0],headers=self.fx.headers);self.assertEqual(central.status_code,200,central.text)
        t=html.fromstring(central.data)
        self.assertEqual(t.xpath('//input[@name="billing_month" and @id="output-month"]/@value'),['2026-10'])
        self.assertEqual(t.xpath('//select[@id="output-recipient"]/option[@selected]/@value'),['11'])
    def test_existing_comparison_and_arrangement_balances_are_reused(self):
        with self.fx.app.test_request_context():original=self.fx.metsoa.build(26,'2026-10',recipient_id=11)
        with self.fx.app.test_request_context():data=self.output.report(26,dict(source='reconciliation',report='utilities_reconciliation',billing_month='2026-10',recipient_id='11',preview='1'))
        self.assertEqual(data['report_payload']['pm_comparisons'],original['pm_comparisons'])
        self.assertEqual(data['report_payload']['grand_total'],original['grand_total'])
        with self.fx.app.test_request_context():data=self.output.report(26,dict(source='arrangements',report='arrangement_register',billing_month='2026-10',preview='1'))
        self.assertEqual(data['report_payload'][0]['scheduled_balance'],Decimal(800))
    def test_report_parameters_and_context_are_bound_to_preview(self):
        page=self.get('readings','history');fields=self.fields(page)
        fields['report']='municipal_submission'
        changed=self.fx.client.get(self.url,query_string=dict(fields,format='pdf'),headers=self.fx.headers)
        self.assertEqual(changed.status_code,400)
        missing=self.get('metsoa','metsoa_statement');self.assertEqual(missing.status_code,400)
        self.assertIn('registered recipient',missing.text)
        self.assertEqual(self.get('metsoa','metsoa_statement',recipient_id='999999').status_code,403)
        self.assertEqual(self.get('accounts','account_register',item_id='999999').status_code,403)
        self.assertEqual(self.fx.client.get(self.url.replace('/26/','/28/'),headers=self.fx.headers).status_code,403)
        self.assertEqual(self.get('accounts','metsoa_statement').status_code,400)
    def test_arrangement_output_retains_active_portfolio_access_rule(self):
        self.fx.session.execute(text('UPDATE "user" SET is_active=false WHERE id=7'))
        self.fx.session.commit();self.fx.conn.commit()
        self.assertEqual(self.get('arrangements','arrangement_register').status_code,403)

    def test_pdf_and_email_dispatch_preserve_original_renderers(self):
        cases=[('readings','history'),('readings','municipal_submission'),('reconciliation','metro_reconciliation'),('reconciliation','utilities_reconciliation'),('metsoa','metsoa_statement'),('accounts','account_register'),('arrangements','arrangement_register')]
        output=Path('tmp/pdfs/central-output');output.mkdir(parents=True,exist_ok=True)
        for source,key in cases:
            with self.subTest(report=key):
                page=self.get(source,key,recipient_id='11');self.assertEqual(page.status_code,200,page.text)
                fields=self.fields(page)
                pdf=self.fx.client.get(self.url,query_string=dict(fields,format='pdf',download='1'),headers=self.fx.headers)
                self.assertEqual(pdf.status_code,200,pdf.text if pdf.mimetype!='application/pdf' else '')
                content=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
                self.assertTrue(content.strip())
                (output/(key+'.pdf')).write_bytes(pdf.data)
                if key=='municipal_submission':self.assertIn('MUNICIPAL ACKNOWLEDGEMENT OF RECEIPT',content)
                if key=='metsoa_statement':self.assertIn('Statement For All Metro Meters',content)
                with patch.object(self.output,'email_pdf',return_value=True) as sender:
                    sent=self.fx.client.post(self.url,data=dict(fields,action='email',email_to='manager@example.com'),headers=self.fx.headers)
                    self.assertEqual(sent.status_code,302,sent.text)
                    self.assertTrue(sender.call_args.args[2].startswith(b'%PDF-'))
    def test_browser_report_choices_and_conditional_month_recipient(self):
        from playwright.sync_api import sync_playwright
        response=self.fx.client.get(self.url,headers=self.fx.headers)
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                page=browser.new_page();page.route('**/*',lambda route:route.abort());page.set_content(response.text);page.add_script_tag(path='app/static/js/pm_utilities_output.js')
                self.assertTrue(page.locator('#output-filters').is_visible())
                self.assertFalse(page.locator('#output-period-controls').is_visible())
                self.assertEqual(page.locator('#output-report option').all_text_contents(),['Reading History','Municipal Reading Submission'])
                page.select_option('#output-source','reconciliation')
                self.assertEqual(page.locator('#output-report option').all_text_contents(),['Metro Reconciliation','Utilities Reconciliation'])
                self.assertFalse(page.locator('#output-period-controls').is_visible())
                page.select_option('#output-report','utilities_reconciliation');self.assertTrue(page.locator('#output-period-controls').is_visible())
                page.select_option('#output-source','metsoa');self.assertTrue(page.locator('#output-period-controls').is_visible())
                page.select_option('#output-source','accounts');self.assertFalse(page.locator('#output-period-controls').is_visible())
                self.assertTrue(page.locator('#output-from').is_disabled())
            finally:browser.close()

if __name__=='__main__':unittest.main()
