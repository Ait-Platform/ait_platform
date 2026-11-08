"""Shared report filters, PDF, explicit email and access checks on PostgreSQL."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
from io import BytesIO
from pypdf import PdfReader
import test_pm_statement_filter as fixture

class UtilitiesOutputTests(fixture.StatementFilterTests):
    base='/billing/utilities/26/output'
    def setUp(self):
        super().setUp()
        from app.program_billing import utilities_output
        self.output=utilities_output;self.user.email='manager@example.com'
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(99,26,'REPORT-W','Water'),(97,28,'PRIVATE-W','Water'),(96,26,'REPORT-E','Electrical')"))
        for day,value in [(1,10),(15,20),(30,30)]:
            self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(99,CAST(:d AS date),:v)'),dict(d='2026-09-%02d'%day,v=value))
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(96,'2026-09-15',49.9)"))
        self.session.execute(text('CREATE TEMP TABLE bil_muni_payment(id integer PRIMARY KEY,account_id integer,month text,amount numeric(18,2))'))
        self.session.execute(text("INSERT INTO bil_muni_payment VALUES(1,1,'2026-07',5.55),(2,2,'2026-07',99.99),(3,3,'2026-07',10000)"))
        self.session.commit();self.conn.commit()
    def preview(self,source='readings',item='99',start='2026-09-01',end='2026-09-30'):
        return self.client.get(self.base,query_string=dict(source=source,item_id=item,date_from=start,date_to=end,preview='1'),headers=self.headers)
    def email_fields(self,page):
        tree=html.fromstring(page.data)
        fields={e.get('name'):e.get('value','') for e in tree.xpath('//form[@id="output-email-form"]//input[@name]')}
        fields.update(action='email',email_to='manager@example.com')
        return fields
    def test_inclusive_reading_dates_all_meters_units_and_empty(self):
        before=self.snapshot();page=self.preview();self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);self.assertEqual(len(tree.xpath('//section[@id="utilities-report"]//tbody/tr')),3)
        self.assertIn('10 kL',page.text);self.assertIn('30 kL',page.text)
        page=self.preview(start='2026-09-15',end='2026-09-15')
        self.assertEqual(len(html.fromstring(page.data).xpath('//section[@id="utilities-report"]//tbody/tr')),1)
        page=self.preview(item='',start='',end='');self.assertEqual(page.status_code,200);self.assertIn('All Meters',page.text);self.assertIn('49.9 kWh',page.text);self.assertNotIn('PRIVATE-W',page.text)
        empty=self.preview(start='2025-01-01',end='2025-01-31');self.assertIn('No records match this report.',empty.text)
        self.assertEqual(before,self.snapshot())
    def test_reconciliation_precision_and_account_period_paid(self):
        before=self.snapshot();page=self.preview('reconciliation','1','2026-06-20','2026-07-20')
        self.assertEqual(page.status_code,200,page.text)
        tree=html.fromstring(page.data);rows=tree.xpath('//section[@id="utilities-report"]//tbody/tr')
        self.assertEqual(len(rows),2);self.assertIn('R5.55',rows[0].text_content());self.assertIn('R0.00',rows[1].text_content())
        self.assertIn('R10.01',rows[0].text_content());self.assertIn('R100.02',rows[0].text_content());self.assertNotIn('R99.99',page.text)
        all_accounts=self.preview('reconciliation','','','');self.assertEqual(len(html.fromstring(all_accounts.data).xpath('//section[@id="utilities-report"]//tbody/tr')),9)
        self.assertEqual(before,self.snapshot())
    def test_paid_unavailable_without_account_period_payment_source(self):
        original=self.capture.available
        with patch.object(self.capture,'available',side_effect=lambda table,columns:False if table=='bil_muni_payment' else original(table,columns)):
            page=self.preview('reconciliation','1','2026-07-01','2026-07-31')
        self.assertEqual(page.status_code,200)
        table=html.fromstring(page.data).xpath('//section[@id="utilities-report"]//tbody')[0]
        self.assertIn('Unavailable',table.text_content());self.assertNotIn('R5.55',table.text_content())
    def test_invalid_dates_permissions_and_preview_requirement(self):
        invalid=self.preview(start='2026-10-01',end='2026-09-01');self.assertEqual(invalid.status_code,400);self.assertIn('Date From must not be later',invalid.text)
        self.assertEqual(self.preview(start='bad').status_code,400)
        self.assertEqual(self.preview(item='97').status_code,403)
        self.assertEqual(self.preview('reconciliation','3').status_code,403)
        self.assertEqual(self.client.get(self.base).status_code,403)
        self.assertEqual(self.client.get(self.base.replace('/26/','/28/'),headers=self.headers).status_code,403)
        pdf=self.client.get(self.base+'?source=readings&format=pdf',headers=self.headers);self.assertEqual(pdf.status_code,400)
        self.assertEqual(self.client.post(self.base,data={},headers=self.headers).status_code,400)
    def test_pdf_reports_match_preview_and_have_no_controls(self):
        before=self.snapshot()
        for source,item,start,end in [('readings','99','2026-09-01','2026-09-30'),('reconciliation','1','2026-06-20','2026-07-20')]:
            page=self.preview(source,item,start,end);fields=self.email_fields(page)
            response=self.client.get(self.base,query_string=dict(fields,format='pdf'),headers=self.headers)
            self.assertEqual(response.status_code,200,response.text if response.mimetype!='application/pdf' else '')
            self.assertEqual(response.mimetype,'application/pdf');self.assertIn('inline',response.headers['Content-Disposition'])
            reader=PdfReader(BytesIO(response.data));self.assertGreaterEqual(len(reader.pages),1)
            content=' '.join(p.extract_text() or '' for p in reader.pages)
            self.assertIn('Property ID 26',content);self.assertIn('2026-09-30' if source=='readings' else 'R100.02',content)
            self.assertNotIn('Send PDF',content);self.assertNotIn('Preview',content)
            from pathlib import Path
            import fitz
            out=Path('tmp/pdfs/utilities-output');out.mkdir(parents=True,exist_ok=True)
            (out/(source+'.pdf')).write_bytes(response.data)
            document=fitz.open(stream=response.data,filetype='pdf');document[0].get_pixmap(matrix=fitz.Matrix(1.4,1.4)).save(str(out/(source+'.png')))
        self.assertEqual(before,self.snapshot())
    def test_email_success_uses_existing_pdf_helper_and_explicit_post_only(self):
        before=self.snapshot();page=self.preview();fields=self.email_fields(page)
        with patch('app.utils.mailer.send_pdf_email',return_value=True) as sender:
            self.assertEqual(self.client.get(self.base,headers=self.headers).status_code,200);sender.assert_not_called()
            result=self.client.post(self.base,data=fields,headers=self.headers)
            self.assertEqual(result.status_code,302,result.text);sender.assert_called_once()
            args=sender.call_args.args;self.assertEqual(args[0],'manager@example.com');self.assertTrue(args[3].startswith(b'%PDF-'))
            self.assertIn('REPORT-W',' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(args[3])).pages))
            success=self.client.get(result.location,headers=self.headers);self.assertIn('Mail transport accepted the Utilities report. Inbox delivery is not confirmed.',success.text)
        self.assertEqual(before,self.snapshot())
    def test_email_pdf_failures_validation_and_stale_preview(self):
        fields=self.email_fields(self.preview());before=self.snapshot()
        with patch.object(self.output,'render_pdf',return_value=b'%PDF-test'),patch.object(self.output,'email_pdf',side_effect=RuntimeError('test delivery failure')):
            with self.assertLogs(self.app.logger,level='ERROR'):result=self.client.post(self.base,data=fields,headers=self.headers)
            self.assertEqual(result.status_code,503);self.assertIn('Delivery was not confirmed',result.text)
        with patch.object(self.output,'render_pdf',side_effect=RuntimeError('test renderer unavailable')),patch.object(self.output,'email_pdf') as sender:
            with self.assertLogs(self.app.logger,level='ERROR'):result=self.client.post(self.base,data=fields,headers=self.headers)
            self.assertEqual(result.status_code,503);self.assertIn('PDF could not be generated',result.text);sender.assert_not_called()
        with patch.object(self.output,'email_pdf') as sender:
            self.assertEqual(self.client.post(self.base,data=dict(fields,email_to='bad address'),headers=self.headers).status_code,400)
            self.assertEqual(self.client.post(self.base,data=dict(fields,preview_token='bad'),headers=self.headers).status_code,400)
            self.assertEqual(self.client.post(self.base,data=dict(fields,item_id='1'),headers=self.headers).status_code,400)
            self.assertEqual(self.client.post(self.base,data=dict(fields,item_id='97'),headers=self.headers).status_code,403);sender.assert_not_called()
        self.assertEqual(before,self.snapshot())
        self.session.execute(text("INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(99,'2026-09-20',25)"));self.session.commit();self.conn.commit()
        with patch.object(self.output,'email_pdf') as sender:
            stale=self.client.post(self.base,data=fields,headers=self.headers);self.assertEqual(stale.status_code,409);self.assertIn('records changed after preview',stale.text);sender.assert_not_called()
    def test_suppressed_transport_never_reports_acceptance(self):
        fields=self.email_fields(self.preview())
        with patch.dict(self.app.config,MAIL_SUPPRESS_SEND=True), patch.object(self.output,'render_pdf',return_value=b'%PDF-test'), patch('app.utils.mailer.send_pdf_email') as sender:
            with self.assertLogs(self.app.logger,level='ERROR'):
                result=self.client.post(self.base,data=fields,headers=self.headers)
            self.assertEqual(result.status_code,503)
            self.assertIn('Delivery was not confirmed',result.text)
            sender.assert_not_called()

    def test_output_menu_is_right_aligned_above_selection_block(self):
        page=self.preview()
        tree=html.fromstring(page.data)
        actions=tree.xpath('//*[@id="output-actions"]')[0]
        self.assertNotEqual(actions.getparent().get('id'),'output-filters')
        self.assertEqual(actions.get('aria-label'),'Output Menu')
        self.assertTrue(actions.xpath('./button[@name="preview" and @form="output-filters"]'))
        self.assertIn('justify-end',actions.get('class'))
        self.assertEqual(actions.xpath('./button/text()|./a/text()'),['Preview','Print','Email','Download PDF'])
        self.assertEqual(tree.xpath('//form[@id="output-filters"]//select/@name'),['source','report','item_id','recipient_id'])
        self.assertEqual(tree.xpath('//form[@id="output-filters"]//input[@type="date"]/@name'),['date_from','date_to'])

    def test_browser_print_email_controls_and_dirty_filter_protection(self):
        from playwright.sync_api import sync_playwright
        page=self.preview()
        with sync_playwright() as runtime:
            browser=runtime.chromium.launch(headless=True)
            try:
                tab=browser.new_page();tab.route('**/*',lambda r:r.abort());tab.set_content(page.text,wait_until='domcontentloaded')
                tab.add_script_tag(path='app/static/js/pm_utilities_output.js');tab.evaluate('() => {window.printCalls=0;window.print=()=>{window.printCalls++;};}')
                tab.click('#output-print');self.assertEqual(tab.evaluate('window.printCalls'),1)
                tab.click('#output-email');self.assertTrue(tab.locator('#output-email-form').is_visible());self.assertEqual(tab.locator('#email-to').input_value(),'manager@example.com')
                tab.select_option('#output-source','reconciliation');self.assertTrue(tab.locator('#output-print').is_disabled());self.assertTrue(tab.locator('#output-email').is_disabled());self.assertFalse(tab.locator('#output-email-form').is_visible())
                self.assertEqual(tab.locator('#output-item option').count(),3)
                tab.emulate_media(media='print');self.assertEqual(tab.locator('.output-controls').first.evaluate('(e)=>getComputedStyle(e).display'),'none')
            finally:browser.close()

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(UtilitiesOutputTests(name) for name in UtilitiesOutputTests.__dict__ if name.startswith('test_'))
