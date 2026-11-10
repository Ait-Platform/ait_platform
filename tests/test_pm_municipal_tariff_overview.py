"""Read-only summary/UI tests; no downloads, updates or activation on hub GET."""
import types
import unittest
from unittest.mock import patch
from lxml import html
from sqlalchemy import event,text
from test_pm_municipal_tariff_verification import VerificationTests


class OverviewTests(VerificationTests):
    def setUp(self):
        super().setUp()
        self.overview=self.load('municipal_tariff_overview',{})

    def test_hub_shows_only_six_summary_items_and_two_main_actions(self):
        before=self.snapshot();queries=[]
        def record(conn,cursor,sql,parameters,context,many):queries.append(sql)
        event.listen(self.conn,'before_cursor_execute',record)
        try:page=self.client.get(self.url)
        finally:event.remove(self.conn,'before_cursor_execute',record)
        self.assertEqual(page.status_code,200,page.text)
        doc=html.fromstring(page.data)
        self.assertEqual(len(doc.xpath('//dl[@id="annual-tariff-summary"]/div')),6)
        self.assertEqual(doc.xpath('//form[@id="annual-tariff-hub"]//button/text()'),['Download New Tariffs','Review Exceptions'])
        self.assertFalse(doc.xpath('//section[@id="annual-tariff-workspace"]'))
        self.assertNotIn('NOT APPROVED',page.text);self.assertNotIn('Approve and Activate',page.text)
        self.assertTrue(all(sql.lstrip().upper().startswith('SELECT') for sql in queries),queries)
        self.assertEqual(before,self.snapshot())

    def test_extracted_counts_use_actual_pdf_not_candidate_worksheet_and_deduplicate(self):
        self.run_download()
        document=types.SimpleNamespace(is_encrypted=False,pages=[types.SimpleNamespace(extract_text=lambda value=value:value) for value in self.fixture['pages'].values()])
        extract=self.verification.extract
        with self.app.app_context(),patch.object(self.verification,'PdfReader',return_value=document),patch.object(self.verification,'extract',wraps=extract) as parser:
            result=self.overview.build('2026/27')
        self.assertEqual(result['download_status'],'Downloaded')
        self.assertEqual(result['extracted'],14)
        self.assertEqual(result['passing'],0);self.assertEqual(result['exceptions'],16)
        self.assertEqual(parser.call_count,1) # Three receipts, one unique source PDF.

    def test_cleared_large_increase_is_not_counted_as_passing_ten_percent(self):
        self.run_download()
        report=dict(rows=[dict(status='VERIFIED',review=dict(check=dict(result='PASS'))),dict(status='VERIFIED',review=dict(check=dict(result='ABOVE 10%'))),dict(status='HOLD',review=dict(check=dict(result='PASS')))])
        with self.app.app_context(),patch.object(self.workflow,'schedule',return_value=report):
            result=self.overview.build('2026/27')
        self.assertEqual(result['passing'],1);self.assertEqual(result['exceptions'],1)

    def test_updated_count_uses_verified_families_in_selected_year(self):
        for identifier,code,when in ((80,'W01','2026-07-01'),(81,'W01','2026-08-01'),(82,'W02','2026-07-01')):
            self.seed(identifier,code,'1',when)
            if identifier!=82:
                self.conn.execute(text("INSERT INTO bil_tariff_verification(tariff_id,municipality,municipal_category,source_reference,effective_date,verified_by) VALUES(:id,'eThekwini Municipality','Domestic','Synthetic verified fixture',:d,7)"),dict(id=identifier,d=when))
        self.conn.commit()
        with self.app.app_context():
            self.assertEqual(self.overview.build('2026/27')['updated'],1)
            self.assertEqual(self.overview.build('2025/26')['updated'],0)

    def test_missing_verification_history_does_not_invent_an_update_count(self):
        with self.app.app_context(),patch.object(self.tariff_service,'verification_records',return_value=None):
            self.assertIsNone(self.overview.build('2026/27')['updated'])

    def test_storage_failure_is_not_reported_as_zero_exceptions(self):
        with self.app.app_context(),patch.object(self.documents,'backend',side_effect=ValueError('Synthetic unavailable storage')):
            result=self.overview.build('2026/27')
        self.assertIn('Unavailable',result['download_status'])
        self.assertIsNone(result['extracted']);self.assertIsNone(result['passing']);self.assertIsNone(result['exceptions'])


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(OverviewTests(name) for name in OverviewTests.__dict__ if name.startswith('test_'))
