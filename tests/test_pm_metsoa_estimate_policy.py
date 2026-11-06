"""Explicit current-tariff fallback; no historical tariff or financial mutations."""
import unittest
from decimal import Decimal
from unittest.mock import patch
from io import BytesIO
from pypdf import PdfReader
from lxml import html
from sqlalchemy import text
import test_pm_metsoa_admin_selection as fixture

class EstimatePolicyTests(fixture.AdminSelectionTests):
    def use_current(self):
        with self.app.test_request_context():return self.metsoa.build(26,'2026-02',recipient_id=11,use_current_tariffs=True)
    def unavailable_history(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-07-01'"))
        self.session.commit();self.conn.commit()
    def tariff_snapshot(self):
        return list(self.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))

    def test_official_historical_rates_win_even_when_checkbox_is_selected(self):
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,rate,effective_date) VALUES('electricity','ELEC01',99,'2026-07-01')"))
        self.session.commit();self.conn.commit()
        data=self.use_current()
        self.assertFalse(data['pm_estimated']);self.assertTrue(data['pm_complete'])
        self.assertEqual(data['electricity']['rows'][0]['rate'],Decimal('3.2795'))
        self.assertEqual(data['pm_estimated_components'],[])

    def test_missing_history_without_consent_keeps_consumption_and_unavailable_rates(self):
        self.unavailable_history();before=self.tariff_snapshot()
        for suffix in ('','&use_current_tariffs=0'):
            page=self.client.get(self.february+suffix,headers=self.headers)
            self.assertEqual(page.status_code,200,page.text)
            self.assertIn('Rate unavailable',page.text)
            self.assertNotIn('HISTORICAL ESTIMATE',page.text)
            self.assertIn('CEL888',html.fromstring(page.data).get_element_by_id('metsoa-consumption').text_content())
            self.assertFalse(html.fromstring(page.data).xpath('//input[@name="use_current_tariffs"][@checked]'))
        self.assertEqual(before,self.tariff_snapshot())

    def test_consent_prices_estimate_preserves_dates_and_marks_comparisons(self):
        self.unavailable_history();before=self.tariff_snapshot();readings=self.snapshot()
        bills=list(self.session.execute(text('SELECT * FROM bil_municipal_statement ORDER BY id')))
        data=self.use_current()
        self.assertTrue(data['pm_estimated']);self.assertTrue(data['pm_pricing_complete']);self.assertFalse(data['pm_complete'])
        self.assertTrue(data['electricity']['rows']);self.assertTrue(data['water']['meters'])
        comparison=next(r for r in data['pm_comparisons'] if r['account']=='83192896040')
        self.assertIsNotNone(comparison['difference']);self.assertTrue(comparison['label'].startswith('Estimated '))
        self.assertTrue(all(r['effective_date'].isoformat()=='2026-07-01' for r in data['pm_estimated_components']))
        page=self.client.get(self.february+'&use_current_tariffs=1',headers=self.headers)
        tree=html.fromstring(page.data)
        self.assertIn('HISTORICAL ESTIMATE',tree.get_element_by_id('historical-estimate').text_content())
        self.assertIn('CURRENT TARIFFS APPLIED',page.text)
        self.assertIn('Estimated utilities total',page.text)
        self.assertNotIn('Amount Due R',page.text)
        self.assertTrue(tree.xpath('//input[@name="use_current_tariffs"][@checked]'))
        self.assertTrue(all('use_current_tariffs=1' in link for link in tree.xpath('//a[contains(@href,"format=pdf")]/@href')))
        pdf=self.client.get(self.february+'&use_current_tariffs=1&format=pdf&download=1',headers=self.headers)
        self.assertEqual(pdf.status_code,200)
        self.assertIn('attachment',pdf.headers['Content-Disposition'])
        content=' '.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn('HISTORICAL ESTIMATE',content);self.assertIn('CURRENT TARIFFS APPLIED',content)
        self.assertIn('Estimated',content);self.assertNotIn('Amount Due R',content)
        self.assertEqual(before,self.tariff_snapshot());self.assertEqual(readings,self.snapshot())
        self.assertEqual(bills,list(self.session.execute(text('SELECT * FROM bil_municipal_statement ORDER BY id'))))

    def test_estimated_debit_and_credit_differences_remain_display_only(self):
        self.unavailable_history();data=self.use_current()
        total=next(r['calculated'] for r in data['pm_comparisons'] if r['account']=='83192896040')
        for billed,label in [(total-10,'Estimated Debit difference'),(total+10,'Estimated Credit difference')]:
            self.session.execute(text('UPDATE bil_municipal_statement SET due=:v WHERE account_id=1'),dict(v=billed))
            self.session.commit();self.conn.commit()
            before=list(self.session.execute(text('SELECT * FROM bil_municipal_statement ORDER BY id')))
            result=next(r for r in self.use_current()['pm_comparisons'] if r['account']=='83192896040')
            self.assertEqual(result['label'],label);self.assertEqual(abs(result['difference']),Decimal('10.00'))
            self.assertEqual(before,list(self.session.execute(text('SELECT * FROM bil_municipal_statement ORDER BY id'))))

    def test_partial_historical_coverage_estimates_only_missing_component(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-07-01',rate=25 WHERE code='WF01'"))
        self.session.commit();self.conn.commit();data=self.use_current()
        self.assertEqual([r['code'] for r in data['pm_estimated_components']],['WF01'])
        self.assertEqual(data['electricity']['rows'][0]['rate'],Decimal('3.2795'))
        water=next(w for w in data['water']['meters'] if w['meter_number']=='CEL888')
        self.assertEqual(next(r['due'] for r in water['ws_breakdown'] if r['desc']=='Water Loss Levy (insurance)'),Decimal(25))
        self.assertTrue(data['pm_pricing_complete']);self.assertFalse(data['pm_complete'])

    def test_future_only_current_component_stays_unavailable_and_incomplete(self):
        self.unavailable_history()
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2999-07-01' WHERE code='SDSurcharge'"))
        self.session.commit();self.conn.commit();data=self.use_current()
        self.assertTrue(data['pm_estimated']);self.assertFalse(data['pm_pricing_complete'])
        self.assertIn('Sewerage surcharge',str(data['pm_unpriced_components']))
        result=next(r for r in data['pm_comparisons'] if r['account']=='83192896040')
        self.assertIsNone(result['difference'])
        page=self.client.get(self.february+'&use_current_tariffs=1',headers=self.headers)
        self.assertIn('INCOMPLETE',page.text);self.assertIn('Rate unavailable',page.text)

    def test_override_never_replaces_ambiguous_official_records_or_crosses_users(self):
        self.session.execute(text("INSERT INTO bil_tariff(utility_type,code,rate,effective_date) VALUES('electricity','ELEC01',9,'2025-07-01')"))
        self.session.commit();self.conn.commit();data=self.use_current()
        self.assertEqual(data['electricity']['rows'],[])
        self.assertIn('conflicting records',str(data['pm_issues']))
        self.assertEqual(self.client.get(self.february.replace('/26/','/28/')+'&use_current_tariffs=1',headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(self.february.replace('recipient_id=11','recipient_id=999999')+'&use_current_tariffs=1',headers=self.headers).status_code,403)

    def test_old_reading_capture_and_correction_do_not_require_tariffs(self):
        with patch('app.program_billing.metsoa_tariffs.select',side_effect=AssertionError('Tariffs must not gate reading capture')):
            self.assertEqual(self.capture_post(self.readings_url,meter_id=1,reading_date='2020-01-31',reading_value='100').status_code,302)
            row=self.session.execute(text("SELECT * FROM bil_meter_reading WHERE meter_id=1 AND reading_date='2020-01-31'")).mappings().one()
            self.assertEqual(self.capture_post(self.readings_url,meter_id=1,record_id=row['id'],version=self.capture.fingerprint(row),reading_date='2020-01-31',reading_value='90').status_code,302)
        self.assertEqual(self.session.execute(text("SELECT count(*) FROM bil_meter_reading WHERE reading_date='2020-01-31' AND meter_id=1")).scalar(),1)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(EstimatePolicyTests(n) for n in EstimatePolicyTests.__dict__ if n.startswith('test_'))
